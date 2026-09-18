# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM migration load module."""

import json
import re

from cds_rdm.clc_sync.models import CDSToCLCSyncModel
from cds_rdm.inspire_harvester.load.matcher import ArxivIdentifierMatchFilter
from cds_rdm.inspire_harvester.utils import retrieve_identifiers
from cds_rdm.legacy.models import CDSMigrationLegacyRecord
from cds_rdm.legacy.resolver import get_pid_by_legacy_recid
from cds_rdm.minters import legacy_recid_minter
from cds_rdm.schemes import cds_rdm_regexp
from flask import current_app
from invenio_access.permissions import system_identity
from invenio_db import db
from invenio_db.uow import ModelCommitOp, UnitOfWork
from invenio_i18n import _
from invenio_pidstore.errors import PIDDoesNotExistError
from invenio_pidstore.models import PersistentIdentifier
from invenio_rdm_migrator.load.base import Load
from invenio_rdm_records.proxies import current_rdm_records_service
from invenio_records.systemfields.relations import InvalidRelationValue
from invenio_search.engine import dsl
from marshmallow import ValidationError
from sqlalchemy.orm.exc import NoResultFound

from cds_migrator_kit.errors import (
    CDSMigrationException,
    GrantCreationError,
    ManualImportRequired,
    RecordFlaggedCuration,
    UnexpectedValue,
)
from cds_migrator_kit.rdm.records.transform.entities.migration import MigrationEntry

from .entities.parent import ParentLoad
from .entities.record import RecordLoad
from .entities.request import RequestLoad


class CDSMigrationEntryLoad(Load):
    """Loads a plain (non-EP-approval) ``MigrationEntry`` end to end."""

    def __init__(
        self,
        db_uri=None,
        data_dir=None,
        entries=None,
        dry_run=False,
        legacy_pids_to_redirect=None,
        update_new_version_publication_date=False,
        create_inclusion_request=False,
        migration_logger=None,
        record_state_logger=None,
    ):
        """Constructor."""
        self.dry_run = dry_run
        self.legacy_pids_to_redirect = {}
        self.update_new_version_publication_date = update_new_version_publication_date
        self.create_inclusion_request = create_inclusion_request
        self.migration_logger = migration_logger
        self.record_state_logger = record_state_logger
        self.parent_load_cls = ParentLoad
        self.record_load_cls = RecordLoad
        self.request_load_cls = RequestLoad

        if legacy_pids_to_redirect is not None:
            with open(legacy_pids_to_redirect, "r") as fp:
                self.legacy_pids_to_redirect = json.load(fp)
        else:
            self.legacy_pids_to_redirect = {}

    def _apply_clc_sync(self, record_state, entry: MigrationEntry):
        """Create the CLC sync entry after the load has committed."""
        if entry.get("_clc_sync", False):
            sync = CDSToCLCSyncModel(
                parent_record_pid=record_state["parent_recid"],
                status="P",
                auto_sync=True,
            )
            db.session.add(sync)
            db.session.commit()

    def _save_original_dumped_record(self, entry: MigrationEntry, recid_state, uow):
        """Save the original dumped record.

        This is the originally extracted record before any transformation.
        """
        _original_dump = entry["_original_dump"]
        _original_dump_model = CDSMigrationLegacyRecord(
            json=_original_dump,
            parent_object_uuid=recid_state["parent_object_uuid"],
            migrated_record_object_uuid=recid_state["latest_version_object_uuid"],
            legacy_recid=entry["record"].recid,
        )
        uow.register(ModelCommitOp(_original_dump_model))

    @staticmethod
    def _have_migrated_recid(recid):
        """Check if we have minted `lrecid` pid."""
        pid = PersistentIdentifier.query.filter_by(
            pid_type="lrecid",
            pid_value=recid,
        ).one_or_none()
        return pid is not None

    def _should_skip_recid(self, entry: MigrationEntry):
        """Skip if this legacy recid is already on new CDS."""
        recid = entry["record"].recid
        if recid in self.legacy_pids_to_redirect or self._have_migrated_recid(recid):
            self.migration_logger.add_information(
                recid, state={"message": "Record already migrated", "value": recid}
            )
            self.migration_logger.finalise_record(recid)
            return True
        try:
            existing = self._existing_cds_record(entry)
        except ManualImportRequired as exc:
            self.migration_logger.add_log(exc, record=entry)
            return True
        if not existing:
            return False
        if not self.dry_run:
            legacy_recid_minter(recid, existing._record.parent.model.id)
            db.session.commit()
        self.migration_logger.add_information(
            recid,
            {
                "message": "Record already submitted on new CDS",
                "value": existing.id,
            },
        )
        self.migration_logger.finalise_record(recid)
        return True

    def _one_pid(self, pid_type, values, field):
        """Return the record for these pids if exactly one parent matches."""
        by_parent = {}
        for value in dict.fromkeys(v for v in values if v):
            for pid in PersistentIdentifier.query.filter_by(
                pid_type=pid_type, pid_value=value, object_type="rec"
            ):
                recid = (
                    pid
                    if pid_type == "recid"
                    else PersistentIdentifier.query.filter_by(
                        object_uuid=pid.object_uuid,
                        object_type="rec",
                        pid_type="recid",
                    ).one_or_none()
                )
                if not recid:
                    continue
                try:
                    record = current_rdm_records_service.read_latest(
                        system_identity, id_=recid.pid_value
                    )
                except (PIDDoesNotExistError, NoResultFound):
                    continue
                by_parent[record._record.parent.pid.pid_value] = record
        if len(by_parent) > 1:
            raise ManualImportRequired(
                message="Multiple existing CDS records match this legacy record",
                field=field,
                stage="load",
                value=", ".join(sorted(by_parent)),
                priority="warning",
            )
        return next(iter(by_parent.values()), None)

    def _existing_cds_record(self, entry: MigrationEntry):
        """Find a hand-submitted CDS record this dump would duplicate."""
        body = entry["record"].body
        metadata = body.get("metadata", {})
        prefix = current_app.config["DATACITE_PREFIX"]
        identifiers = metadata.get("identifiers", []) + metadata.get(
            "related_identifiers", []
        )
        dois = list(retrieve_identifiers(identifiers, "doi"))
        doi = body.get("pids", {}).get("doi", {}).get("identifier")
        if doi:
            dois.append(doi)

        # New-CDS ids from related urls, structured CDSRDM, and CERN DOI suffixes.
        pat = cds_rdm_regexp.pattern
        rdm_ids = set()
        for url in retrieve_identifiers(identifiers, "url"):
            rdm_ids.update(
                re.findall(
                    rf"repository\.cern/(?:api/)?records/({pat})", url, re.I
                )
            )
        raw = entry["record"].raw_dump_entry
        raw = raw if isinstance(raw, dict) else {}
        items = list(identifiers)
        for key in (
            "identifiers",
            "related_identifiers",
            "external_system_identifiers",
        ):
            items.extend(raw.get(key) or [])
        for item in items:
            if not isinstance(item, dict):
                continue
            scheme = item.get("scheme") or item.get("schema") or ""
            value = item.get("identifier") or item.get("value")
            if (
                scheme.upper() == "CDSRDM"
                and isinstance(value, str)
                and cds_rdm_regexp.fullmatch(value)
            ):
                rdm_ids.add(value)
        rdm_ids.update(
            d.split("/", 1)[1] for d in dois if d.startswith(f"{prefix}/")
        )

        arxivs = list(retrieve_identifiers(identifiers, "arxiv"))
        cores = [
            v.split(":", 1)[-1] if v.lower().startswith("arxiv:") else v
            for v in arxivs
        ]
        # Includes external DOIs and arXiv DataCite DOIs (10.48550/…).
        dois.extend(f"10.48550/arXiv.{c}" for c in cores)

        record = self._one_pid("recid", rdm_ids, "cdsrdm") or self._one_pid(
            "doi", dois, "doi"
        )
        if record or not arxivs:
            return record

        candidate = ArxivIdentifierMatchFilter(
            values=list(dict.fromkeys([*arxivs, *cores]))
        )
        result = current_rdm_records_service.search(
            system_identity,
            extra_filter=dsl.Q("bool", filter=candidate.query),
            params={"size": 25},
        )
        return self._one_pid(
            "recid", [hit["parent"]["id"] for hit in result.hits], "arxiv"
        )

    def _load(self, entry: MigrationEntry):
        """Use the services to load the entry."""
        if not entry:
            return

        recid = entry["record"].recid
        if self._should_skip_recid(entry):
            return

        record_load = RecordLoad(
            entry["record"],
            entry["parent"],
            self.migration_logger,
            is_final_record=True,
            update_new_version_publication_date=self.update_new_version_publication_date,
            record_state_logger=self.record_state_logger,
        )
        try:
            if self.dry_run:
                record_load.dry_load()
                recid_state_after_load = None
                self.migration_logger.finalise_record(recid)
            else:
                with UnitOfWork(db.session) as uow:
                    records = record_load.load(entry, uow=uow)
                    recid_state_after_load = record_load.build_record_state(
                        recid, records
                    )
                    if recid_state_after_load:
                        self._save_original_dumped_record(
                            entry, recid_state_after_load, uow
                        )
                        self.parent_load_cls(
                            entry, self.migration_logger, recid_state_after_load
                        ).load(published_record=records[-1], uow=uow)
                        self.request_load_cls(entry).load(
                            records, self.create_inclusion_request, uow
                        )
                    uow.commit()
                if recid_state_after_load:
                    # only log to disk once the unit of work has actually
                    # committed - logging any earlier risks recording a
                    # record that a later failure in the same uow rolls back
                    record_load.log_record_state(recid_state_after_load)
                    self.migration_logger.finalise_record(recid)
                    # apply after record fully finished (does not sync at the spot, only enabled)
                    self._apply_clc_sync(recid_state_after_load, entry)
            return recid_state_after_load
        except (UnexpectedValue, ManualImportRequired, GrantCreationError) as e:
            self.migration_logger.add_log(e, record=entry)
        except (CDSMigrationException, ValidationError, InvalidRelationValue) as e:
            exc = ManualImportRequired(
                message=str(e),
                field="validation",
                stage="load",
                recid=recid,
                priority="warning",
            )
            self.migration_logger.add_log(exc, record=entry)
        except Exception as e:
            exc = ManualImportRequired(
                message=str(e),
                field="validation",
                stage="load",
                recid=recid,
                priority="warning",
            )
            self.migration_logger.add_log(exc, record=entry)

    def _cleanup(self, *args, **kwargs):
        """Post migration process."""
        for legacy_src_pid, legacy_dest_pid in self.legacy_pids_to_redirect.items():
            if self._have_migrated_recid(legacy_src_pid):
                continue
            try:
                parent_dest_pid = get_pid_by_legacy_recid(str(legacy_dest_pid))
                assert str(parent_dest_pid.status) == "R"
                legacy_recid_minter(legacy_src_pid, parent_dest_pid.object_uuid)
                db.session.commit()
                self.migration_logger.finalise_record(legacy_src_pid)
            except Exception as exc:
                db.session.rollback()
                self.migration_logger.add_log(
                    f"Failed to redirect {legacy_src_pid} to {legacy_dest_pid}: {str(exc)}",
                    record={"recid": legacy_src_pid},
                )
