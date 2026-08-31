# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM migration load module."""

import json

from cds_rdm.clc_sync.models import CDSToCLCSyncModel
from cds_rdm.legacy.models import CDSMigrationLegacyRecord
from cds_rdm.legacy.resolver import get_pid_by_legacy_recid
from cds_rdm.minters import legacy_recid_minter
from invenio_db import db
from invenio_db.uow import ModelCommitOp, UnitOfWork
from invenio_i18n import _
from invenio_pidstore.models import PersistentIdentifier
from invenio_rdm_migrator.load.base import Load
from invenio_records.systemfields.relations import InvalidRelationValue
from marshmallow import ValidationError

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

    def _should_skip_recid(self, recid):
        """Check if recid should be skipped."""
        if recid in self.legacy_pids_to_redirect or self._have_migrated_recid(recid):
            self.migration_logger.add_information(
                recid, state={"message": "Record already migrated", "value": recid}
            )
            self.migration_logger.finalise_record(recid)
            return True
        return False

    def _load(self, entry: MigrationEntry):
        """Use the services to load the entry."""
        if not entry:
            return

        recid = entry["record"].recid
        if self._should_skip_recid(recid):
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
