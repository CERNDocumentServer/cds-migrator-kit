# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM transform step module."""
import logging
import re
from collections import OrderedDict
from copy import deepcopy
from pathlib import Path
from typing import Optional

from cds_rdm.legacy.models import CDSMigrationAffiliationMapping
from cds_rdm.legacy.resolver import get_pid_by_legacy_recid
from invenio_access.permissions import system_identity
from invenio_pidstore.models import PersistentIdentifier, PIDStatus
from invenio_rdm_migrator.logging import Logger
from invenio_rdm_records.proxies import current_rdm_records_service, \
    current_record_communities_service
from sqlalchemy.exc import NoResultFound

from cds_migrator_kit.errors import (
    ManualImportRequired,
    MissingRequiredField,
    MultipleModelsMatched,
    RestrictedFileDetected,
    UnexpectedValue,
)
from cds_migrator_kit.rdm.records.transform.entities.migration import MigrationEntry
from cds_migrator_kit.rdm.records.transform.entities.parent import RecordParent
from cds_migrator_kit.rdm.records.transform.entities.record import (
    RecordEntry,
    RecordEntryData,
)
from cds_migrator_kit.rdm.records.transform.entities.request import RecordRequest
from cds_migrator_kit.rdm.records.transform.transform_versions import \
    RecordVersionsTransform
from cds_migrator_kit.transform.dumper import CDSRecordDump
from cds_migrator_kit.transform.errors import LossyConversion

cli_logger = logging.getLogger("migrator")


class CDSToRDMRecordTransform:
    """Assembles the ETL entry consumed by ``CDSRecordServiceLoad``.

    Wraps the ``record`` content built by ``RecordEntry`` together
    with ``versions``/``parent`` (computed here - ``parent`` is a
    ``RecordParent``, built directly in ``_transform()``) and the
    ETL-envelope extras that aren't record content (currently just
    ``_original_dump`` and ``_clc_sync`` - see ``_transform()``).
    """

    def __init__(
        self,
        workers=None,
        throw=True,
        files_dump_dir=None,
        missing_users=None,
        communities_ids=None,
        dry_run=False,
        collection=None,
        restricted=False,
        plots=False,
        migration_logger=None,
        record_state_logger=None,
        access_grants_view=None,
        preferred_model=None,
    ):
        """Constructor."""
        self._workers = workers
        self._throw = throw
        self._logger = None
        self.files_dump_dir = Path(files_dump_dir).absolute().as_posix()
        self.missing_users_dir = Path(missing_users).absolute().as_posix()
        self.communities_ids = communities_ids
        self.dry_run = dry_run
        self.collection = collection
        self.restricted = restricted
        self.access_grants_view = access_grants_view
        self.plots = plots
        self.migration_logger = migration_logger
        self.record_state_logger = record_state_logger
        self.preferred_model = preferred_model
        self.db_state = {"affiliations": CDSMigrationAffiliationMapping}
        # the DOJSON-processed record data for the entry currently being
        # transformed - populated by transform_xml_to_json(), read by
        # _transform() to build this record's RecordParent (access grants).
        self.raw_json_entry = None

    @property
    def logger(self):
        """Return the base logger."""
        if self._logger is None:
            self._logger = Logger.get_logger()
        return self._logger

    def _transform_xml_to_json(self, entry):
        """Parse the legacy dump into the DOJSON-processed record.

        The one place per record that runs ``CDSRecordDump`` - populates
        ``self.raw_json_entry`` with the DOJSON-mapped record content and
        returns the ``CDSRecordDump`` instance itself, since record-level
        facts derived from the dump (created/updated/recid/files) live on
        that object, not as keys in ``raw_json_entry``.
        """
        record_dump = CDSRecordDump(entry, preferred_model=self.preferred_model)
        record_dump.prepare_revisions()
        timestamp, json_data = record_dump.latest_revision
        self.raw_json_entry = json_data
        self.record_state_logger.add_record(json_data)
        return record_dump

    def _parent(self, entry, record):
        return RecordParent(
            record=record,
            entry=entry,
            json_entry=self.raw_json_entry,
            communities_ids=self.communities_ids,
            access_grants_view=self.access_grants_view,
        ).build()

    def _transform(self, entry) -> Optional[MigrationEntry]:
        """Transform a single entry."""
        # creates the output structure for load step
        migration_logger = self.migration_logger
        try:
            # could be in draft as well, depends on how we decide to publish
            record_dump = self._transform_xml_to_json(entry)

            # ETL-envelope concern, stripped before the record body is built -
            # _metadata()'s forgotten-key check doesn't know this key.
            clc_sync = deepcopy(self.raw_json_entry.get("_clc_sync", False))
            if "_clc_sync" in self.raw_json_entry:
                del self.raw_json_entry["_clc_sync"]

            # same reason: "request_data" must be off raw_json_entry before
            # RecordEntry.transform() runs _metadata()'s forgotten-key
            # check, which doesn't know this key either.
            record_request = RecordRequest(
                json_entry=self.raw_json_entry,
                recid=entry["recid"],
                migration_logger=self.migration_logger,
            ).build()

            record = self._record(entry, record_dump)

            if record:
                return MigrationEntry(
                    _original_dump=entry,
                    record=record,
                    versions=self._versions(entry, record),
                    parent=self._parent(entry, record),
                    _clc_sync=clc_sync,
                    _request_data=record_request,
                    ep_approval=entry.get("ep_approval", []),
                )

        except (
                LossyConversion,
                RestrictedFileDetected,
                UnexpectedValue,
                ManualImportRequired,
                MissingRequiredField,
                MultipleModelsMatched,
        ) as e:
            migration_logger.add_log(e, record=entry)

    def _record(self, entry, record_dump) -> RecordEntryData:
        entry_builder = RecordEntry(
            missing_users_dir=self.missing_users_dir,
            affiliations_mapping=self.db_state["affiliations"],
            dry_run=self.dry_run,
            collection=self.collection,
            restricted=self.restricted,
            migration_logger=self.migration_logger,
            record_state_logger=self.record_state_logger,
        )
        return entry_builder.transform(entry, record_dump, self.raw_json_entry)

    def _versions(self, entry, record: RecordEntryData):
        return RecordVersionsTransform(
            entry=entry,
            record=record,
            files_dump_dir=self.files_dump_dir,
            plots=self.plots,
            migration_logger=self.migration_logger,
        ).build()

    def _load_migrated_recids(self):
        """Load all already-migrated legacy record IDs into a set once."""
        return {
            pid.pid_value
            for pid in PersistentIdentifier.query.filter_by(
                pid_type="lrecid",
                status=PIDStatus.REGISTERED,
            ).all()
        }

    def should_skip(self, entry):
        return str(entry["recid"]) in self._migrated_recids

    def _existing_record_is_restricted(self, record_id):
        """Check the current access state of an already-migrated RDM record.

        Reads live from the record service (DB) rather than the legacy
        MARCXML, since access restrictions may have been changed in RDM
        after migration and the legacy record data would be stale.
        """
        record = current_rdm_records_service.read_latest(
            system_identity, id_=record_id
        )
        access = record.data.get("access", {})
        return access.get("record") != "public" or access.get("files") != "public"

    def run(self, entries):
        """Run transformation step."""
        self._migrated_recids = self._load_migrated_recids()
        for entry in entries:
            if self.should_skip(entry):
                recid = entry["recid"]
                try:
                    parent_pid = get_pid_by_legacy_recid(str(recid))
                    # we don't check here if the record has 980:MIGRATED
                    # because this does not add anything and it should not be deciding factor.
                    # if the legacy recid has been minted - we know already the record has been migrated
                    if self._existing_record_is_restricted(parent_pid.pid_value):
                        # checking if we need to be more careful while assigning access
                        # to various communities
                        raise ManualImportRequired(
                            message=(
                                "Existing record is restricted or has "
                                "restricted files; not adding to "
                                "communities automatically"
                            ),
                            field="access",
                            stage="transform",
                            recid=recid,
                            priority="warning",
                        )
                    # bulk_add resolves record_ids via RDMRecord.pid.resolve(),
                    # which -- unlike read_latest -- has no fallback for a
                    # parent-level recid (what get_pid_by_legacy_recid returns).
                    # We must pass the record's own recid instead.
                    record_item = current_rdm_records_service.read_latest(
                        system_identity, id_=parent_pid.pid_value
                    )
                    for community_id in self.communities_ids:
                        current_record_communities_service.bulk_add(
                            system_identity,
                            community_id,
                            [record_item.id],
                        )
                except NoResultFound:
                    self.migration_logger.add_information(
                        recid,
                        {
                            "message": (
                                "Problem with PIDs - minted legacy recid but"
                                "no corresponding parent record found."
                            ),
                            "value": recid,
                        },
                    )
                except ManualImportRequired as exc:
                    self.migration_logger.add_log(exc, record=entry)

                self.migration_logger.add_information(
                    recid,
                    {
                        "message": "Record already migrated, skipping,"
                                   " added existing {} to communities {}".format(
                            recid, self.communities_ids),
                        "value": recid,
                    },
                )
                self.migration_logger.finalise_record(recid)
                continue
            try:
                yield self._transform(entry)
            except Exception:
                self.logger.exception(entry, exc_info=True)
                if self._throw:
                    raise
                continue
