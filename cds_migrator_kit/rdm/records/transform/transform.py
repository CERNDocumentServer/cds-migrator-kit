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
from cds_migrator_kit.rdm.records.transform.entities.record import RecordEntry
from cds_migrator_kit.rdm.records.transform.entities.request import RecordRequest
from cds_migrator_kit.rdm.records.transform.transform_versions import \
    RecordVersionsTransform
from cds_migrator_kit.transform.dumper import CDSRecordDump
from cds_migrator_kit.transform.errors import LossyConversion

cli_logger = logging.getLogger("migrator")


class CDSToRDMRecordTransform:
    """Assembles the ETL entry consumed by ``CDSMigrationEntryLoad``.

    Wraps the ``record`` content built by ``RecordEntry`` together
    with ``versions``/``parent`` (computed here - ``parent`` is a
    ``RecordParent``, built directly in ``_transform()``) and the
    ETL-envelope extras that aren't record content (currently
    ``_original_dump``/``_clc_sync``/``ep_approval`` - see
    ``_transform()``). Also
    owns the "did every DOJSON-produced key get consumed by something"
    check across the whole entry - see ``_check_forgotten_keys()``.
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
        """Constructor.

        ``workers``/``missing_users``/``dry_run``/``collection`` are
        accepted (not stored) purely because
        ``cds_migrator_kit.runner.runner.Runner`` always passes them when
        constructing this class from ``streams.yaml`` config - they aren't
        used by this class or ``RecordEntry``.
        """
        self._throw = throw
        self._logger = None
        self.files_dump_dir = Path(files_dump_dir).absolute().as_posix()
        self.communities_ids = communities_ids
        self.restricted = restricted
        self.access_grants_view = access_grants_view
        self.plots = plots
        self.migration_logger = migration_logger
        self.record_state_logger = record_state_logger
        self.preferred_model = preferred_model
        self.db_state = {"affiliations": CDSMigrationAffiliationMapping}
        # the DOJSON-processed record data for the entry currently being
        # transformed - populated by _transform_xml_to_json(), read by
        # _transform() to build this record's RecordParent (access grants).
        self.dojson_entry = None

    @property
    def logger(self):
        """Return the base logger."""
        if self._logger is None:
            self._logger = Logger.get_logger()
        return self._logger

    def _transform_xml_to_json(self, raw_dump_entry):
        """Parse the legacy dump into the DOJSON-processed record.

        The one place per record that runs ``CDSRecordDump`` - populates
        ``self.dojson_entry`` with the DOJSON-mapped record content and
        returns the ``CDSRecordDump`` instance itself, since record-level
        facts derived from the dump (created/updated/recid/files) live on
        that object, not as keys in ``dojson_entry``.
        """
        dump = CDSRecordDump(raw_dump_entry, preferred_model=self.preferred_model)
        dump.prepare_revisions()
        if dump.multiple_models_warning:
            w = dump.multiple_models_warning
            recid = raw_dump_entry.get("recid") or raw_dump_entry.get("record", {}).get(
                "recid")
            matched = re.findall(r"\['(\w+)',", w.message or "")
            self.migration_logger.add_information(
                str(recid),
                {
                    "type": w.type,
                    "error": w.description,
                    "message": w.message,
                    "value": ", ".join(matched),
                    "priority": "warning",
                },
            )
        timestamp, dojson_entry = dump.latest_revision
        self.dojson_entry = dojson_entry
        self.record_state_logger.add_record(dojson_entry)
        return dump

    def _parent(self, raw_dump_entry, record):
        return RecordParent(
            record=record,
            raw_dump_entry=raw_dump_entry,
            dojson_entry=self.dojson_entry,
            communities_ids=self.communities_ids,
            access_grants_view=self.access_grants_view,
        ).build()

    def _request(self, raw_dump_entry):
        """Build the community-inclusion request for this entry.

        "request_data" must be off ``dojson_entry`` before
        ``_check_forgotten_keys()`` runs - ``RecordRequest.build()`` pops
        it right here, so that check never sees it either.
        """
        return RecordRequest(
            dojson_entry=self.dojson_entry,
            recid=raw_dump_entry["recid"],
            migration_logger=self.migration_logger,
        ).build()

    def _transform(self, raw_dump_entry) -> Optional[MigrationEntry]:
        """Transform a single entry."""
        # creates the output structure for load step
        migration_logger = self.migration_logger
        try:
            # could be in draft as well, depends on how we decide to publish
            dump = self._transform_xml_to_json(raw_dump_entry)

            # ETL-envelope concern, stripped before the record body is built -
            # _check_forgotten_keys() doesn't know this key.
            clc_sync = deepcopy(self.dojson_entry.pop("_clc_sync", False))

            # legacy_recid is produced by a dojson rule but never consumed
            # anywhere - it always carries the same value as recid (just
            # int vs str). Pop it so it doesn't trip _check_forgotten_keys().
            self.dojson_entry.pop("legacy_recid", None)
            # ep_approval (the record's EP-approval workflow history, from
            # the ^9031_ MARC tag) is an ETL-envelope concern like
            # _clc_sync above - popped here rather than left as record
            # content, and consumed directly below via MigrationEntry.
            ep_approval = self.dojson_entry.pop("ep_approval", [])
            record_request = self._request(raw_dump_entry)
            record = self._record(raw_dump_entry, dump)

            if record:
                versions = self._versions(raw_dump_entry, record)
                # RecordParent is the last entity to touch dojson_entry -
                # it pops submitter/communities/access_grants - so the
                # forgotten-keys check must run after it.
                parent = self._parent(raw_dump_entry, record)
                self._check_forgotten_keys(record)

                return MigrationEntry(
                    _original_dump=raw_dump_entry,
                    record=record,
                    versions=versions,
                    parent=parent,
                    _clc_sync=clc_sync,
                    _request_data=record_request,
                    ep_approval=ep_approval,
                )

        except (
                LossyConversion,
                RestrictedFileDetected,
                UnexpectedValue,
                ManualImportRequired,
                MissingRequiredField,
                MultipleModelsMatched,
        ) as e:
            migration_logger.add_log(e, record=raw_dump_entry)

    def _check_forgotten_keys(self, record: RecordEntry):
        """Ensure every key DOJSON produced for this entry got consumed.

        A "global" check across the whole ETL entry, run once all entities
        (``RecordEntry``, ``RecordParent``, ...) have built - and popped
        their own keys off ``self.dojson_entry`` - rather than
        ``RecordEntry`` checking only its own record body/metadata.
        """
        # recid/status_week_date are read live by field mappers (recid for
        # error reporting, status_week_date by PublicationDateMapper to
        # derive metadata["publication_date"]) but never popped, since
        # they're needed for the duration of that mapping; custom_fields
        # is read the same way by TitleMapper's meeting-title fallback.
        helper_keys = ["recid", "status_week_date", "custom_fields", "agency_code"]
        keys = [key for key in self.dojson_entry if key not in helper_keys]
        metadata_keys = record.body["metadata"].keys()
        forgotten_keys = [key for key in keys if key not in metadata_keys]
        if forgotten_keys:
            raise ManualImportRequired("Unassigned metadata key", value=forgotten_keys)

    def _record(self, raw_dump_entry, dump) -> RecordEntry:
        entry_builder = RecordEntry(
            dump=dump,
            dojson_entry=self.dojson_entry,
            affiliations_mapping=self.db_state["affiliations"],
            restricted=self.restricted,
            migration_logger=self.migration_logger,
        )
        entry_builder.build()
        return entry_builder

    def _versions(self, raw_dump_entry, record: RecordEntry):
        return RecordVersionsTransform(
            raw_dump_entry=raw_dump_entry,
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

    def should_skip(self, raw_dump_entry):
        return str(raw_dump_entry["recid"]) in self._migrated_recids

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
        for raw_dump_entry in entries:
            if self.should_skip(raw_dump_entry):
                recid = raw_dump_entry["recid"]
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
                    self.migration_logger.add_log(exc, record=raw_dump_entry)

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
                yield self._transform(raw_dump_entry)
            except Exception:
                self.logger.exception(raw_dump_entry, exc_info=True)
                if self._throw:
                    raise
                continue
