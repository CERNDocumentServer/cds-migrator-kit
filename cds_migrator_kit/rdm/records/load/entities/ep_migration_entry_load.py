# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Splits and loads an EP approval record as a public/restricted pair."""

from invenio_access.permissions import system_identity
from invenio_db import db
from invenio_db.uow import UnitOfWork
from invenio_rdm_records.proxies import current_rdm_records_service
from invenio_rdm_records.records.api import RDMParent

from cds_migrator_kit.errors import ManualImportRequired, UnexpectedValue
from cds_migrator_kit.rdm.records.transform.entities.migration import MigrationEntry

from ..load import CDSMigrationEntryLoad
from .approval_request import ApprovalRequest
from .approval_request_load import ApprovalRequestLoad
from .ep_split import PublicEntry, RestrictedEntry
from .parent import ParentLoad


class EPMigrationEntryLoad(CDSMigrationEntryLoad):
    """Splits and loads an EP approval record as a public/restricted pair.

    Adds EP-approval splitting on top of the plain per-entry loader: builds
    and validates the ``ApprovalRequest``, splits the entry into
    ``PublicEntry``/``RestrictedEntry``, creates both records directly via
    ``RecordLoad`` (not by delegating to ``super()._load()``, since the two
    halves need different treatment - see ``_load_split()``), creates/
    approves the EP approval request, links the two records with
    related_identifiers, and writes EP approval metadata onto both parents -
    all inside a single unit of work so a partial failure rolls back
    everything instead of leaving an orphaned restricted record and/or
    approval request behind.

    Registered as the stream's ``load_cls`` in place of
    ``CDSMigrationEntryLoad`` - every entry goes through this class, and
    ``_load()`` decides per-entry whether to split or fall back to the
    inherited plain behavior.
    """

    def _load(self, entry: MigrationEntry):
        """Route to the plain per-entry loader, or split, based on the entry.

        Overrides ``CDSMigrationEntryLoad._load()`` to add the ep_approval
        check *before* delegating - the base class has no knowledge of
        splitting at all.
        """
        if not entry:
            return

        recid = entry["record"].recid
        if self._should_skip_recid(recid):
            return

        if not entry.get("ep_approval"):
            return super()._load(entry)

        return self._load_split(entry, recid)

    def _load_split(self, entry: MigrationEntry, recid):
        """Load a record with EP approval by splitting it into two records.

        Configure the 2 records by separating the files, then:
        1. create the restricted record
        2. create and approve the EP approval request
        3. create the public record and link both with related_identifiers
        4. write EP approval metadata on both parents

        Steps 1-4 are grouped into a single unit of work so a failure at any
        point rolls back everything, instead of leaving an orphaned
        restricted record and/or approval request committed behind. This
        path has its own uow/error handling (rather than sharing the plain
        single-record path's) since a partial failure here is more severe -
        it can leave a public/restricted pair half-created - so unexpected
        errors are logged as "critical" here, not "warning".

        Parent access grants and the community-inclusion request are
        applied to the *restricted* half - it's the one carrying the actual
        restricted files and (per ``RestrictedEntry``) any ``_request_data``
        (``PublicEntry`` strips it). Original-dump persistence and CLC sync
        are applied to the *public* half - the discoverable/"final" record
        for this legacy recid.
        """
        try:
            ep_approval = entry.get("ep_approval")
            record_body = entry["record"].body
            metadata = record_body.get("metadata", {})

            self.approval_request = ApprovalRequest(
                ep_approval=ep_approval,
                legacy_recid=recid,
                title=metadata.get("title"),
                resource_type=metadata.get("resource_type"),
                dry_run=self.dry_run,
            )
            self.approval_request.validate()
            approval_request_load = ApprovalRequestLoad(self.approval_request)

            # Split the metadata and files
            public_entry = PublicEntry(
                entry,
                approval_request=self.approval_request,
                migration_logger=self.migration_logger,
            ).build()
            restricted_entry = RestrictedEntry(
                entry,
                approval_request=self.approval_request,
                migration_logger=self.migration_logger,
            ).build()

            restricted_record_load = self.record_load_cls(
                restricted_entry["record"],
                restricted_entry["parent"],
                self.migration_logger,
                is_final_record=False,
                update_new_version_publication_date=self.update_new_version_publication_date,
                record_state_logger=self.record_state_logger,
            )
            public_record_load = self.record_load_cls(
                public_entry["record"],
                public_entry["parent"],
                self.migration_logger,
                is_final_record=True,
                update_new_version_publication_date=self.update_new_version_publication_date,
                record_state_logger=self.record_state_logger,
            )

            if self.dry_run:
                # We are already validating the approval request, so trying to load it
                # is not necessary. There is now uow here so we can't call `approval_request_load.create`
                restricted_record_load.dry_load()
                public_record_load.dry_load()
                # We need to finalise here, or the log would only list the
                # records that failed.
                self.migration_logger.finalise_record(recid)
                return

            with UnitOfWork(db.session) as uow:
                # 1. Create restricted record
                restricted_records = restricted_record_load.load(
                    restricted_entry, uow=uow
                )
                restricted_record_state = restricted_record_load.build_record_state(
                    recid, restricted_records
                )

                # 2. Create and approve EP approval request
                approval_request_load.create(restricted_record_state, uow=uow)

                # Parent access grants + community-inclusion request for
                # the restricted half.
                self.parent_load_cls(
                    restricted_entry, self.migration_logger, restricted_record_state
                ).load(published_record=restricted_records[-1], uow=uow)
                self.request_load_cls(restricted_entry).load(
                    restricted_records, self.create_inclusion_request, uow
                )

                # 3. Create public record
                public_records = public_record_load.load(public_entry, uow=uow)
                public_record_state = public_record_load.build_record_state(
                    recid, public_records
                )
                if not public_record_state:
                    raise UnexpectedValue(
                        message="Public record is required for EP approval.",
                        stage="load",
                        recid=recid,
                        priority="critical",
                    )

                # Original-dump persistence for the public half.
                self._save_original_dumped_record(
                    public_entry, public_record_state, uow
                )

                # Link the records with related_identifiers
                self._append_related_identifier(
                    public_record_state["latest_version"],
                    restricted_record_state["latest_version"],
                    "isversionof",
                    self.approval_request.resource_type,
                    uow=uow,
                )
                self._append_related_identifier(
                    restricted_record_state["latest_version"],
                    public_record_state["latest_version"],
                    "isvariantformof",
                    self.approval_request.resource_type,
                    uow=uow,
                )

                # 4. Write EP approval metadata on both parents
                self._link_parent_ep_approvals(
                    restricted_record_state,
                    public_record_state,
                    legacy_recid=recid,
                    uow=uow,
                )

                public_record_state["internal_version"] = restricted_record_state[
                    "latest_version"
                ]

                uow.commit()

                # Only log to disk once the unit of work has actually
                # committed - logging any earlier risks recording a record
                # that a later failure in the same uow rolls back.
                public_record_load.log_record_state(public_record_state)

                # The public record is the final one; finalise it only now
                # that the whole split has actually committed (see the
                # matching `uow is None` guard in the plain path).
                self.migration_logger.finalise_record(recid)

            # CLC sync for the public half, run after commit.
            self._apply_clc_sync(public_record_state, public_entry)
        except (UnexpectedValue, ManualImportRequired) as e:
            self.migration_logger.add_log(e, record=entry)
        except Exception as e:
            exc = ManualImportRequired(
                message=str(e),
                field="validation",
                stage="load",
                recid=recid,
                priority="critical",
            )
            self.migration_logger.add_log(exc, record=entry)

    def _link_parent_ep_approvals(
        self, restricted_record_state, public_record_state, legacy_recid, uow=None
    ):
        """Write parent metadata and link public/restricted records.

        If ``uow`` is provided, the writes are registered on it without
        committing, so the caller can group this atomically with other
        operations.
        """
        if self.dry_run:
            return

        if not restricted_record_state or not public_record_state:
            raise UnexpectedValue(
                message="Both public and restricted records are required for EP approval.",
                stage="load",
                recid=legacy_recid,
                priority="critical",
            )

        if uow is not None:
            self._write_parent_ep_approvals(
                restricted_record_state, public_record_state, uow
            )
            return

        with UnitOfWork() as inner_uow:
            self._write_parent_ep_approvals(
                restricted_record_state, public_record_state, inner_uow
            )
            inner_uow.commit()

    def _write_parent_ep_approvals(
        self, restricted_record_state, public_record_state, uow
    ):
        """Register the EP approval parent writes for both records on the given uow."""
        report_number = self.approval_request.report_number
        approval_iso = self.approval_request.approved_at.isoformat()
        restricted_recid = restricted_record_state["latest_version"]
        public_recid = public_record_state["latest_version"]
        restricted_parent = RDMParent.get_record(
            restricted_record_state["parent_object_uuid"]
        )
        public_parent = RDMParent.get_record(public_record_state["parent_object_uuid"])

        ParentLoad.write_committee_approval_obj(
            restricted_parent,
            {
                "reportnumber": report_number,
                "datetime": approval_iso,
                "approved_internal_version": restricted_recid,
                "approved_public_version": public_recid,
                "source_public_version": restricted_recid,
            },
            uow,
        )
        ParentLoad.write_committee_approval_obj(
            public_parent,
            {
                "reportnumber": report_number,
                "source_internal_version": restricted_recid,
            },
            uow,
        )

    def _append_related_identifier(
        self, record_id, target_id, relation_id, resource_type, uow=None
    ):
        """Append the related identifier to the record.

        If ``uow`` is provided, it is passed through to the record service
        calls so they register on it without committing, letting the caller
        group this atomically with other operations.
        """
        draft = current_rdm_records_service.edit(
            system_identity, id_=record_id, uow=uow
        )
        data = draft.data
        related = list(data.get("metadata", {}).get("related_identifiers", []))

        entry = {
            "identifier": target_id,
            "scheme": "cds",
            "relation_type": {"id": relation_id},
        }
        if resource_type:
            entry["resource_type"] = resource_type
        related.append(entry)
        data.setdefault("metadata", {})["related_identifiers"] = related
        current_rdm_records_service.update_draft(
            system_identity, id_=draft.id, data=data, uow=uow
        )
        current_rdm_records_service.publish(system_identity, id_=draft.id, uow=uow)
        return True
