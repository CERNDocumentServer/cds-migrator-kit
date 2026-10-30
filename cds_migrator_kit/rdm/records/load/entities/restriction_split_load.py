# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Splits and loads records with mixed file restrictions (public/restricted) into separate public and restricted records."""

from __future__ import annotations

from invenio_db.uow import UnitOfWork
from invenio_rdm_records.records.api import RDMParent

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.rdm.records.load.entities.approval_request import ApprovalRequest
from cds_migrator_kit.rdm.records.load.entities.approval_request_load import (
    ApprovalRequestLoad,
)
from cds_migrator_kit.rdm.records.load.entities.parent import ParentLoad
from cds_migrator_kit.rdm.records.load.entities.split_entries import (
    PublicEntry,
    RestrictedEntry,
)
from cds_migrator_kit.rdm.records.load.load import CDSMigrationEntryLoad
from cds_migrator_kit.rdm.records.transform.entities.migration import MigrationEntry


class RecordSplitStrategy:
    """The base splitting strategy: splits a record based on the restriction of its files."""

    def __init__(
        self, loader: CDSMigrationEntryLoad, entry: MigrationEntry, recid: str
    ) -> None:
        self.loader = loader
        self.entry = entry
        self.recid = recid

    @staticmethod
    def should_use(entry: MigrationEntry) -> bool:
        return (
            entry["record"].has_any_restricted_file()
            and entry["record"].has_any_non_restricted_file()
        )

    def build_entries(self) -> tuple[MigrationEntry, MigrationEntry]:
        # Split the metadata and files
        public_entry = PublicEntry(
            self.entry,
            migration_logger=self.loader.migration_logger,
        ).build()
        restricted_entry = RestrictedEntry(
            self.entry,
            migration_logger=self.loader.migration_logger,
        ).build()
        return (public_entry, restricted_entry)

    def after_restricted_parent(self, uow: UnitOfWork, restricted_record_state: dict):
        pass

    def after_both_loaded(
        self,
        uow: UnitOfWork,
        public_record_state: dict,
        restricted_record_state: dict,
    ):
        pass

    def public_relation_to_restricted(self) -> tuple[str, str | None]:
        """
        The details for how the restricted record should be listed as a relation on the public record.

        A tuple of `(relation_id, resource_type | None)`
        """
        return ("isvariantformof", None)

    def restricted_relation_to_public(self) -> tuple[str, str | None]:
        """
        The details for how the public record should be listed as a relation on the restricted record.

        A tuple of `(relation_id, resource_type | None)`
        """
        return ("isvariantformof", None)


class EPRecordSplitStrategy(RecordSplitStrategy):
    """EP-specific splitting strategy, similar to the base."""

    def __init__(
        self, loader: CDSMigrationEntryLoad, entry: MigrationEntry, recid: str
    ) -> None:
        super().__init__(loader, entry, recid)

    @staticmethod
    def should_use(entry: MigrationEntry):
        ep_approval = entry.get("ep_approval")
        return ep_approval is not None and len(ep_approval) != 0

    def build_entries(self) -> tuple[MigrationEntry, MigrationEntry]:
        record_body = self.entry["record"].body
        assert record_body is not None
        metadata = record_body.get("metadata", {})
        ep_approval = self.entry.get("ep_approval")

        self.approval_request = ApprovalRequest(
            ep_approval=ep_approval,
            legacy_recid=self.recid,
            title=metadata.get("title"),
            resource_type=metadata.get("resource_type"),
            dry_run=self.loader.dry_run,
        )
        self.approval_request.validate()

        # Split the metadata and files
        public_entry = PublicEntry(
            self.entry,
            migration_logger=self.loader.migration_logger,
            approval_request=self.approval_request,
        ).build()
        restricted_entry = RestrictedEntry(
            self.entry,
            migration_logger=self.loader.migration_logger,
            approval_request=self.approval_request,
        ).build()
        return (public_entry, restricted_entry)

    def after_restricted_parent(self, uow: UnitOfWork, restricted_record_state: dict):
        super().after_restricted_parent(uow, restricted_record_state)

        approval_request_load = ApprovalRequestLoad(self.approval_request)
        approval_request_load.create(restricted_record_state, uow=uow)

    def after_both_loaded(
        self,
        uow: UnitOfWork,
        public_record_state: dict,
        restricted_record_state: dict,
    ):
        self._link_parent_ep_approvals(
            restricted_record_state,
            public_record_state,
            legacy_recid=self.recid,
            uow=uow,
        )

    def public_relation_to_restricted(self) -> tuple[str, str | None]:
        """
        The details for how the restricted record should be listed as a relation on the public record.

        A tuple of `(relation_id, resource_type | None)`
        """
        return ("isversionof", self.approval_request.resource_type)

    def restricted_relation_to_public(self) -> tuple[str, str | None]:
        """
        The details for how the public record should be listed as a relation on the restricted record.

        A tuple of `(relation_id, resource_type | None)`
        """
        return ("isvariantformof", self.approval_request.resource_type)

    def _link_parent_ep_approvals(
        self, restricted_record_state, public_record_state, legacy_recid, uow=None
    ):
        """Write parent metadata and link public/restricted records.

        If ``uow`` is provided, the writes are registered on it without
        committing, so the caller can group this atomically with other
        operations.
        """
        if self.loader.dry_run:
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
                "reportnumber": [report_number],
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
                "reportnumber": [report_number],
                "source_internal_version": restricted_recid,
            },
            uow,
        )
