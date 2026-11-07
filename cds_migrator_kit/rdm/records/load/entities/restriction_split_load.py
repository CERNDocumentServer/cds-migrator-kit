# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Splits and loads records with mixed file restrictions (public/restricted) into separate public and restricted records."""

from __future__ import annotations

from abc import ABC, abstractmethod

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


class RecordSplitStrategy(ABC):
    """
    A strategy for splitting records depending on why we're splitting them (e.g. if they have mixed file restrictions).

    This specifies some methods that the SplitMigrationEntryLoad hooks into during the load process, depending on which strategy
    it has selected.
    """

    def __init__(
        self, loader: CDSMigrationEntryLoad, entry: MigrationEntry, recid: str
    ) -> None:
        """Constructor."""
        self.loader = loader
        self.entry = entry
        self.recid = recid

    @staticmethod
    @abstractmethod
    def should_use(entry: MigrationEntry) -> bool:
        """Whether this strategy should be used.

        This is evaluated in a list in split_migration_entry_load.py. Whichever strategy is first in that list to return True for this method is the one that gets selected.

        If no strategies match, the regular non-split load is used.
        """
        raise NotImplementedError

    @abstractmethod
    def build_entries(self) -> tuple[MigrationEntry, MigrationEntry]:
        """Build the public/restricted entries."""
        raise NotImplementedError

    @abstractmethod
    def after_restricted_parent(self, uow: UnitOfWork, restricted_record_state: dict):
        """Actions to perform after the restricted record's parent has been created."""
        raise NotImplementedError

    @abstractmethod
    def after_both_loaded(
        self,
        uow: UnitOfWork,
        public_record_state: dict,
        restricted_record_state: dict,
    ):
        """Actions to perform after both the restricted and non-restricted record have been loading."""
        raise NotImplementedError

    @abstractmethod
    def public_relation_to_restricted(self) -> tuple[str, str | None]:
        """
        The details for how the restricted record should be listed as a relation on the public record.

        A tuple of `(relation_id, resource_type | None)`
        """
        raise NotImplementedError

    @abstractmethod
    def restricted_relation_to_public(self) -> tuple[str, str | None]:
        """
        The details for how the public record should be listed as a relation on the restricted record.

        A tuple of `(relation_id, resource_type | None)`
        """
        raise NotImplementedError


class MixedRestrictionSplitStrategy(RecordSplitStrategy):
    """Splits a record based on the restriction of its files."""

    @staticmethod
    def should_use(entry: MigrationEntry) -> bool:
        """This strategy is used if the record has a mix of restricted and non-restricted files."""
        return (
            entry["record"].has_any_restricted_file()
            and entry["record"].has_any_non_restricted_file()
        )

    def build_entries(self) -> tuple[MigrationEntry, MigrationEntry]:
        """Build the public/restricted entries."""
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
        """Not applicable."""
        pass

    def after_both_loaded(
        self,
        uow: UnitOfWork,
        public_record_state: dict,
        restricted_record_state: dict,
    ):
        """Not applicable."""
        pass

    def public_relation_to_restricted(self) -> tuple[str, str | None]:
        """For mixed-restriction records, we use isvariantformof in both directions (see decision log)."""
        return ("isvariantformof", None)

    def restricted_relation_to_public(self) -> tuple[str, str | None]:
        """For mixed-restriction records, we use isvariantformof in both directions (see decision log)."""
        return ("isvariantformof", None)


class EPRecordSplitStrategy(RecordSplitStrategy):
    """EP-specific splitting strategy."""

    @staticmethod
    def should_use(entry: MigrationEntry):
        """This strategy is used if the parent record has an EP approval set."""
        ep_approval = entry.get("ep_approval")
        return ep_approval is not None and len(ep_approval) != 0

    def build_entries(self) -> tuple[MigrationEntry, MigrationEntry]:
        """Build the entries with the approval request attached."""
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
        """Create the approval request only after the parent has been created."""
        approval_request_load = ApprovalRequestLoad(self.approval_request)
        approval_request_load.create(restricted_record_state, uow=uow)

    def after_both_loaded(
        self,
        uow: UnitOfWork,
        public_record_state: dict,
        restricted_record_state: dict,
    ):
        """Write the EP approval object to the parent record."""
        self._link_parent_ep_approvals(
            restricted_record_state,
            public_record_state,
            legacy_recid=self.recid,
            uow=uow,
        )

    def public_relation_to_restricted(self) -> tuple[str, str | None]:
        """The public record is a version of the restricted record, since usually only one of the draft versions gets published."""
        return ("isversionof", self.approval_request.resource_type)

    def restricted_relation_to_public(self) -> tuple[str, str | None]:
        """The restricted record is a variant form of the public record, since it contains the original drafts and possibly more versions."""
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
