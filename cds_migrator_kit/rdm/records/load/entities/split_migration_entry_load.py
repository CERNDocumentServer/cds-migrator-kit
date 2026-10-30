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
from cds_migrator_kit.rdm.records.load.entities.restriction_split_load import (
    EPRecordSplitStrategy,
    RecordSplitStrategy,
)
from cds_migrator_kit.rdm.records.transform.entities.migration import MigrationEntry

from ..load import CDSMigrationEntryLoad
from .approval_request import ApprovalRequest
from .approval_request_load import ApprovalRequestLoad
from .parent import ParentLoad
from .split_entries import PublicEntry, RestrictedEntry


class SplitMigrationEntryLoad(CDSMigrationEntryLoad):
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
        if self._should_skip_replicated_record(entry):
            return

        split_strategies: list[type[RecordSplitStrategy]] = [
            EPRecordSplitStrategy,
            RecordSplitStrategy,
        ]
        # Find the first split strategy that thinks it should be used for this record.
        split_strategy = next(
            (strat for strat in split_strategies if strat.should_use(entry)), None
        )

        if split_strategy is None:
            return super()._load(entry)

        return self._load_split(entry, recid, split_strategy(self, entry, recid))

    def _load_split(
        self, entry: MigrationEntry, recid: str, strategy: RecordSplitStrategy
    ):
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
            record_body = entry["record"].body
            metadata = record_body.get("metadata", {})

            public_entry, restricted_entry = strategy.build_entries()

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
                assert restricted_record_state is not None

                # Parent access grants + communities before EP approval request
                # (needs parent.communities.default for referee group lookup).
                self.parent_load_cls(
                    restricted_entry, self.migration_logger, restricted_record_state
                ).load(published_record=restricted_records[-1], uow=uow)

                strategy.after_restricted_parent(uow, restricted_record_state)

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
                        message="Public record is required for record splitting.",
                        stage="load",
                        recid=recid,
                        priority="critical",
                    )

                # Load access grants + communities for the public record
                self.parent_load_cls(
                    public_entry, self.migration_logger, public_record_state
                ).load(published_record=public_records[-1], uow=uow)

                # Original-dump persistence for the public half.
                self._save_original_dumped_record(
                    public_entry, public_record_state, uow
                )

                # Link the records with related_identifiers
                public_to_restricted_relation_id, public_to_restricted_resource_type = (
                    strategy.public_relation_to_restricted()
                )
                self._append_related_identifier(
                    public_record_state["latest_version"],
                    restricted_record_state["latest_version"],
                    public_to_restricted_relation_id,
                    public_to_restricted_resource_type,
                    uow=uow,
                )
                restricted_to_public_relation_id, restricted_to_public_resource_type = (
                    strategy.restricted_relation_to_public()
                )
                self._append_related_identifier(
                    restricted_record_state["latest_version"],
                    public_record_state["latest_version"],
                    restricted_to_public_relation_id,
                    restricted_to_public_resource_type,
                    uow=uow,
                )

                # 4. Write EP approval metadata on both parents
                strategy.after_both_loaded(
                    uow, public_record_state, restricted_record_state
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
