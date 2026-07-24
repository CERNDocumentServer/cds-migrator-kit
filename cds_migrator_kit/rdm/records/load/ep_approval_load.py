# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM migration load module for records with EP approval."""
import json

from cds_rdm.legacy.resolver import get_pid_by_legacy_recid
from cds_rdm.minters import legacy_recid_minter
from invenio_access.permissions import system_identity
from invenio_db import db
from invenio_db.uow import UnitOfWork
from invenio_drafts_resources.services.records.uow import ParentRecordCommitOp
from invenio_pidstore.models import PersistentIdentifier
from invenio_rdm_migrator.load.base import Load
from invenio_rdm_records.proxies import current_rdm_records_service
from invenio_rdm_records.records.api import RDMParent

from cds_migrator_kit.errors import ManualImportRequired, UnexpectedValue

from .approval_request import ApprovalRequest
from .ep_approval_entry import PublicEntry, RestrictedEntry
from .load import CDSRecordServiceLoad


class CDSEPApprovalRecordServiceLoad(Load):
    """Load records with EP approval.

    Splits a legacy record into two RDM records before load:
    - a public record with non-EPPHAPP files
    - a restricted record with restricted EPPHAPP files
    """

    def __init__(
        self,
        db_uri,
        data_dir,
        entries=None,
        dry_run=False,
        legacy_pids_to_redirect=None,
        collection=None,
        update_new_version_publication_date=False,
        create_inclusion_request=False,
        migration_logger=None,
        record_state_logger=None,
    ):
        self.dry_run = dry_run
        self.legacy_pids_to_redirect = {}
        self.clc_sync = False
        self.collection = collection
        self.update_new_version_publication_date = update_new_version_publication_date
        self.create_inclusion_request = create_inclusion_request
        self.migration_logger = migration_logger
        self.record_state_logger = record_state_logger
        self.approval_request = None
        if legacy_pids_to_redirect is not None:
            with open(legacy_pids_to_redirect, "r") as fp:
                self.legacy_pids_to_redirect = json.load(fp)

    def _load(self, entry):
        """
        Load the record with EP approval.
        Configure the 2 records by separating the files, then:
        1. create the restricted record
        2. create and approve the EP approval request
        3. create the public record and link both with related_identifiers

        Steps 1-4 are grouped into a single unit of work so a failure at any
        point rolls back everything, instead of leaving an orphaned
        restricted record and/or approval request committed behind.
        """
        if not entry:
            return
        try:
            recid = entry.get("record", {}).get("recid")

            # The same legacy recid can be cross-listed under multiple EP
            # collections (e.g. a joint ALEPH/DELPHI/L3/OPAL paper appears in
            # all four experiments' dumps). Once one pass has fully migrated
            # it, later passes should skip cleanly instead of failing on the
            # already-created approval request.
            if CDSRecordServiceLoad._have_migrated_recid(recid):
                self.migration_logger.add_information(
                    recid,
                    state={"message": "Record already migrated", "value": recid},
                )
                self.migration_logger.finalise_record(recid)
                return

            ep_approval = entry.get("record", {}).get("ep_approval")
            if not ep_approval:
                raise UnexpectedValue(
                    message="EP approval request not found",
                    stage="load",
                    recid=recid,
                    priority="critical",
                )
            record_json = entry.get("record", {}).get("json", {})
            metadata = record_json.get("metadata", {})

            self.approval_request = ApprovalRequest(
                ep_approval=ep_approval,
                legacy_recid=recid,
                title=metadata.get("title"),
                resource_type=metadata.get("resource_type"),
                dry_run=self.dry_run,
            )
            self.approval_request.validate()

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

            restricted_record_service = CDSRecordServiceLoad(
                dry_run=self.dry_run,
                collection=self.collection,
                create_inclusion_request=self.create_inclusion_request,
                migration_logger=self.migration_logger,
                record_state_logger=self.record_state_logger,
                legacy_pids_to_redirect=self.legacy_pids_to_redirect,
                _is_final_record=False,
            )
            public_record_service = CDSRecordServiceLoad(
                dry_run=self.dry_run,
                collection=self.collection,
                create_inclusion_request=self.create_inclusion_request,
                migration_logger=self.migration_logger,
                record_state_logger=self.record_state_logger,
                legacy_pids_to_redirect=self.legacy_pids_to_redirect,
                _is_final_record=True,
            )

            if self.dry_run:
                # 1. Create restricted record
                restricted_record_state = restricted_record_service._load(
                    restricted_entry
                )
                # 2. Create and approve EP approval request
                self.approval_request.create(restricted_record_state)
                # 3. Create public record
                public_record_service._load(public_entry)
                return

            with UnitOfWork(db.session) as uow:
                # 1. Create restricted record
                restricted_record_state = restricted_record_service._load(
                    restricted_entry, uow=uow
                )

                # 2. Create and approve EP approval request
                self.approval_request.create(restricted_record_state, uow=uow)

                # 3. Create public record
                public_record_state = public_record_service._load(
                    public_entry, uow=uow
                )
                if not public_record_state:
                    raise UnexpectedValue(
                        message="Public record is required for EP approval.",
                        stage="load",
                        recid=recid,
                        priority="critical",
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

                # The public record is the final one; finalise it only now
                # that the whole split has actually committed (see the
                # matching `uow is None` guard in CDSRecordServiceLoad._load).
                self.migration_logger.finalise_record(recid)
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
        public_parent = RDMParent.get_record(
            public_record_state["parent_object_uuid"]
        )

        self._write_parent_ep_approval(
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
        self._write_parent_ep_approval(
            public_parent,
            {
                "reportnumber": report_number,
                "source_internal_version": restricted_recid,
            },
            uow,
        )

    def _write_parent_ep_approval(
        self,
        parent,
        ep_approval,
        uow,
    ):
        """Write the EP approval metadata to the parent record."""
        pf = parent.get("permission_flags") or {}
        pf["committee_approval"] = ep_approval
        parent["permission_flags"] = pf
        uow.register(ParentRecordCommitOp(parent))

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

    def _cleanup(self, *args, **kwargs):
        """Post migration process."""
        for legacy_src_pid, legacy_dest_pid in self.legacy_pids_to_redirect.items():
            if CDSRecordServiceLoad._have_migrated_recid(legacy_src_pid):
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
