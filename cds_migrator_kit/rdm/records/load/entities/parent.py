# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Materializes a ``RecordParent`` against a real RDM parent record."""
from flask import current_app
from invenio_access.permissions import system_identity
from invenio_accounts.models import User
from invenio_drafts_resources.services.records.uow import ParentRecordCommitOp
from invenio_pidstore.errors import PIDAlreadyExists
from invenio_pidstore.models import PersistentIdentifier, PIDStatus
from invenio_rdm_records.proxies import current_rdm_records_service

from cds_migrator_kit.errors import GrantCreationError, ManualImportRequired
from cds_migrator_kit.rdm.records.transform.entities.migration import MigrationEntry
from cds_migrator_kit.rdm.records.transform.entities.parent import RecordParent


class ParentLoad:
    """Loads a ``RecordParent``'s access/communities/grants onto a real RDM parent.

    Takes the already-built transform-side ``RecordParent`` (access/
    communities computed, access grants resolved from legacy metadata) and
    applies it to the parent of a draft/published record - the load-side
    counterpart of ``RecordParent``, the way ``load.py``'s record-loading
    methods are the counterpart of ``RecordEntry``.
    """

    def __init__(self, entry: MigrationEntry, migration_logger, record_state):
        """Constructor.

        :param record_parent: the built ``RecordParent`` for this entry
            (``MigrationEntry["parent"]``).
        :param migration_logger: kept for parity with the other load-side
            entity collaborators; not used directly by this class today.
        """
        self.record_parent = entry["parent"]
        self.migration_logger = migration_logger
        self.record_state = record_state
        self.migration_entry = entry
        self.versions = entry["versions"]
        self.ep_approval = entry.get("ep_approval")

    def load(self, published_record, uow):
        """Load access/communities, then access grants for every version.

        Called once from ``CDSMigrationEntryLoad._load()`` after the whole
        entry has published successfully, with the latest published
        record. Access/communities are parent-level (shared across all
        versions), so applying them via the latest record still sets them
        correctly for the parent as a whole.

        Order matters: ``load_access_and_communities`` replaces the whole
        ``parent.access`` field wholesale from ``record_parent.body``, so it
        must run *before* ``load_access_grants`` - otherwise it would
        overwrite the grants just created (each committed on its own
        freshly-read parent instance, which ``load_access_and_communities``'s
        stale in-memory parent doesn't see).
        """
        self.load_access_and_communities(published_record)
        self.load_access_grants(published_record)
        self._set_committee_approval(published_record, uow)

    def load_access_and_communities(self, draft):
        """Load access rights and communities in a single parent commit."""
        parent = draft._record.parent
        parent.access = self.record_parent.body["access"]
        for community in self.record_parent.communities["ids"]:
            parent.communities.add(community)
        parent.communities.default = self.record_parent.communities["default"]
        parent.commit()

    def load_access_grants(self, published_record):
        """Load access grants from metadata and record grants efficiently.

        :param draft: the draft/published record whose parent grants are set.
        :param recid: this record's legacy recid, for error reporting.
        """
        recid = self.migration_entry["record"].recid
        # `record_state["versions"]` is keyed by invenio's own sequential
        # `record.versions.index` (always 1, 2, 3, ...), while `self.versions`
        # (`entry["versions"]`) is keyed by the *legacy* version number, which
        # can start above 1 when the legacy version 1 was hard-deleted (see
        # ``RecordLoad.pre_publish``). Both lists are built by iterating the
        # same records in the same order (``RecordLoad._load_versions``), so
        # match them up by position rather than by (possibly mismatched) key.
        legacy_versions = list(self.versions.keys())
        for legacy_version, version_state in zip(
            legacy_versions, self.record_state["versions"]
        ):
            access_dict = self.versions[legacy_version]["access"]
            published_record = current_rdm_records_service.read(
                system_identity, version_state["new_recid"]
            )

            parent = published_record._record.parent
            identity = system_identity

            specific_file_restrictions = access_dict.get("meta", "")
            if not specific_file_restrictions and not self.record_parent.access_grants:
                continue
            default_permission = "view"

            groups, emails, grants_with_perms = self.record_parent.resolve_grants(
                specific_file_restrictions
            )

            def _create_grant(subject_type, subject_id, permission):
                grant_data = {
                    "grants": [
                        {
                            "subject": {"type": subject_type, "id": str(subject_id)},
                            "permission": permission,
                        }
                    ]
                }
                current_rdm_records_service.access.schema_grants.load(
                    grant_data,
                    context={"identity": identity},
                    raise_errors=True,
                )

                grant = parent.access.grants.create(
                    subject_type=subject_type,
                    subject_id=subject_id,
                    permission=permission,
                    origin="migrated",
                )
                is_local_dev = current_app.config.get("CDS_MIGRATOR_KIT_ENV") == "local"
                is_valid = current_rdm_records_service.access._validate_grant_subject(
                    identity, grant
                )
                if not is_local_dev and not is_valid:
                    raise ManualImportRequired(
                        message="Verification of access subject failed (likely not existing entry)",
                        field="access",
                        subfield="subject.id",
                        stage="load",
                        recid=recid,
                        priority="warning",
                        value=subject_id,
                    )

            # Create grants for groups
            for group in groups:
                _create_grant(
                    subject_type="role",
                    subject_id=group.lower(),
                    permission=grants_with_perms.get(group, default_permission),
                )

            # Fetch existing users
            existing_users = {
                user.email: user.id
                for user in User.query.filter(User.email.in_(emails)).all()
            }
            # raise error for missing user
            missing_emails = emails - existing_users.keys()
            if missing_emails:
                raise GrantCreationError(
                    message=f"Users not found for emails: {', '.join(missing_emails)}",
                    stage="load",
                    recid=recid,
                    value=list(missing_emails),
                    priority="warning",
                )

            # Create grants for users
            for email, user_id in existing_users.items():
                _create_grant(
                    subject_type="user",
                    subject_id=user_id,
                    permission=grants_with_perms.get(email, default_permission),
                )

            parent.commit()

    @staticmethod
    def write_committee_approval(parent, ep_approval, uow):
        """Write EP approval metadata onto an already-published parent.

        :param parent: an ``RDMParent`` fetched by uuid (post-publish - not
            the parent of a draft in this uow; see the EP approval split's
            restricted/public parents in ``load.py``).
        :param ep_approval: the ``permission_flags.committee_approval`` dict
            to write.
        :param uow: registers the commit op on it without committing.
        """
        pf = parent.get("permission_flags") or {}
        pf["committee_approval"] = ep_approval
        parent["permission_flags"] = pf
        uow.register(ParentRecordCommitOp(parent))

    def _set_committee_approval(self, published_record, uow):
        """Write committee_approval to parent for records already EP-approved pre-migration.

        Only runs when:
          1. entry["ep_approval"] is empty — record did NOT go through the
             9031_/EPPHAPP path (those are handled by CDSEPApprovalRecordServiceLoad
             which already writes committee_approval correctly for both records).
          2. The record carries at least one apprn identifier.

        For these records the migrated record IS the final public version (no
        separate internal draft exists). We write the same "public side"
        committee_approval block that ep_approval_load writes, pointing
        source_internal_version at the record's own PID so that
        get_committee_approval_state returns is_public_approved_record=True.
        """
        if self.ep_approval:
            return

        record = published_record._record
        identifiers = record.get("metadata", {}).get("identifiers", [])
        apprn_ids = [i["identifier"] for i in identifiers if i.get("scheme") == "apprn"]
        if not apprn_ids:
            return

        parent = record.parent
        pf = parent.get("permission_flags") or {}
        if pf.get("committee_approval", {}).get("source_internal_version"):
            return  # idempotency: already written

        pf["committee_approval"] = {
            "source_internal_version": str(record.pid.pid_value),
            "reportnumber": apprn_ids[0],
        }
        parent["permission_flags"] = pf
        uow.register(ParentRecordCommitOp(parent))

        # Mint apprn PIDs in pidstore — same logic as ApprovalRequest._mint_apprn_pid.
        from cds_rdm.requests.committee_approval import APPRN_PID_TYPE
        for apprn_value in apprn_ids:
            try:
                PersistentIdentifier.create(
                    pid_type=APPRN_PID_TYPE,
                    pid_value=apprn_value,
                    object_type="rec",
                    object_uuid=str(record.id),
                    status=PIDStatus.REGISTERED,
                )
            except PIDAlreadyExists:
                pass  # already minted on a previous run — idempotent
