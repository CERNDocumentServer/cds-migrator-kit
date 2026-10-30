# -*- coding: utf-8 -*-
#
# Copyright (C) 2022-2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""The RDM parent record for one migrated CDS record."""

import re

from flask import current_app
from invenio_accounts.models import User
from sqlalchemy.exc import NoResultFound

from cds_migrator_kit.errors import (
    ManualImportRequired,
    MissingConfiguration,
    UnexpectedValue,
)
from cds_migrator_kit.rdm.records.transform.config import CERN_SCIENTIFIC_RESOURCE_TYPES
from cds_migrator_kit.rdm.records.transform.entities.community_members import (
    CommunityMembersCheck,
)
from cds_migrator_kit.rdm.records.transform.entities.restrictions import (
    RecordRestrictions,
    RecordRestrictionsFile,
    legacy_role_subjects,
    normalize_group_name,
    parse_firerole,
)
from cds_migrator_kit.rdm.records.transform.mappers.base import RecordTransformContext
from cds_migrator_kit.rdm.records.transform.mappers.record import AccessGrantsMapper

EMAIL_PATTERN = re.compile(r"[^@]+@[^@]+\.[^@]+")

# Cache submitter email → user_id across records.  Stable for one migration
# run since users are created before the records stream starts.
_submitter_id_by_email: dict = {}


class RecordParent:
    """The parent record for one migrated CDS record.

    Owns everything needed to build the RDM parent's access, community
    membership, and access grants - built by ``build()``, called from
    ``CDSToRDMRecordTransform._transform()`` (the one place that has both
    the already-built record content and the DOJSON-processed
    ``dojson_entry`` that access-grant resolution needs).
    """

    def __init__(
        self,
        record,
        raw_dump_entry,
        dojson_entry,
        communities_ids,
        access_grants_view,
        migration_logger=None,
        access_collections=None,
    ):
        """Constructor.

        :param record: the already-built ``RecordEntry`` (needs ``recid``).
        :param raw_dump_entry: the original harvested legacy entry (needs
            ``recid``, for error reporting).
        :param dojson_entry: the DOJSON-processed record data - owner/
            communities/access grants are popped straight off it
            (``submitter``/``communities``/``access_grants``) - this is the
            last entity to touch it, so it's also where those keys stop
            existing for the forgotten-keys check in
            ``CDSToRDMRecordTransform._check_forgotten_keys()``.
        :param communities_ids: configured target community ids for this
            migration run (``CDSToRDMRecordTransform.communities_ids``).
        :param access_grants_view: configured collection-wide view grants
            (``CDSToRDMRecordTransform.access_grants_view``).
        :param migration_logger: where non-blocking warnings of this record
            are logged (e.g. extra members in a restricted community).
        :param access_collections: the legacy collections whose roles are
            checked against the target communities' members, see
            ``CommunityMembersCheck``.
        """
        self.record = record
        self.raw_dump_entry = raw_dump_entry
        self.dojson_entry = dojson_entry
        self.communities_ids = communities_ids
        self.access_grants_view = access_grants_view
        self.migration_logger = migration_logger
        self.access_collections = access_collections
        self.body = None
        self.communities = None
        self.access_grants = None

    def build(self):
        """Populate ``body``/``communities``/``access_grants``; returns self."""
        # Owners and viewers are read from the dump's `restrictions`: the
        # values the MARC rules produce are discarded, so that they are not
        # flagged by the forgotten-keys check
        self.dojson_entry.pop("submitter", None)
        self.dojson_entry.pop("access_grants", None)
        access = self._build_access()
        CommunityMembersCheck(
            restrictions=self.raw_dump_entry["restrictions"],
            communities_ids=self.communities_ids,
            recid=self.record.recid,
            migration_logger=self.migration_logger,
            access_collections=self.access_collections,
        ).check()
        self.communities = self._build_communities()
        self.access_grants = self._build_access_grants_from_record_dump()
        self.body = {
            # loader is responsible for creating/updating if the PID exists,
            # this part will be simply omitted.
            "id": f"{self.record.recid}-parent",
            "access": access,
            "communities": self.communities,
        }
        return self

    def _build_access(self):
        """Resolve the owner and return the parent's access dict."""
        restrictions: RecordRestrictions = self.raw_dump_entry["restrictions"]
        owners: set[str] = set()
        for field in restrictions["record"]["owners"]:
            owners.update(restrictions["record"]["owners"][field])

        if len(owners) > 1:
            raise ManualImportRequired(
                message="Cannot handle records with more than one owner",
                field="owned_by",
                subfield="user",
                stage="transform",
                recid=self.record.recid,
                priority="critical",
                value=owners,
            )

        email = None
        if owners:
            email = owners.pop()
        if not email:
            owner = "system"
        elif email in _submitter_id_by_email:
            owner = _submitter_id_by_email[email]
        else:
            try:
                user = User.query.filter_by(email=email).one()
                _submitter_id_by_email[email] = user.id
                owner = user.id
            except NoResultFound:
                raise UnexpectedValue(
                    message=f"{email} not found - did you run user migration?",
                    stage="transform",
                    recid=self.raw_dump_entry["recid"],
                    value=email,
                    priority="critical",
                )
        return {"owned_by": {"user": owner}}

    def _should_add_scientific_community(self):
        if self.record.has_any_restrictions():
            return False
        resource_type_id = (
            self.record.body.get("metadata", {}).get("resource_type", {}).get("id")
        )
        return resource_type_id in CERN_SCIENTIFIC_RESOURCE_TYPES

    def _build_communities(self):
        """Combine the configured target communities with the record's own."""
        communities = self.dojson_entry.pop("communities", [])
        communities = self.communities_ids + [slug for slug in communities]

        scientific_community = current_app.config.get(
            "CDS_CERN_SCIENTIFIC_COMMUNITY_ID"
        )
        if not scientific_community:
            raise MissingConfiguration(
                "CDS_CERN_SCIENTIFIC_COMMUNITY_ID is not configured"
            )
        if self._should_add_scientific_community():
            if scientific_community not in communities:
                communities.append(scientific_community)

        if communities:
            return {"ids": communities, "default": self.communities_ids[0]}
        return {}

    def _build_access_grants_from_record_dump(self):
        """Compute the access grants to create on this parent after publish."""
        ctx = RecordTransformContext(
            dojson_entry=self.dojson_entry,
            raw_dump_entry=self.raw_dump_entry,
            access_grants_view=self.access_grants_view,
        )
        # This creates view grants for users included in the record dump's
        # `restrictions` dict as viewers.
        return AccessGrantsMapper().map_value(ctx)

    def resolve_grants(self):
        """Resolve which groups/emails/permissions get access grants for a version.

        Combines this parent's own ``access_grants`` (record-level, from the
        dump's ``restrictions["record"]["viewers"]``) with the file
        restrictions in ``restrictions["files"]``. Pure computation - the
        caller (``load/entities/parent.py``) is responsible for actually
        creating the grants against the RDM parent.

        :return: ``(groups, emails, grants_with_perms)`` - see
            ``ParentLoad`` for how these are consumed to create the grants.
        """
        default_permission = "view"
        grants_with_perms = {}

        # ----Parse file status metadata----#
        groups, emails = self._resolve_file_restriction_groups_and_emails()

        file_restrictions: list[RecordRestrictionsFile] = self.raw_dump_entry[
            "restrictions"
        ]["files"]

        # ----Parse record access grants----#
        for grant_info in self.access_grants:
            if not isinstance(grant_info, dict) or not grant_info:
                continue

            subject, permission = next(iter(grant_info.items()))
            permission = permission or default_permission
            grants_with_perms[subject] = permission

            # attention!
            # this is important - if there was no specific restrictions on the file,
            # then the record grands takes over - but if file had specific status,
            # then we take the least possible access
            if not file_restrictions:
                if EMAIL_PATTERN.match(subject):
                    emails.add(subject)
                else:
                    groups.add(self._normalize_group_name(subject))

        return groups, emails, grants_with_perms

    def _resolve_file_restriction_groups_and_emails(self):
        """Resolve the groups/emails that can view the record's restricted files.

        Unions ``_resolve_file_restriction()`` over every entry of the dump's
        ``restrictions["files"]`` (which only lists files with a non-empty
        ``status``). ``RecordVersion`` already rejects records whose restricted
        files don't share a single status.

        :return: ``(groups, emails)`` - empty when no file is restricted.
        """
        groups = set()
        emails = set()
        file_restrictions: list[RecordRestrictionsFile] = self.raw_dump_entry[
            "restrictions"
        ]["files"]
        for file_restriction in file_restrictions:
            file_groups, file_emails = self._resolve_file_restriction(file_restriction)
            groups.update(file_groups)
            emails.update(file_emails)
        return groups, emails

    def _resolve_file_restriction(self, file_restriction: RecordRestrictionsFile):
        """Resolve the groups/emails implied by one restricted file.

        Covers every form a legacy file ``status`` can take (see
        ``docs/legacy_restriction_paths.md``), in order: the literal
        ``"restricted"``, a bare ``"<name> [CERN]"`` e-group name, a
        ``firerole:`` expression, a key in ``CDS_ACCESS_GROUP_MAPPINGS``
        (curated override), and finally the legacy roles authorised for the
        status, as dumped in the file's ``roles``. Raises
        ``ManualImportRequired`` for anything unrecognised.
        """
        status = file_restriction["status"].strip()
        roles = file_restriction["roles"]
        group_mappings = current_app.config.get("CDS_ACCESS_GROUP_MAPPINGS", {})

        def _manual_import_required(message):
            return ManualImportRequired(
                message=message,
                field="access",
                subfield="subject.id",
                stage="transform",
                recid=self.record.recid,
                priority="critical",
                value=status,
            )

        if status == "SSO" or status == "restricted":
            # https://cds.cern.ch/admin/webaccess/webaccessadmin.py/showroledetails?id_role=69
            return {"cern-personnel"}, set()
        if status.endswith("[CERN]") and not any(
            kw in status for kw in ("firerole:", "allow ")
        ):
            # bare CERN e-group name, e.g.
            # "cds-ph-ep-publications-referee-non-lhc [CERN]"
            return {self._normalize_group_name(status)}, set()
        if status.startswith("firerole:"):
            try:
                return parse_firerole(status)
            except ValueError as e:
                raise _manual_import_required(f"Unexpected permission format: {e}")
        if status in group_mappings:
            # a simple keyword mapped to CERN e-group(s)
            return set(group_mappings[status]), set()
        if roles:
            try:
                return legacy_role_subjects(roles)
            except ValueError as e:
                raise _manual_import_required(
                    f"Cannot map the roles of file status '{status}': {e}"
                )
        raise _manual_import_required("Unexpected permission format.")

    @staticmethod
    def _normalize_group_name(subject):
        return normalize_group_name(subject)
