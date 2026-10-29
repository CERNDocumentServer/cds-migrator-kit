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
        migration_logger=None,
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
        :param migration_logger: where non-blocking warnings of this record
            are logged (e.g. extra members in a restricted community).
        """
        self.record = record
        self.raw_dump_entry = raw_dump_entry
        self.dojson_entry = dojson_entry
        self.communities_ids = communities_ids
        self.migration_logger = migration_logger
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
            if field == "270__m":
                # For now, we don't consider 270__m to represent owners but rather submitters
                # or contact persons. So we ignore these.
                continue

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

    def resource_type_applicable_for_scientific_community(self):
        """Whether the resource type is applicable for the scientific community."""
        resource_type_id = (
            self.record.body.get("metadata", {}).get("resource_type", {}).get("id")
        )
        return resource_type_id in CERN_SCIENTIFIC_RESOURCE_TYPES

    def should_add_scientific_community(self):
        """Whether to add the scientific community during the transform step.

        If the record has mixed public/restricted files but the resource type is applicable for the scientific
        community, it will get added during the load step in `split_entries.py` instead.
        """
        return (
            not self.record.has_any_restrictions()
            and self.resource_type_applicable_for_scientific_community()
        )

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
        if self.should_add_scientific_community():
            if scientific_community not in communities:
                communities.append(scientific_community)

        if communities:
            return {"ids": communities, "default": self.communities_ids[0]}
        return {}

    def _build_access_grants_from_record_dump(self) -> list[dict[str, str]]:
        """Compute the access grants to create on this parent after publish."""
        restrictions: RecordRestrictions = self.raw_dump_entry["restrictions"]
        access_grants = []

        for field in restrictions["record"]["viewers"]:
            if field == "270__m":
                # Shouldn't exist here but we'll skip just in case, for the same
                # reason as above (for restrictions.record.owners)
                continue

            access_grants.extend(
                {grant: "view"} for grant in restrictions["record"]["viewers"][field]
            )

        return access_grants

    # TODO: move to loader
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

        # TODO: the priority application should be in one place

        # This should only do simple extraction, the loader should have all the business logic

        # ----Parse file status metadata----#
        groups, emails = self._resolve_file_restriction_groups_and_emails()
        file_restrictions_empty = not groups and not emails

        assert self.access_grants is not None
        # ----Parse record access grants----#
        for grant_info in self.access_grants:
            subject, permission = next(iter(grant_info.items()))
            permission = permission or default_permission
            grants_with_perms[subject] = permission

            # attention!
            # this is important - if there were no restrictions on the files,
            # then the record grants take over - but if a file had a specific status,
            # then we take the least possible access
            if file_restrictions_empty:
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
            # TODO: cern-accounts-primary
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
