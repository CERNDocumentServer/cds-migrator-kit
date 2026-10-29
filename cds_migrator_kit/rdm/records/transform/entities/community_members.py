# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Checks of the legacy roles against the members of the target RDM communities."""

from functools import lru_cache

from flask import current_app
from invenio_accounts.models import Role, User
from invenio_communities.members.records.models import MemberModel
from invenio_db import db

from cds_migrator_kit.errors import ManualImportRequired
from cds_migrator_kit.rdm.records.transform.entities.restrictions import (
    RecordRestrictions,
    legacy_role_subjects,
)


# Members don't change during a migration run: query each community only once.
@lru_cache
def community_subjects(community_id: str) -> tuple[frozenset[str], frozenset[str]]:
    """Return the ``(group names, user emails)`` of a community's active members.

    Reads the DB directly rather than the search index, so that members added
    right before the run are not missed. Pending invitations are not active.
    """
    members = MemberModel.query.filter_by(community_id=community_id, active=True)

    group_ids = [m.group_id for m in members if m.group_id]
    user_ids = [m.user_id for m in members if m.user_id]

    groups = frozenset(
        name for (name,) in db.session.query(Role.name).filter(Role.id.in_(group_ids))
    )
    emails = frozenset(
        email.lower()
        for (email,) in db.session.query(User.email).filter(User.id.in_(user_ids))
    )
    return groups, emails


class CommunityMembersCheck:
    """Checks the target communities' members cover the legacy roles of a record.

    Run by ``RecordParent.build()``. Raises ``ManualImportRequired`` when
    someone who could view or edit the record on legacy would lose access on
    RDM.
    """

    def __init__(
        self,
        restrictions: RecordRestrictions,
        communities_ids,
        recid,
        migration_logger=None,
        access_collections=None,
    ):
        """Constructor.

        :param restrictions: the dump's ``restrictions`` of the record.
        :param communities_ids: configured target community ids for this
            migration run (``CDSToRDMRecordTransform.communities_ids``).
        :param recid: the legacy record id, for error reporting.
        :param migration_logger: where non-blocking warnings of this record
            are logged (e.g. extra members in a restricted community).
        :param access_collections: the legacy collections whose roles define
            the access of the stream's records (stream config). Legacy records
            belong to many collections, but their access is normally set by
            only one, so the roles of the others are not checked. ``None``
            checks the roles of all the record's collections.
        """
        self.restrictions = restrictions
        self.communities_ids = communities_ids
        self.recid = recid
        self.migration_logger = migration_logger
        self.access_collections = access_collections

    def check(self):
        """Run all the community members checks."""
        self._check_restricted_collections()
        self._check_edit_roles()

    def _check_restricted_collections(self):
        """Check the target communities give access to the restricted collection.

        A legacy collection is restricted when it has roles. Everyone covered by
        these roles must be a member of each configured target community, or
        they would lose access on RDM. Extra community members are allowed, but
        logged. Records in several restricted collections can't be mapped to one
        set of members, so they need manual curation.
        """
        restricted = {
            name: roles
            for name, roles in self._access_relevant_collections(
                self.restrictions["collections"]
            ).items()
            if roles
        }
        if not restricted:
            return

        recid = self.recid
        if len(restricted) > 1:
            raise ManualImportRequired(
                message="Record belongs to more than one restricted collection",
                field="restrictions",
                subfield="collections",
                stage="transform",
                recid=recid,
                priority="critical",
                value=sorted(restricted),
            )

        name, roles = restricted.popitem()
        groups, emails = self._check_missing_community_members(
            roles,
            f"read access to the restricted legacy collection '{name}'",
        )

        for community_id in self._member_checked_communities_ids():
            # Calculate which roles the community has extra relative to the legacy
            # CDS collection (missing ones were checked above)
            community_groups, community_emails = community_subjects(community_id)

            extra = (community_groups - groups) | (community_emails - emails)
            if extra and self.migration_logger:
                # Don't raise if the community has too many members (relative to the
                # roles on the legacy collection). It is perfectly reasonable that
                # we manually add some new members during the migration, so we should just
                # add a message rather than an error.
                self.migration_logger.add_information(
                    recid,
                    {
                        "message": (
                            f"Community {community_id} has members that are not in "
                            f"the roles of restricted collection '{name}'"
                        ),
                        "value": ", ".join(sorted(extra)),
                    },
                )

    def _check_edit_roles(self):
        """Check everyone who could edit the legacy record is a community member.

        Applies to every record, restricted or not: the roles allowed to edit
        the record's access collections. The roles allowed to edit any
        collection (``any_collection``) are left out: they are the few
        instance-wide curators, who must not become community members.
        """
        collections = self._access_relevant_collections(
            self.restrictions["edit"]["collections"]
        )
        roles = [role for roles in collections.values() for role in roles]
        self._check_missing_community_members(
            roles,
            "legacy edit/curation permissions",
        )

    def _access_relevant_collections(self, collections):
        """Keep only the entries of ``collections`` for the access collections."""
        if self.access_collections is None:
            return collections
        return {
            name: roles
            for name, roles in collections.items()
            if name in self.access_collections
        }

    def _member_checked_communities_ids(self):
        """Target communities whose members must match the legacy roles.

        The CERN Research community is a common community for all streams, so
        its members are not expected to match any collection's roles.
        """
        scientific_community = current_app.config.get(
            "CDS_CERN_SCIENTIFIC_COMMUNITY_ID"
        )
        return [
            community_id
            for community_id in self.communities_ids
            if str(community_id) != str(scientific_community)
        ]

    def _check_missing_community_members(self, roles, label):
        """Raise unless everyone covered by ``roles`` is in each target community.

        :param label: what the roles are, for the error message.
        :return: ``(groups, emails)`` covered by ``roles``.
        """

        def _manual_import_required(message, value):
            return ManualImportRequired(
                message=message,
                field="restrictions",
                stage="transform",
                recid=self.recid,
                priority="critical",
                value=value,
            )

        try:
            groups, emails = legacy_role_subjects(roles)
        except ValueError as e:
            raise _manual_import_required(
                f"Cannot map the {label}: {e}",
                [role["firerole_def_src"] for role in roles],
            )

        if not groups and not emails:
            return groups, emails

        for community_id in self._member_checked_communities_ids():
            community_groups, community_emails = community_subjects(community_id)
            missing = (groups - community_groups) | (emails - community_emails)
            if missing:
                raise _manual_import_required(
                    f"Community {community_id} is missing members with {label}",
                    sorted(missing),
                )

        return groups, emails
