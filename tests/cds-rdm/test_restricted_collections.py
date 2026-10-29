# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Tests for the restricted collection roles vs. target community members check."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from invenio_accounts.models import Role
from invenio_communities.members.records.models import MemberModel

from cds_migrator_kit.errors import ManualImportRequired
from cds_migrator_kit.rdm.records.transform.entities import (
    community_members as community_members_module,
)
from cds_migrator_kit.rdm.records.transform.entities.community_members import (
    CommunityMembersCheck,
    community_subjects,
)
from cds_migrator_kit.rdm.records.transform.entities.restrictions import (
    legacy_role_subjects,
    parse_firerole,
)


def make_role(firerole_def_src="", users=()):
    return {
        "id": 1,
        "name": "SomeRole",
        "description": "",
        "firerole_def_src": firerole_def_src,
        "users": [{"email": email} for email in users],
    }


NA61_ROLE = make_role('allow group "cds-edboard-na61 [CERN]"', ["Jane.Doe@cern.ch"])


def make_check(
    collections,
    members,
    monkeypatch,
    communities_ids=("comm-a",),
    edit=None,
    access_collections=None,
):
    """Check of the given collections, against communities with ``members``."""
    monkeypatch.setattr(
        community_members_module,
        "community_subjects",
        lambda community_id: members[community_id],
    )
    monkeypatch.setattr(
        community_members_module, "current_app", SimpleNamespace(config={})
    )
    return CommunityMembersCheck(
        restrictions={
            "collections": collections,
            "edit": edit or {"collections": {}, "any_collection": []},
        },
        communities_ids=list(communities_ids),
        recid="123",
        migration_logger=MagicMock(),
        access_collections=access_collections,
    )


class TestParseFirerole:
    def test_groups_and_emails(self):
        groups, emails = parse_firerole(
            'allow group "a [CERN]", "b [CERN]"\r\nallow email "X@cern.ch"\ndeny any'
        )
        assert groups == {"a", "b"}
        assert emails == {"x@cern.ch"}

    def test_firerole_prefix(self):
        assert parse_firerole('firerole: allow group "a [CERN]"') == ({"a"}, set())

    @pytest.mark.parametrize(
        "src", ["allow any", 'deny group "a [CERN]"', 'allow remote_ip "1.2.3.4"']
    )
    def test_unsupported(self, src):
        with pytest.raises(ValueError):
            parse_firerole(src)

    def test_role_subjects(self):
        assert legacy_role_subjects([NA61_ROLE, make_role(users=["b@cern.ch"])]) == (
            {"cds-edboard-na61"},
            {"jane.doe@cern.ch", "b@cern.ch"},
        )


class TestCheckRestrictedCollections:
    def test_no_restricted_collection(self, monkeypatch):
        check = make_check({"NA61": [], "NA61 Papers": []}, {}, monkeypatch)
        check._check_restricted_collections()
        check.migration_logger.add_information.assert_not_called()

    def test_several_restricted_collections(self, monkeypatch):
        check = make_check({"A": [NA61_ROLE], "B": [NA61_ROLE]}, {}, monkeypatch)
        with pytest.raises(ManualImportRequired):
            check._check_restricted_collections()

    def test_exact_match(self, monkeypatch):
        members = {"comm-a": ({"cds-edboard-na61"}, {"jane.doe@cern.ch"})}
        check = make_check({"NA61": [NA61_ROLE]}, members, monkeypatch)
        check._check_restricted_collections()
        check.migration_logger.add_information.assert_not_called()

    def test_missing_member(self, monkeypatch):
        members = {
            "comm-a": ({"cds-edboard-na61"}, {"jane.doe@cern.ch"}),
            "comm-b": ({"cds-edboard-na61"}, set()),
        }
        check = make_check(
            {"NA61": [NA61_ROLE]}, members, monkeypatch, ("comm-a", "comm-b")
        )
        with pytest.raises(ManualImportRequired) as e:
            check._check_restricted_collections()
        assert "comm-b" in e.value.message
        assert e.value.value == ["jane.doe@cern.ch"]

    def test_extra_member_is_logged(self, monkeypatch):
        members = {
            "comm-a": ({"cds-edboard-na61", "curators"}, {"jane.doe@cern.ch"}),
        }
        check = make_check({"NA61": [NA61_ROLE]}, members, monkeypatch)
        check._check_restricted_collections()
        check.migration_logger.add_information.assert_called_once()
        _, state = check.migration_logger.add_information.call_args.args
        assert state["value"] == "curators"

    def test_only_access_collections_checked(self, monkeypatch):
        """Restricted collections outside the access collections are ignored."""
        members = {"comm-a": ({"cds-edboard-na61"}, {"jane.doe@cern.ch"})}
        other_role = make_role('allow group "other [CERN]"')
        check = make_check(
            {"NA61": [NA61_ROLE], "Other": [other_role]},
            members,
            monkeypatch,
            access_collections=["NA61"],
        )
        check._check_restricted_collections()

    def test_unsupported_firerole(self, monkeypatch):
        check = make_check({"NA61": [make_role("allow any")]}, {}, monkeypatch)
        with pytest.raises(ManualImportRequired):
            check._check_restricted_collections()


def test_community_subjects(db, community, uploader):
    role = Role(id="cds-edboard-na61", name="cds-edboard-na61")
    db.session.add(role)
    db.session.flush()
    for kwargs in ({"group_id": role.id}, {"user_id": uploader.id}):
        db.session.add(
            MemberModel(
                community_id=community.id,
                role="reader",
                visible=True,
                active=True,
                **kwargs,
            )
        )
    db.session.commit()

    community_subjects.cache_clear()
    assert community_subjects(str(community.id)) == (
        frozenset({"cds-edboard-na61"}),
        frozenset({"uploader@inveniosoftware.org"}),
    )


class TestCheckEditRoles:
    EDITOR_ROLE = make_role('allow group "cds-curators [CERN]"', ["Ed@cern.ch"])

    def test_no_edit_roles(self, monkeypatch):
        check = make_check({}, {}, monkeypatch)
        check._check_edit_roles()

    def test_members(self, monkeypatch):
        members = {"comm-a": ({"cds-curators", "other"}, {"ed@cern.ch"})}
        edit = {"collections": {"NA61": [self.EDITOR_ROLE]}, "any_collection": []}
        check = make_check({}, members, monkeypatch, edit=edit)
        check._check_edit_roles()

    def test_any_collection_roles_not_checked(self, monkeypatch):
        """Instance-wide curators must not have to be community members."""
        members = {"comm-a": (set(), set())}
        edit = {"collections": {}, "any_collection": [self.EDITOR_ROLE]}
        check = make_check({}, members, monkeypatch, edit=edit)
        check._check_edit_roles()

    def test_only_access_collections_checked(self, monkeypatch):
        members = {"comm-a": ({"cds-curators"}, {"ed@cern.ch"})}
        edit = {
            "collections": {
                "NA61": [self.EDITOR_ROLE],
                "Articles": [make_role('allow group "articles-editors [CERN]"')],
            },
            "any_collection": [],
        }
        check = make_check(
            {}, members, monkeypatch, edit=edit, access_collections=["NA61"]
        )
        check._check_edit_roles()

        check.access_collections = None
        with pytest.raises(ManualImportRequired):
            check._check_edit_roles()

    def test_missing_member(self, monkeypatch):
        members = {"comm-a": ({"cds-curators"}, set())}
        edit = {"collections": {"NA61": [self.EDITOR_ROLE]}, "any_collection": []}
        check = make_check({}, members, monkeypatch, edit=edit)
        with pytest.raises(ManualImportRequired) as e:
            check._check_edit_roles()
        assert e.value.value == ["ed@cern.ch"]


def test_cern_scientific_community_is_not_checked(monkeypatch):
    """The CERN Research community's members are never compared to the roles."""
    members = {"comm-a": ({"cds-edboard-na61"}, {"jane.doe@cern.ch"})}
    check = make_check(
        {"NA61": [NA61_ROLE]}, members, monkeypatch, ("comm-a", "cern-research")
    )
    monkeypatch.setattr(
        community_members_module,
        "current_app",
        SimpleNamespace(config={"CDS_CERN_SCIENTIFIC_COMMUNITY_ID": "cern-research"}),
    )
    check._check_restricted_collections()
