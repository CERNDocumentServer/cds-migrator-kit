# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Unit tests for ``RecordParent.resolve_grants()``.

These characterise the current behaviour of every branch that turns a
legacy file-restriction string (``VersionEntry["access"]["meta"]``) plus
the record's own access grants into ``(groups, emails, grants_with_perms)``.
<<<<<<< HEAD
They rely on the shared ``test_app`` fixture (conftest) for the application
context and its ``CDS_ACCESS_GROUP_MAPPINGS`` config (``SSO`` ->
``cern-personnel``, ``HrDepRestrFile`` -> ``hr-dep`` + ``fap-dep-tpr-mi-staf``).
=======
They are intentionally self-contained (a minimal Flask app providing only
``CDS_ACCESS_GROUP_MAPPINGS``) so they run without Docker services.
>>>>>>> 8214940 (fix(files-access): flag mixed file restrictions for curation)
"""

from types import SimpleNamespace

import pytest
<<<<<<< HEAD
=======
from flask import Flask
>>>>>>> 8214940 (fix(files-access): flag mixed file restrictions for curation)

from cds_migrator_kit.errors import ManualImportRequired
from cds_migrator_kit.rdm.records.transform.entities.parent import RecordParent

<<<<<<< HEAD
=======
GROUP_MAPPINGS = {
    "SSO": ["cern-personnel"],
    "ITDepRestrFile": ["it-dep"],
    "HrDepRestrFile": ["hr-dep", "fap-dep-tpr-mi-staf"],
}


@pytest.fixture()
def app_ctx():
    """Minimal app context exposing CDS_ACCESS_GROUP_MAPPINGS."""
    app = Flask("test_resolve_grants")
    app.config["CDS_ACCESS_GROUP_MAPPINGS"] = GROUP_MAPPINGS
    with app.app_context():
        yield app

>>>>>>> 8214940 (fix(files-access): flag mixed file restrictions for curation)

def make_parent(access_grants=None):
    """Build a RecordParent bypassing build(); set access_grants directly."""
    parent = RecordParent(
        record=SimpleNamespace(recid="123"),
        raw_dump_entry={"recid": "123"},
        dojson_entry={},
        communities_ids=["some-community"],
        access_grants_view=[],
    )
    parent.access_grants = access_grants or []
    return parent


# --- File restriction (``specific_file_restrictions``) only ----------------


<<<<<<< HEAD
def test_empty_restriction_no_grants(test_app):
=======
def test_empty_restriction_no_grants(app_ctx):
>>>>>>> 8214940 (fix(files-access): flag mixed file restrictions for curation)
    """No file restriction and no record grants -> everything empty."""
    groups, emails, grants = make_parent().resolve_grants("")
    assert groups == set()
    assert emails == set()
    assert grants == {}


<<<<<<< HEAD
def test_mapped_status_resolves_to_group(test_app):
=======
def test_mapped_status_resolves_to_group(app_ctx):
>>>>>>> 8214940 (fix(files-access): flag mixed file restrictions for curation)
    """A status in CDS_ACCESS_GROUP_MAPPINGS resolves to its mapped groups."""
    groups, emails, grants = make_parent().resolve_grants("SSO")
    assert groups == {"cern-personnel"}
    assert emails == set()
    assert grants == {}


<<<<<<< HEAD
def test_mapped_status_multiple_groups(test_app):
=======
def test_mapped_status_multiple_groups(app_ctx):
>>>>>>> 8214940 (fix(files-access): flag mixed file restrictions for curation)
    """A status mapping to several groups adds all of them."""
    groups, _, _ = make_parent().resolve_grants("HrDepRestrFile")
    assert groups == {"hr-dep", "fap-dep-tpr-mi-staf"}


<<<<<<< HEAD
def test_literal_restricted_maps_to_cern_personnel(test_app):
=======
def test_literal_restricted_maps_to_cern_personnel(app_ctx):
>>>>>>> 8214940 (fix(files-access): flag mixed file restrictions for curation)
    """The literal 'restricted' status maps to cern-personnel."""
    groups, emails, grants = make_parent().resolve_grants("restricted")
    assert groups == {"cern-personnel"}
    assert emails == set()


<<<<<<< HEAD
def test_bare_cern_egroup_name(test_app):
=======
def test_bare_cern_egroup_name(app_ctx):
>>>>>>> 8214940 (fix(files-access): flag mixed file restrictions for curation)
    """A bare '<name> [CERN]' e-group name is added, '[CERN]' stripped."""
    groups, emails, _ = make_parent().resolve_grants(
        "cds-ph-ep-publications-referee-non-lhc [CERN]"
    )
    assert groups == {"cds-ph-ep-publications-referee-non-lhc"}
    assert emails == set()


<<<<<<< HEAD
def test_firerole_allow_group(test_app):
=======
def test_firerole_allow_group(app_ctx):
>>>>>>> 8214940 (fix(files-access): flag mixed file restrictions for curation)
    """A firerole 'allow group' string parses out the group(s)."""
    groups, emails, _ = make_parent().resolve_grants(
        'firerole: allow group "atlas-active-members [CERN]"\ndeny all'
    )
    assert groups == {"atlas-active-members"}
    assert emails == set()


<<<<<<< HEAD
def test_firerole_allow_multiple_groups(test_app):
=======
def test_firerole_allow_multiple_groups(app_ctx):
>>>>>>> 8214940 (fix(files-access): flag mixed file restrictions for curation)
    """A firerole 'allow group' with several quoted groups parses all."""
    groups, _, _ = make_parent().resolve_grants(
        'firerole: allow group "atlas-active-members [CERN]", "cms-members [CERN]"\n'
        "deny all"
    )
    assert groups == {"atlas-active-members", "cms-members"}


<<<<<<< HEAD
def test_firerole_allow_email(test_app):
=======
def test_firerole_allow_email(app_ctx):
>>>>>>> 8214940 (fix(files-access): flag mixed file restrictions for curation)
    """A firerole 'allow email' string parses out the email(s)."""
    groups, emails, _ = make_parent().resolve_grants(
        'firerole: allow email "a@cern.ch"\ndeny all'
    )
    assert groups == set()
    assert emails == {"a@cern.ch"}


<<<<<<< HEAD
def test_unmapped_non_firerole_status_raises(test_app):
=======
def test_unmapped_non_firerole_status_raises(app_ctx):
>>>>>>> 8214940 (fix(files-access): flag mixed file restrictions for curation)
    """An unknown status that is neither mapped nor firerole hard-raises."""
    with pytest.raises(ManualImportRequired):
        make_parent().resolve_grants("TOTALLY_UNKNOWN_STATUS")


# --- Record access grants (``self.access_grants``) -------------------------


<<<<<<< HEAD
def test_record_group_grant_without_file_restriction(test_app):
=======
def test_record_group_grant_without_file_restriction(app_ctx):
>>>>>>> 8214940 (fix(files-access): flag mixed file restrictions for curation)
    """With no file restriction, a record group grant feeds groups + perms.

    Note the normalised name goes to ``groups`` while the raw subject is the
    ``grants_with_perms`` key.
    """
    groups, emails, grants = make_parent(
        [{"some-group [CERN]": "manage"}]
    ).resolve_grants("")
    assert groups == {"some-group"}
    assert emails == set()
    assert grants == {"some-group [CERN]": "manage"}


<<<<<<< HEAD
def test_record_email_grant_defaults_to_view(test_app):
=======
def test_record_email_grant_defaults_to_view(app_ctx):
>>>>>>> 8214940 (fix(files-access): flag mixed file restrictions for curation)
    """A record email grant with no permission defaults to 'view'."""
    groups, emails, grants = make_parent([{"user@cern.ch": None}]).resolve_grants("")
    assert groups == set()
    assert emails == {"user@cern.ch"}
    assert grants == {"user@cern.ch": "view"}


<<<<<<< HEAD
def test_file_restriction_withholds_record_subjects_from_access(test_app):
=======
def test_file_restriction_withholds_record_subjects_from_access(app_ctx):
>>>>>>> 8214940 (fix(files-access): flag mixed file restrictions for curation)
    """When a file restriction exists, record subjects are kept out of
    groups/emails (least access) but still recorded in grants_with_perms."""
    groups, emails, grants = make_parent([{"extra-group": "view"}]).resolve_grants(
        "SSO"
    )
    assert groups == {"cern-personnel"}
    assert emails == set()
    assert grants == {"extra-group": "view"}
