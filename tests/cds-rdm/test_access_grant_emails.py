# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Tests for pre-creating accounts for direct emails found in field 506.

See cds_migrator_kit/rdm/users/transform/xml_processing/rules/access_grants.py
and cds_migrator_kit/users/load.py::CDSSubmitterLoad._access_grant_emails.
"""

from cds_dojson.marc21.utils import create_record
from invenio_accounts.testutils import create_test_user

from cds_migrator_kit.rdm.users.transform.xml_processing.models.submitter import (
    submitter_model,
)
from cds_migrator_kit.users.load import CDSSubmitterLoad


def _do(marcxml):
    return submitter_model.do(create_record(marcxml))


class TestAccessGrantEmailsRule:
    """Test the "^506[1_]_" -> "access_grant_emails" dojson rule."""

    def test_extracts_email_from_subfield_d(self, base_app):
        """A direct email in 506__d (e.g. record 2045640) is picked up."""
        with base_app.app_context():
            out = _do("""
                <record>
                <datafield tag="506" ind1=" " ind2=" ">
                  <subfield code="d">cds-edboard-dirac@cern.ch</subfield>
                </datafield>
                </record>
                """)
        assert out["access_grant_emails"] == ["cds-edboard-dirac@cern.ch"]

    def test_ignores_egroup_names(self, base_app):
        """E-group names (subfield m/a, no "@") are not treated as emails."""
        with base_app.app_context():
            out = _do("""
                <record>
                <datafield tag="506" ind1=" " ind2=" ">
                  <subfield code="m">cds-edboard-dirac [CERN]</subfield>
                </datafield>
                <datafield tag="506" ind1=" " ind2=" ">
                  <subfield code="m">cds-ph-ep-publications-referee-non-lhc [CERN]</subfield>
                </datafield>
                </record>
                """)
        assert out.get("access_grant_emails", []) == []

    def test_deduplicates_and_lowercases(self, base_app):
        """Repeated/differently-cased emails across occurrences collapse to one."""
        with base_app.app_context():
            out = _do("""
                <record>
                <datafield tag="506" ind1=" " ind2=" ">
                  <subfield code="d">Jane.Doe@cern.ch</subfield>
                </datafield>
                <datafield tag="506" ind1=" " ind2=" ">
                  <subfield code="m">jane.doe@cern.ch</subfield>
                </datafield>
                </record>
                """)
        assert out["access_grant_emails"] == ["jane.doe@cern.ch"]


class TestAccessGrantEmailsLoad:
    """Test CDSSubmitterLoad._access_grant_emails()."""

    def test_finds_existing_account(self, app, db):
        """An email matching an existing account is resolved, not recreated."""
        user = create_test_user(email="cds-edboard-dirac@cern.ch")
        db.session.commit()

        load = CDSSubmitterLoad(dry_run=True)
        load._access_grant_emails(
            {"access_grant_emails": ["cds-edboard-dirac@cern.ch"]}
        )

        found = load._find_or_create_by_email("cds-edboard-dirac@cern.ch")
        assert found == user.id

    def test_no_emails_is_a_noop(self, app, db):
        """No access_grant_emails key/empty list does nothing."""
        load = CDSSubmitterLoad(dry_run=True)
        load._access_grant_emails({})
        load._access_grant_emails({"access_grant_emails": []})
