# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Tests for reviewer resolution encapsulated in RecordRequest (entities/request.py)."""

import pytest
from invenio_accounts.testutils import create_test_user

from cds_migrator_kit.errors import RecordFlaggedCuration
from cds_migrator_kit.rdm.records.transform.entities.request import RecordRequest


class TestIsEmail:
    """Test the RecordRequest._is_email helper."""

    def test_email_detected(self):
        """Test that a string with '@' is detected as an email."""
        assert RecordRequest._is_email("john.doe@cern.ch") is True

    def test_name_not_detected_as_email(self):
        """Test that a plain name is not detected as an email."""
        assert RecordRequest._is_email("Doe, John") is False
        assert RecordRequest._is_email("John Doe") is False


class TestParseReviewerName:
    """Test the RecordRequest._parse_reviewer_name helper."""

    def test_family_comma_given(self):
        """Test 'Family, Given' format."""
        assert RecordRequest._parse_reviewer_name("Doe, John") == ("Doe", "John")

    def test_given_family_no_comma(self):
        """Test 'Given Family' format (no comma)."""
        assert RecordRequest._parse_reviewer_name("John Doe") == ("Doe", "John")

    def test_multi_word_given_name(self):
        """Test a multi-word given name without a comma."""
        assert RecordRequest._parse_reviewer_name("John Michael Doe") == (
            "Doe",
            "John Michael",
        )

    def test_family_name_only(self):
        """Test a single-word name with no given name available."""
        assert RecordRequest._parse_reviewer_name("Doe") == ("Doe", "")

    def test_strips_whitespace(self):
        """Test that surrounding and inner whitespace is stripped."""
        assert RecordRequest._parse_reviewer_name("  Doe ,  John  ") == (
            "Doe",
            "John",
        )


class TestFindReviewer:
    """Test RecordRequest._find_reviewer() DB resolution (email or profile name match)."""

    def test_find_reviewer_by_email(self, app, db):
        """Test that a reviewer given as an email is resolved by email."""
        user = create_test_user(email="jane.smith@cern.ch")
        db.session.commit()

        found = RecordRequest._find_reviewer("jane.smith@cern.ch")
        assert found.id == user.id

    def test_find_reviewer_by_profile_name_family_given(self, app, db):
        """Test that a 'Family, Given' reviewer is resolved via profile JSON."""
        user = create_test_user(
            email="john.doe@cern.ch",
            user_profile={"family_name": "Doe", "given_name": "John"},
        )
        db.session.commit()

        found = RecordRequest._find_reviewer("Doe, John")
        assert found.id == user.id

    def test_find_reviewer_by_profile_name_given_family(self, app, db):
        """Test that a 'Given Family' reviewer (no comma) is resolved via profile JSON."""
        user = create_test_user(
            email="mary.jones@cern.ch",
            user_profile={"family_name": "Jones", "given_name": "Mary"},
        )
        db.session.commit()

        found = RecordRequest._find_reviewer("Mary Jones")
        assert found.id == user.id

    def test_find_reviewer_name_match_is_case_insensitive(self, app, db):
        """Test that profile name matching ignores case."""
        user = create_test_user(
            email="anna.lee@cern.ch",
            user_profile={"family_name": "Lee", "given_name": "Anna"},
        )
        db.session.commit()

        found = RecordRequest._find_reviewer("lee, ANNA")
        assert found.id == user.id

    def test_find_reviewer_family_name_only(self, app, db):
        """Test that a family-name-only reviewer resolves when unambiguous."""
        user = create_test_user(
            email="solo@cern.ch",
            user_profile={"family_name": "Solo", "given_name": "Han"},
        )
        db.session.commit()

        found = RecordRequest._find_reviewer("Solo")
        assert found.id == user.id

    def test_find_reviewer_by_email_not_found_raises(self, app, db):
        """Test that an unmatched email raises RecordFlaggedCuration."""
        with pytest.raises(RecordFlaggedCuration):
            RecordRequest._find_reviewer("nobody@cern.ch")

    def test_find_reviewer_by_name_not_found_raises(self, app, db):
        """Test that an unmatched name raises RecordFlaggedCuration."""
        with pytest.raises(RecordFlaggedCuration):
            RecordRequest._find_reviewer("Nobody, Here")

    def test_find_reviewer_name_wrong_given_name_raises(self, app, db):
        """Test that a family-name match with a mismatched given name raises."""
        create_test_user(
            email="doe2@cern.ch",
            user_profile={"family_name": "Doe", "given_name": "John"},
        )
        db.session.commit()

        with pytest.raises(RecordFlaggedCuration):
            RecordRequest._find_reviewer("Doe, Someone Else")
