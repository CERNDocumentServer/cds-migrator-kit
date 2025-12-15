# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Tests for base.py migration rules."""

import pytest
from dojson.errors import IgnoreKey

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.rdm.records.transform.xml_processing.rules.base import (
    custom_fields_693,
    normalize,
    note,
    recid,
    record_restriction,
    urls,
)


class TestRecid:
    """Test recid function from base.py."""

    def test_recid_basic(self):
        """Test basic recid translation."""
        record = {}
        result = recid(record, "001", "123456")
        assert result == 123456
        assert record["recid"] == "123456"
        assert {"identifier": "123456", "scheme": "cds"} in record["identifiers"]

    def test_recid_adds_to_existing_identifiers(self):
        """Test that recid adds to existing identifiers."""
        record = {"identifiers": [{"identifier": "foo", "scheme": "bar"}]}
        result = recid(record, "001", "789")
        assert result == 789
        assert len(record["identifiers"]) == 2
        assert {"identifier": "foo", "scheme": "bar"} in record["identifiers"]
        assert {"identifier": "789", "scheme": "cds"} in record["identifiers"]

    def test_recid_no_duplicate(self):
        """Test that recid doesn't create duplicates."""
        record = {"identifiers": [{"identifier": "123", "scheme": "cds"}]}
        result = recid(record, "001", "123")
        assert result == 123
        # Should still have only one identifier
        assert len(record["identifiers"]) == 1

    def test_recid_string_conversion_to_int(self):
        """Test that recid converts string to int."""
        record = {}
        result = recid(record, "001", "999999")
        assert isinstance(result, int)
        assert result == 999999


class TestRecordRestriction:
    """Test record_restriction function from base.py."""

    def test_record_restriction_public(self):
        """Test that PUBLIC returns 'public'."""
        record = {}
        result = record_restriction(record, "963__", {"a": "PUBLIC"})
        assert result == "public"

    def test_record_restriction_public_lowercase(self):
        """Test that public (lowercase) returns 'public'."""
        record = {}
        result = record_restriction(record, "963__", {"a": "public"})
        assert result == "public"

    def test_record_restriction_public_mixed_case(self):
        """Test that Public (mixed case) returns 'public'."""
        record = {}
        result = record_restriction(record, "963__", {"a": "Public"})
        assert result == "public"

    def test_record_restriction_restricted_raises_error(self):
        """Test that non-public values raise UnexpectedValue."""
        record = {}
        with pytest.raises(UnexpectedValue):
            record_restriction(record, "963__", {"a": "RESTRICTED"})

    def test_record_restriction_empty_raises_error(self):
        """Test that empty value raises UnexpectedValue."""
        record = {}
        with pytest.raises(UnexpectedValue):
            record_restriction(record, "963__", {"a": ""})

    def test_record_restriction_cern_internal_raises_error(self):
        """Test that CERN INTERNAL raises UnexpectedValue."""
        record = {}
        with pytest.raises(UnexpectedValue):
            record_restriction(record, "963__", {"a": "CERN INTERNAL"})


class TestCustomFields693:
    """Test custom_fields_693 function from base.py."""

    def test_custom_fields_693_experiments(self):
        """Test experiments field extraction."""
        record = {}
        with pytest.raises(IgnoreKey):
            custom_fields_693(record, "693__", {"e": "ATLAS"})
        assert "ATLAS" in record["custom_fields"]["cern:experiments"]

    def test_custom_fields_693_accelerators(self):
        """Test accelerators field extraction."""
        record = {}
        with pytest.raises(IgnoreKey):
            custom_fields_693(record, "693__", {"a": "LHC"})
        assert "LHC" in record["custom_fields"]["cern:accelerators"]

    def test_custom_fields_693_projects(self):
        """Test projects field extraction."""
        record = {}
        with pytest.raises(IgnoreKey):
            custom_fields_693(record, "693__", {"p": "HL-LHC"})
        assert "HL-LHC" in record["custom_fields"]["cern:projects"]

    def test_custom_fields_693_facilities(self):
        """Test facilities field extraction."""
        record = {}
        with pytest.raises(IgnoreKey):
            custom_fields_693(record, "693__", {"f": "ISOLDE"})
        assert "ISOLDE" in record["custom_fields"]["cern:facilities"]

    def test_custom_fields_693_studies(self):
        """Test studies field extraction."""
        record = {}
        with pytest.raises(IgnoreKey):
            custom_fields_693(record, "693__", {"s": "Physics Study"})
        assert "Physics Study" in record["custom_fields"]["cern:studies"]

    def test_custom_fields_693_beams(self):
        """Test beams field extraction."""
        record = {}
        with pytest.raises(IgnoreKey):
            custom_fields_693(record, "693__", {"b": "Proton"})
        assert "Proton" in record["custom_fields"]["cern:beams"]

    def test_custom_fields_693_multiple_fields(self):
        """Test multiple fields in one call."""
        record = {}
        with pytest.raises(IgnoreKey):
            custom_fields_693(record, "693__", {"e": "CMS", "a": "LHC", "p": "HL-LHC"})
        assert "CMS" in record["custom_fields"]["cern:experiments"]
        assert "LHC" in record["custom_fields"]["cern:accelerators"]
        assert "HL-LHC" in record["custom_fields"]["cern:projects"]

    def test_custom_fields_693_appends_to_existing(self):
        """Test that fields are appended to existing values."""
        record = {"custom_fields": {"cern:experiments": ["ATLAS"]}}
        with pytest.raises(IgnoreKey):
            custom_fields_693(record, "693__", {"e": "CMS"})
        assert "ATLAS" in record["custom_fields"]["cern:experiments"]
        assert "CMS" in record["custom_fields"]["cern:experiments"]

    def test_custom_fields_693_empty_values_ignored(self):
        """Test that empty values are not added."""
        record = {}
        with pytest.raises(IgnoreKey):
            custom_fields_693(record, "693__", {"e": ""})
        assert record["custom_fields"]["cern:experiments"] == []

    def test_custom_fields_693_list_values(self):
        """Test that list values are properly handled."""
        record = {}
        with pytest.raises(IgnoreKey):
            custom_fields_693(record, "693__", {"e": ["ATLAS", "CMS"]})
        assert "ATLAS" in record["custom_fields"]["cern:experiments"]
        assert "CMS" in record["custom_fields"]["cern:experiments"]

    def test_custom_fields_693_all_fields_at_once(self):
        """Test all fields can be set in one call."""
        record = {}
        with pytest.raises(IgnoreKey):
            custom_fields_693(
                record,
                "693__",
                {
                    "e": "ALICE",
                    "a": "LHC",
                    "p": "HL-LHC",
                    "f": "ISOLDE",
                    "s": "Study1",
                    "b": "Proton",
                },
            )
        assert "ALICE" in record["custom_fields"]["cern:experiments"]
        assert "LHC" in record["custom_fields"]["cern:accelerators"]
        assert "HL-LHC" in record["custom_fields"]["cern:projects"]
        assert "ISOLDE" in record["custom_fields"]["cern:facilities"]
        assert "Study1" in record["custom_fields"]["cern:studies"]
        assert "Proton" in record["custom_fields"]["cern:beams"]


class TestNormalize:
    """Test normalize utility function from base.py."""

    def test_normalize_year(self):
        """Test normalizing a year."""
        result = normalize("2021")
        assert result == "2021"

    def test_normalize_year_month(self):
        """Test normalizing year-month."""
        result = normalize("2021-05")
        assert result == "2021-05"

    def test_normalize_full_date(self):
        """Test normalizing full date."""
        result = normalize("2021-05-15")
        assert result == "2021-05-15"

    def test_normalize_date_with_text_month(self):
        """Test normalizing date with text month."""
        result = normalize("May 15, 2021")
        assert result == "2021-05-15"

    def test_normalize_different_formats(self):
        """Test normalizing different date formats."""
        # Test various formats
        assert normalize("2021/05/15") == "2021-05-15"
        assert normalize("15.05.2021") == "2021-05-15"

    def test_normalize_year_only_number(self):
        """Test normalizing year as integer."""
        result = normalize("2020")
        assert result == "2020"


class TestUrls:
    """Test urls function from base.py."""

    def test_urls_basic(self):
        """Test basic URL translation (https converted to http)."""
        record = {"recid": "123456"}
        result = urls(
            record, "8564_", {"u": "https://example.com", "y": "Example Link"}
        )
        assert result == [
            {
                "identifier": "http://example.com",
                "scheme": "url",
                "relation_type": {"id": "references"},
                "resource_type": {"id": "other"},
            }
        ]

    def test_urls_without_description(self):
        """Test URL without description."""
        record = {"recid": "123456"}
        result = urls(record, "8564_", {"u": "https://example.com"})
        assert len(result) == 1
        # URLs are normalized to http
        assert result[0]["identifier"] == "http://example.com"
        assert result[0]["scheme"] == "url"
        assert result[0]["relation_type"]["id"] == "references"

    def test_urls_http_protocol(self):
        """Test HTTP protocol URL."""
        record = {"recid": "123456"}
        result = urls(record, "8564_", {"u": "http://example.com"})
        assert result[0]["identifier"] == "http://example.com"

    def test_urls_empty_url_ignored(self):
        """Test that empty URL raises UnexpectedValue."""
        record = {"recid": "123456"}
        with pytest.raises(UnexpectedValue):
            urls(record, "8564_", {"u": ""})

    def test_urls_no_url_field_ignored(self):
        """Test that missing URL field raises UnexpectedValue."""
        record = {"recid": "123456"}
        with pytest.raises(UnexpectedValue):
            urls(record, "8564_", {})

    def test_urls_custom_subfield(self):
        """Test URL with custom subfield (https converted to http)."""
        record = {"recid": "123456"}
        result = urls(record, "8564_", {"x": "https://custom.com"}, subfield="x")
        # URLs are converted to http
        assert result[0]["identifier"] == "http://custom.com"


class TestNote:
    """Test note function from base.py."""

    def test_note_basic(self):
        """Test basic note translation."""
        record = {}
        with pytest.raises(IgnoreKey):
            note(record, "595__", {"a": "This is a note"})
        assert {"note": "This is a note"} in record["internal_notes"]

    def test_note_multiple_notes(self):
        """Test multiple notes."""
        record = {}
        with pytest.raises(IgnoreKey):
            note(record, "595__", {"a": "Note 1"})
        with pytest.raises(IgnoreKey):
            note(record, "595__", {"a": "Note 2"})
        assert {"note": "Note 1"} in record["internal_notes"]
        assert {"note": "Note 2"} in record["internal_notes"]

    def test_note_preserves_existing_notes(self):
        """Test that new notes preserve existing ones."""
        record = {"internal_notes": [{"note": "Existing note"}]}
        with pytest.raises(IgnoreKey):
            note(record, "595__", {"a": "New note"})
        assert {"note": "Existing note"} in record["internal_notes"]
        assert {"note": "New note"} in record["internal_notes"]
        assert len(record["internal_notes"]) == 2

    def test_note_empty_ignored(self):
        """Test that empty note is ignored."""
        record = {}
        with pytest.raises(IgnoreKey):
            note(record, "595__", {"a": ""})

    def test_note_whitespace_only_ignored(self):
        """Test that whitespace-only note is ignored."""
        record = {}
        with pytest.raises(IgnoreKey):
            note(record, "595__", {"a": "   "})
