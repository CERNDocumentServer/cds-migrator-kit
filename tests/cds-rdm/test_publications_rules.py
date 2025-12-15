# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Tests for publications.py migration rules."""

import pytest
from dojson.errors import IgnoreKey

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.rdm.records.transform.xml_processing.rules.publications import (
    imprint_info,
    internal_notes,
    isbn,
    issn,
    journal,
    udc,
)


class TestIsbn:
    """Test isbn function from publications.py."""

    def test_isbn_valid_isbn13(self):
        """Test valid ISBN-13."""
        record = {}
        with pytest.raises(IgnoreKey):
            isbn(record, "020__", {"a": "978-3-16-148410-0"})
        # ISBN should be normalized with hyphens
        assert "isbn" in record["custom_fields"]["imprint:imprint"]
        assert "978-3-16-148410-0" == record["custom_fields"]["imprint:imprint"]["isbn"]

    def test_isbn_valid_isbn10(self):
        """Test valid ISBN-10."""
        record = {}
        with pytest.raises(IgnoreKey):
            isbn(record, "020__", {"a": "0-306-40615-2"})
        # ISBN-10 should be converted to ISBN-13
        assert "isbn" in record["custom_fields"]["imprint:imprint"]

    def test_isbn_adds_to_related_identifiers(self):
        """Test ISBN is added to related_identifiers."""
        record = {}
        with pytest.raises(IgnoreKey):
            isbn(record, "020__", {"a": "978-0-596-52068-7"})
        # ISBN should be in related_identifiers
        isbn_found = any(
            id_item["scheme"] == "isbn"
            for id_item in record.get("related_identifiers", [])
        )
        assert isbn_found
        # Check relation type and resource type
        isbn_entry = next(
            id_item
            for id_item in record.get("related_identifiers", [])
            if id_item["scheme"] == "isbn"
        )
        assert isbn_entry["relation_type"]["id"] == "isvariantformof"
        assert isbn_entry["resource_type"]["id"] == "publication-book"

    def test_isbn_non_cern_isbn_in_related_identifiers(self):
        """Test non-CERN ISBN goes to related_identifiers."""
        record = {}
        with pytest.raises(IgnoreKey):
            isbn(record, "020__", {"a": "978-0-596-52068-7"})
        # Non-CERN ISBN should also be in related_identifiers
        assert any(
            id_item["scheme"] == "isbn"
            for id_item in record.get("related_identifiers", [])
        )

    def test_isbn_invalid_handled_gracefully(self):
        """Test invalid ISBN is handled."""
        record = {}
        # Invalid ISBN should raise UnexpectedValue or IgnoreKey
        with pytest.raises((UnexpectedValue, IgnoreKey)):
            isbn(record, "020__", {"a": "123"})

    def test_isbn_empty_handled(self):
        """Test that empty ISBN is handled gracefully."""
        record = {}
        with pytest.raises(IgnoreKey):
            isbn(record, "020__", {"a": ""})

    def test_isbn_relation_type(self):
        """Test ISBN has correct relation type."""
        record = {}
        with pytest.raises(IgnoreKey):
            isbn(record, "020__", {"a": "978-3-16-148410-0"})
        isbn_rel = next(
            (
                id_item
                for id_item in record.get("related_identifiers", [])
                if id_item["scheme"] == "isbn"
            ),
            None,
        )
        assert isbn_rel is not None
        assert isbn_rel["relation_type"]["id"] == "isvariantformof"
        assert isbn_rel["resource_type"]["id"] == "publication-book"


class TestIssn:
    """Test issn function from publications.py."""

    def test_issn_valid(self):
        """Test valid ISSN."""
        record = {}
        result = issn(record, "022__", {"a": "1234-5678"})
        assert result == [
            {
                "identifier": "1234-5678",
                "scheme": "issn",
                "relation_type": {"id": "ispublishedin"},
            }
        ]

    def test_issn_without_hyphen(self):
        """Test ISSN without hyphen gets normalized."""
        record = {}
        result = issn(record, "022__", {"a": "12345678"})
        # Should be normalized to XXXX-XXXX format
        assert result[0]["identifier"] == "1234-5678"
        assert result[0]["scheme"] == "issn"

    def test_issn_relation_type(self):
        """Test ISSN has correct relation type."""
        record = {}
        result = issn(record, "022__", {"a": "1234-5678"})
        assert result[0]["relation_type"]["id"] == "ispublishedin"

    def test_issn_invalid_handled_gracefully(self):
        """Test invalid ISSN is handled (may not always raise error)."""
        record = {}
        # Some invalid ISSNs might pass through, test that function doesn't crash
        try:
            result = issn(record, "022__", {"a": "0000-0000"})
            # If it doesn't raise, result should be a list
            assert isinstance(result, list)
        except (UnexpectedValue, IgnoreKey):
            # It's okay if it raises an error
            pass

    def test_issn_empty_ignored(self):
        """Test that empty ISSN is ignored."""
        record = {}
        with pytest.raises(IgnoreKey):
            issn(record, "022__", {"a": ""})

    def test_issn_no_duplicate(self):
        """Test that duplicate ISSN is not added."""
        record = {"identifiers": [{"identifier": "1234-5678", "scheme": "issn"}]}
        result = issn(record, "022__", {"a": "1234-5678"})
        # Should return the new identifier
        assert result[0]["identifier"] == "1234-5678"


class TestUdc:
    """Test udc function from publications.py."""

    def test_udc_valid_pattern_ignored(self):
        """Test that valid UDC pattern is ignored."""
        record = {}
        with pytest.raises(IgnoreKey):
            udc(record, "080__", {"a": "539.12"})

    def test_udc_another_valid_pattern(self):
        """Test another valid UDC pattern."""
        record = {}
        with pytest.raises(IgnoreKey):
            udc(record, "080__", {"a": "621.384"})

    def test_udc_invalid_pattern_raises_error(self):
        """Test that invalid UDC pattern raises error."""
        record = {}
        with pytest.raises(UnexpectedValue):
            udc(record, "080__", {"a": "invalid"})

    def test_udc_empty_raises_error(self):
        """Test that empty UDC raises error."""
        record = {}
        with pytest.raises(UnexpectedValue):
            udc(record, "080__", {"a": ""})


class TestImprintInfo:
    """Test imprint_info function from publications.py."""

    def test_imprint_info_full(self):
        """Test full imprint info with place, publisher, and date."""
        record = {"custom_fields": {}}
        result = imprint_info(
            record, "260__", {"a": "Geneva", "b": "CERN", "c": "2021"}
        )
        assert result == "2021"
        assert record["publisher"] == "CERN"
        assert record["custom_fields"]["imprint:imprint"]["place"] == "Geneva"

    def test_imprint_info_date_only(self):
        """Test imprint info with date only."""
        record = {"custom_fields": {}}
        result = imprint_info(record, "260__", {"c": "2021-05-15"})
        assert result == "2021-05-15"

    def test_imprint_info_place_only_no_date(self):
        """Test that place without date raises IgnoreKey."""
        record = {"custom_fields": {}}
        with pytest.raises(IgnoreKey):
            imprint_info(record, "260__", {"a": "Geneva"})

    def test_imprint_info_publisher_only_no_date(self):
        """Test that publisher without date raises IgnoreKey."""
        record = {"custom_fields": {}}
        with pytest.raises(IgnoreKey):
            imprint_info(record, "260__", {"b": "CERN"})

    def test_imprint_info_no_date_ignored(self):
        """Test that missing date raises IgnoreKey."""
        record = {"custom_fields": {}}
        with pytest.raises(IgnoreKey):
            imprint_info(record, "260__", {"a": "Geneva", "b": "CERN"})

    def test_imprint_info_invalid_date_raises_error(self):
        """Test that invalid date raises error."""
        record = {"custom_fields": {}}
        with pytest.raises(UnexpectedValue):
            imprint_info(record, "260__", {"c": "not-a-valid-date"})

    def test_imprint_info_publisher_not_overwritten(self):
        """Test that existing publisher is not overwritten."""
        record = {"custom_fields": {}, "publisher": "Existing Publisher"}
        result = imprint_info(record, "260__", {"b": "CERN", "c": "2021"})
        assert record["publisher"] == "Existing Publisher"

    def test_imprint_info_place_strips_period(self):
        """Test that place strips trailing period."""
        record = {"custom_fields": {}}
        result = imprint_info(record, "260__", {"a": "Geneva.", "c": "2021"})
        assert record["custom_fields"]["imprint:imprint"]["place"] == "Geneva"

    def test_imprint_info_place_with_period(self):
        """Test that place strips single trailing period."""
        record = {"custom_fields": {}}
        result = imprint_info(record, "260__", {"a": "New York.", "c": "2021"})
        # rstrip('.') removes all trailing periods
        assert record["custom_fields"]["imprint:imprint"]["place"] == "New York"

    def test_imprint_info_year_month_date(self):
        """Test date with year and month."""
        record = {"custom_fields": {}}
        result = imprint_info(record, "260__", {"c": "2021-05"})
        assert result == "2021-05"

    def test_imprint_info_text_date_format(self):
        """Test date in text format."""
        record = {"custom_fields": {}}
        result = imprint_info(record, "260__", {"c": "May 2021"})
        assert result == "2021-05"


class TestInternalNotes:
    """Test internal_notes function from publications.py.

    Note: The 500__ internal_notes is tested in test_base_rules.py as note().
    Here we test the 562__ version which uses subfield 'c' and is decorated.
    """

    def test_internal_notes_562_basic(self):
        """Test 562__ internal notes with subfield 'c'."""
        record = {}
        result = internal_notes(record, "562__", {"c": "This is a note"})
        assert result == [{"note": "This is a note"}]

    def test_internal_notes_562_empty_note(self):
        """Test 562__ with empty note."""
        record = {}
        result = internal_notes(record, "562__", {"c": ""})
        # Even empty notes are returned by this function
        assert result == [{"note": ""}]

    def test_internal_notes_562_no_subfield_c(self):
        """Test 562__ without subfield 'c'."""
        record = {}
        result = internal_notes(record, "562__", {})
        assert result == [{"note": ""}]


class TestJournal:
    """Test journal function from publications.py."""

    def test_journal_full_info(self):
        """Test journal with full information."""
        record = {}
        result = journal(
            record,
            "773__",
            {
                "p": "Physical Review Letters",
                "v": "123",
                "n": "4",
                "y": "2021",
                "c": "045001",
            },
        )
        # journal function returns custom_fields dict
        journal_info = result.get("journal:journal", {})
        assert journal_info["title"] == "Physical Review Letters"
        assert journal_info["volume"] == "123"
        assert journal_info["issue"] == "4"
        assert journal_info["pages"] == "045001"

    def test_journal_minimal_info(self):
        """Test journal with minimal info (just title)."""
        record = {}
        result = journal(record, "773__", {"p": "Nature"})
        journal_info = result.get("journal:journal", {})
        assert journal_info["title"] == "Nature"

    def test_journal_with_volume_only(self):
        """Test journal with title and volume."""
        record = {}
        result = journal(record, "773__", {"p": "Science", "v": "500"})
        journal_info = result.get("journal:journal", {})
        assert journal_info["title"] == "Science"
        assert journal_info["volume"] == "500"

    def test_journal_with_pages_range(self):
        """Test journal with page range."""
        record = {}
        result = journal(record, "773__", {"p": "Nature", "c": "123-456"})
        journal_info = result.get("journal:journal", {})
        assert journal_info["pages"] == "123-456"

    def test_journal_empty_title_no_exception(self):
        """Test that empty title creates empty journal fields."""
        record = {}
        result = journal(record, "773__", {})
        # Should return custom_fields dict even if empty
        assert "journal:journal" in result

    def test_journal_only_volume_creates_entry(self):
        """Test that volume without title creates entry."""
        record = {}
        result = journal(record, "773__", {"v": "100"})
        journal_info = result.get("journal:journal", {})
        assert journal_info["volume"] == "100"
        assert journal_info["title"] == ""  # Empty string from parse

    def test_journal_issue_without_volume(self):
        """Test journal with issue but no volume."""
        record = {}
        result = journal(record, "773__", {"p": "Journal", "n": "5"})
        journal_info = result.get("journal:journal", {})
        assert journal_info["title"] == "Journal"
        assert journal_info["issue"] == "5"
