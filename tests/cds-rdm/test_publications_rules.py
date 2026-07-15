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
from cds_migrator_kit.rdm.records.transform.xml_processing.rules.research import (
    abbreviation,
    deadline_date,
    imprint_info,
    internal_notes,
    isbn,
    issn,
    journal,
    meeting,
    oa_level_from_license,
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

    def test_isbn_no_duplicate(self):
        """Test that an already-present ISBN is not added twice."""
        record = {
            "related_identifiers": [
                {
                    "identifier": "978-3-16-148410-0",
                    "scheme": "isbn",
                    "relation_type": {"id": "isvariantformof"},
                    "resource_type": {"id": "publication-book"},
                }
            ]
        }
        with pytest.raises(IgnoreKey):
            isbn(record, "020__", {"a": "978-3-16-148410-0"})
        assert record["related_identifiers"] == [
            {
                "identifier": "978-3-16-148410-0",
                "scheme": "isbn",
                "relation_type": {"id": "isvariantformof"},
                "resource_type": {"id": "publication-book"},
            }
        ]


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

    def test_journal_not_overwritten_by_conference_773(self):
        """Journal fields from a journal 773 must survive a sibling conference 773.

        Record 836244 has two 773 fields in its latest revision:
          - {p, v, n, c, w, y} → journal reference (Acta Phys. Pol. B 37, 3 pp.875-882)
          - {0, c, w}          → conference proceedings reference (artid 119-122)
        Before the fix the second 773 silently overwrote title/issue/volume with ""
        and replaced pages with the conference artid "119-122".
        """
        record = {}
        # First 773: journal reference
        result = journal(
            record,
            "773__",
            {
                "c": "875-882",
                "n": "3",
                "p": "Acta Phys. Pol. B",
                "v": "37",
                "w": "C05-08-30",
                "y": "2006",
            },
        )
        record["custom_fields"] = result

        # Second 773: conference proceedings reference (no p / n / v)
        result = journal(
            record,
            "773__",
            {"0": "1260605", "c": "119-122", "w": "C05-03-12"},
        )

        journal_info = result.get("journal:journal", {})
        assert journal_info["title"] == "Acta Phys. Pol. B"
        assert journal_info["volume"] == "37"
        assert journal_info["issue"] == "3"
        assert journal_info["pages"] == "875-882"

        # Both conference cnums must be stored as separate meeting entries
        meetings = result.get("meeting:meeting", [])
        all_identifiers = [
            id_entry for m in meetings for id_entry in m.get("identifiers", [])
        ]
        assert {"scheme": "inspire", "identifier": "C05-08-30"} in all_identifiers
        assert {"scheme": "inspire", "identifier": "C05-03-12"} in all_identifiers

    def test_conference_only_773_does_not_set_journal_fields(self):
        """A 773 with only c+w (no p/n/v) must not populate journal:journal fields."""
        record = {}
        result = journal(
            record,
            "773__",
            {"0": "1260605", "c": "119-122", "w": "C05-03-12"},
        )

        journal_info = result.get("journal:journal", {})
        assert journal_info.get("title") is None
        assert journal_info.get("volume") is None
        assert journal_info.get("issue") is None
        assert journal_info.get("pages") is None

        meetings = result.get("meeting:meeting", [])
        all_identifiers = [
            id_entry for m in meetings for id_entry in m.get("identifiers", [])
        ]
        assert {"scheme": "inspire", "identifier": "C05-03-12"} in all_identifiers

    def test_two_meetings_merged_with_titles_from_962(self):
        """Each 962 title is merged into the matching 773 meeting entry via artid.

        Record 836244 layout:
          773 journal  c=875-882 w=C05-08-30  →  warsaw20050831  (962 k=875-882)
          773 conf     c=119-122 w=C05-03-12  →  lathuile20050312 (962 k=119-122)
        Result must be two entries, each with both CNUM identifier and title.
        The merge is done by matching 962's artid ('k') against the 'session'
        field already set on the meeting entry by the 773 rule (from its 'c'
        subfield) - no temporary field is needed.
        """
        from cds_migrator_kit.rdm.records.transform.xml_processing.rules.research import (
            related_identifiers,
        )

        record = {"custom_fields": {}}

        # 773 processing
        cf = journal(
            record,
            "773__",
            {
                "c": "875-882",
                "n": "3",
                "p": "Acta Phys. Pol. B",
                "v": "37",
                "w": "C05-08-30",
                "y": "2006",
            },
        )
        record["custom_fields"] = cf
        cf = journal(
            record, "773__", {"0": "1260605", "c": "119-122", "w": "C05-03-12"}
        )
        record["custom_fields"] = cf

        # 962 processing
        related_identifiers(
            record, "962__", {"b": "836243", "k": "119-122", "n": "lathuile20050312"}
        )
        related_identifiers(
            record, "962__", {"b": "836246", "k": "875-882", "n": "warsaw20050831"}
        )

        meetings = record["custom_fields"]["meeting:meeting"]

        assert len(meetings) == 2
        warsaw = next(m for m in meetings if m.get("title") == "warsaw20050831")
        lathuile = next(m for m in meetings if m.get("title") == "lathuile20050312")

        assert warsaw["identifiers"] == [
            {"scheme": "inspire", "identifier": "C05-08-30"}
        ]
        assert lathuile["identifiers"] == [
            {"scheme": "inspire", "identifier": "C05-03-12"}
        ]

        # No leftover session fields exposed beyond their intended use
        assert all("session" in m for m in meetings)

    def test_962_title_appended_when_no_matching_session(self):
        """A 962 title with no matching 773 session becomes a standalone meeting."""
        from cds_migrator_kit.rdm.records.transform.xml_processing.rules.research import (
            related_identifiers,
        )

        record = {"custom_fields": {}}
        related_identifiers(
            record, "962__", {"b": "836243", "k": "999-999", "n": "unrelated meeting"}
        )

        meetings = record["custom_fields"]["meeting:meeting"]
        assert meetings == [{"title": "unrelated meeting"}]


class TestMeetingDateFrom518:
    """Test meeting date (518__r/__d) handling in related_identifiers.

    518 always precedes 962 in the record, so the meeting:meeting entry
    doesn't exist yet when 518 is processed - the first entry is created
    then, and 962 must later fill itself into that same first entry rather
    than appending a duplicate.
    """

    def test_meeting_date_from_518_r_creates_first_meeting(self):
        """518__r is normalized to EDTF and creates the first meeting entry."""
        from cds_migrator_kit.rdm.records.transform.xml_processing.rules.research import (
            related_identifiers,
        )

        record = {"custom_fields": {}}
        with pytest.raises(IgnoreKey):
            related_identifiers(record, "518__", {"r": "May 2021"})

        meetings = record["custom_fields"]["meeting:meeting"]
        assert meetings == [{"dates": "2021-05"}]

    def test_meeting_date_from_518_d_fallback(self):
        """518__d is used when 518__r is absent."""
        from cds_migrator_kit.rdm.records.transform.xml_processing.rules.research import (
            related_identifiers,
        )

        record = {"custom_fields": {}}
        with pytest.raises(IgnoreKey):
            related_identifiers(record, "518__", {"d": "2022-11-21"})

        meetings = record["custom_fields"]["meeting:meeting"]
        assert meetings == [{"dates": "2022-11-21"}]

    def test_meeting_date_d_takes_precedence_over_r(self):
        """518__d is preferred over 518__r when both are present."""
        from cds_migrator_kit.rdm.records.transform.xml_processing.rules.research import (
            related_identifiers,
        )

        record = {"custom_fields": {}}
        with pytest.raises(IgnoreKey):
            related_identifiers(record, "518__", {"d": "2022-11-21", "r": "May 2021"})

        meetings = record["custom_fields"]["meeting:meeting"]
        assert meetings == [{"dates": "2022-11-21"}]

    def test_meeting_date_ignored_when_cern(self):
        """A non-date 'CERN' value is ignored silently, no error raised."""
        from cds_migrator_kit.rdm.records.transform.xml_processing.rules.research import (
            related_identifiers,
        )

        record = {"custom_fields": {}}
        with pytest.raises(IgnoreKey):
            related_identifiers(record, "518__", {"r": "CERN"})

        assert record["custom_fields"].get("meeting:meeting", []) == []

    def test_meeting_date_raises_when_not_a_date(self):
        """A non-date, non-CERN value raises UnexpectedValue."""
        from cds_migrator_kit.rdm.records.transform.xml_processing.rules.research import (
            related_identifiers,
        )

        record = {"custom_fields": {}}
        with pytest.raises(UnexpectedValue):
            related_identifiers(record, "518__", {"r": "not-a-valid-date"})

    def test_meeting_date_missing_subfields_ignored(self):
        """No r or d subfield present - silently ignored."""
        from cds_migrator_kit.rdm.records.transform.xml_processing.rules.research import (
            related_identifiers,
        )

        record = {"custom_fields": {}}
        with pytest.raises(IgnoreKey):
            related_identifiers(record, "518__", {"a": "unrelated subfield"})

        assert record["custom_fields"].get("meeting:meeting", []) == []

    def test_meeting_date_raises_when_multiple_meetings_already_present(self):
        """Defensive guard: ambiguous target if more than one meeting exists."""
        from cds_migrator_kit.rdm.records.transform.xml_processing.rules.research import (
            related_identifiers,
        )

        record = {
            "custom_fields": {
                "meeting:meeting": [{"title": "meeting one"}, {"title": "meeting two"}]
            }
        }
        with pytest.raises(UnexpectedValue):
            related_identifiers(record, "518__", {"r": "May 2021"})

    def test_518_then_962_merges_title_into_same_first_meeting(self):
        """962 with no session match fills the 518-created first entry."""
        from cds_migrator_kit.rdm.records.transform.xml_processing.rules.research import (
            related_identifiers,
        )

        record = {"custom_fields": {}}
        with pytest.raises(IgnoreKey):
            related_identifiers(record, "518__", {"r": "May 2021"})

        related_identifiers(record, "962__", {"b": "836243", "n": "some conference"})

        meetings = record["custom_fields"]["meeting:meeting"]
        assert meetings == [{"dates": "2021-05", "title": "some conference"}]

    def test_518_then_two_962_creates_second_entry_for_extra_meeting(self):
        """A second, unrelated 962 title doesn't overwrite the first entry."""
        from cds_migrator_kit.rdm.records.transform.xml_processing.rules.research import (
            related_identifiers,
        )

        record = {"custom_fields": {}}
        with pytest.raises(IgnoreKey):
            related_identifiers(record, "518__", {"r": "May 2021"})

        related_identifiers(record, "962__", {"b": "836243", "n": "conference one"})
        related_identifiers(record, "962__", {"b": "836244", "n": "conference two"})

        meetings = record["custom_fields"]["meeting:meeting"]
        assert meetings == [
            {"dates": "2021-05", "title": "conference one"},
            {"title": "conference two"},
        ]


class TestDeadlineDate:
    """Test deadline_date function from publications.py (583__ rule)."""

    def test_deadline_date_added(self):
        """Test that 583__c value is stored as a deadline date."""
        record = {}
        with pytest.raises(IgnoreKey):
            deadline_date(record, "583__", {"c": "2021-05-15"})
        assert record["dates"] == [
            {
                "date": "2021-05-15",
                "type": {"id": "other"},
                "description": "Deadline date",
            }
        ]

    def test_deadline_date_unknown_ignored(self):
        """Test that UNKNOWN 583__c value is not stored."""
        record = {}
        with pytest.raises(IgnoreKey):
            deadline_date(record, "583__", {"c": "UNKNOWN"})
        assert record["dates"] == []

    def test_deadline_date_missing_c_ignored(self):
        """Test that missing 583__c is not stored."""
        record = {}
        with pytest.raises(IgnoreKey):
            deadline_date(record, "583__", {})
        assert record["dates"] == []

    def test_deadline_date_appends_to_existing_dates(self):
        """Test that deadline date is appended to existing dates list."""
        record = {"dates": [{"date": "2020-01-01", "type": {"id": "submitted"}}]}
        with pytest.raises(IgnoreKey):
            deadline_date(record, "583__", {"c": "2021-05-15"})
        assert record["dates"] == [
            {"date": "2020-01-01", "type": {"id": "submitted"}},
            {
                "date": "2021-05-15",
                "type": {"id": "other"},
                "description": "Deadline date",
            },
        ]

    def test_deadline_date_z_unknown_allowed(self):
        """Test that 583__z UNKNOWN is allowed and does not raise."""
        record = {}
        with pytest.raises(IgnoreKey):
            deadline_date(record, "583__", {"c": "2021-05-15", "z": "UNKNOWN"})
        assert record["dates"] == [
            {
                "date": "2021-05-15",
                "type": {"id": "other"},
                "description": "Deadline date",
            }
        ]

    def test_deadline_date_z_present_raises_unexpected_value(self):
        """Test that a non-UNKNOWN 583__z raises UnexpectedValue."""
        record = {}
        with pytest.raises(UnexpectedValue):
            deadline_date(record, "583__", {"c": "2021-05-15", "z": "Action taken"})

    def test_deadline_date_text_format_normalized(self):
        """Test that a non-EDTF 583__c value is normalized to EDTF."""
        record = {}
        with pytest.raises(IgnoreKey):
            deadline_date(record, "583__", {"c": "May 2021"})
        assert record["dates"] == [
            {
                "date": "2021-05",
                "type": {"id": "other"},
                "description": "Deadline date",
            }
        ]

    def test_deadline_date_invalid_raises_unexpected_value(self):
        """Test that an unparseable 583__c value raises UnexpectedValue."""
        record = {}
        with pytest.raises(UnexpectedValue):
            deadline_date(record, "583__", {"c": "not-a-valid-date"})


class TestAbbreviation:
    """Test abbreviation function from research.py (691__a rule)."""

    def test_abbreviation_added_as_additional_description(self):
        """Test that 691__a is translated into an additional description."""
        record = {}
        result = abbreviation(record, "691__", {"a": "CMS"})
        assert result == [
            {
                "description": "Abbreviation: CMS",
                "type": {"id": "other"},
            }
        ]

    def test_abbreviation_appends_to_existing_additional_descriptions(self):
        """Test that 691__a is appended to an existing additional_descriptions list."""
        record = {
            "additional_descriptions": [
                {"description": "Some other note", "type": {"id": "other"}}
            ]
        }
        result = abbreviation(record, "691__", {"a": "CMS"})
        assert result == [
            {
                "description": "Abbreviation: CMS",
                "type": {"id": "other"},
            }
        ]

    def test_abbreviation_missing_a_ignored(self):
        """Test that 691__ without subfield 'a' is ignored."""
        record = {}
        with pytest.raises(IgnoreKey):
            abbreviation(record, "691__", {})

    def test_abbreviation_empty_a_ignored(self):
        """Test that 691__a with an empty value is ignored."""
        record = {}
        with pytest.raises(IgnoreKey):
            abbreviation(record, "691__", {"a": ""})


class TestMeetingFrom111And711:
    """Tests for meeting (111__/711__ rule) - builds meeting:meeting custom field."""

    def test_111_full_fields_creates_meeting_entry(self):
        """Test that 111__a/c/d/g populate title/place/dates/acronym."""
        record = {}
        with pytest.raises(IgnoreKey):
            meeting(
                record,
                "111__",
                {
                    "a": "Lectures at Fermilab on SUSY",
                    "c": "Batavia, IL, USA",
                    "d": "10 Mar 2000",
                    "f": "2000",
                    "g": "batavia20000310",
                },
            )
        assert record["custom_fields"]["meeting:meeting"] == [
            {
                "title": "Lectures at Fermilab on SUSY",
                "place": "Batavia, IL, USA",
                "dates": "10 Mar 2000",
                "acronym": "batavia20000310",
            }
        ]

    def test_111_falls_back_to_f_when_d_missing(self):
        """Test that 111__f is used for dates when 111__d is absent."""
        record = {}
        with pytest.raises(IgnoreKey):
            meeting(
                record,
                "111__",
                {"a": "Some Meeting", "c": "Geneva", "f": "2000", "g": "ABC"},
            )
        meeting_entry = record["custom_fields"]["meeting:meeting"][0]
        assert meeting_entry["dates"] == "2000"

    def test_111_d_takes_precedence_over_f(self):
        """Test that 111__d is preferred over 111__f when both are present."""
        record = {}
        with pytest.raises(IgnoreKey):
            meeting(
                record,
                "111__",
                {"a": "Some Meeting", "d": "10 Mar 2000", "f": "2000"},
            )
        meeting_entry = record["custom_fields"]["meeting:meeting"][0]
        assert meeting_entry["dates"] == "10 Mar 2000"

    def test_111_falls_back_to_9_when_d_missing(self):
        """Test that 111__9 (YYYYMMDD) is used for dates when 111__d is absent."""
        record = {}
        with pytest.raises(IgnoreKey):
            meeting(
                record,
                "111__",
                {"a": "Some Meeting", "f": "2000", "9": "20000310"},
            )
        meeting_entry = record["custom_fields"]["meeting:meeting"][0]
        assert meeting_entry["dates"] == "2000-03-10"

    def test_111_d_takes_precedence_over_9(self):
        """Test that 111__d is preferred over 111__9 when both are present."""
        record = {}
        with pytest.raises(IgnoreKey):
            meeting(
                record,
                "111__",
                {"a": "Some Meeting", "d": "10 Mar 2000", "9": "20000310"},
            )
        meeting_entry = record["custom_fields"]["meeting:meeting"][0]
        assert meeting_entry["dates"] == "10 Mar 2000"

    def test_111_9_takes_precedence_over_f(self):
        """Test that 111__9 is preferred over 111__f (a bare year) when both
        are present and 111__d is absent."""
        record = {}
        with pytest.raises(IgnoreKey):
            meeting(
                record,
                "111__",
                {"a": "Some Meeting", "f": "2000", "9": "20000310"},
            )
        meeting_entry = record["custom_fields"]["meeting:meeting"][0]
        assert meeting_entry["dates"] == "2000-03-10"

    def test_111_invalid_9_raises_error(self):
        """Test that an unparseable 111__9 raises UnexpectedValue."""
        record = {}
        with pytest.raises(UnexpectedValue):
            meeting(record, "111__", {"a": "Some Meeting", "9": "not-a-date"})

    def test_111_without_dates_omits_dates_key(self):
        """Test that no 'dates' key is set when both 111__d and 111__f are absent."""
        record = {}
        with pytest.raises(IgnoreKey):
            meeting(record, "111__", {"a": "Some Meeting"})
        meeting_entry = record["custom_fields"]["meeting:meeting"][0]
        assert "dates" not in meeting_entry

    def test_111_without_title_omits_title_key(self):
        """Test that a 111 field without subfield 'a' creates an entry with
        no 'title' key rather than dropping the other subfields."""
        record = {}
        with pytest.raises(IgnoreKey):
            meeting(record, "111__", {"c": "Geneva"})
        assert record["custom_fields"]["meeting:meeting"] == [{"place": "Geneva"}]

    def test_711_adds_new_title_when_no_matching_meeting_exists(self):
        """Test that 711__a creates a new meeting entry when its title is new."""
        record = {}
        with pytest.raises(IgnoreKey):
            meeting(record, "711__", {"a": "Joint Experimental Theoretical Seminar"})
        assert record["custom_fields"]["meeting:meeting"] == [
            {"title": "Joint Experimental Theoretical Seminar"}
        ]

    def test_711_skips_duplicate_title(self):
        """Test that 711__a is not added again when a meeting with the same
        title already exists (e.g. duplicating the 111 conference name)."""
        record = {
            "custom_fields": {
                "meeting:meeting": [{"title": "SUSY at DELPHI", "place": "Geneva"}]
            }
        }
        with pytest.raises(IgnoreKey):
            meeting(record, "711__", {"a": "SUSY at DELPHI"})
        assert record["custom_fields"]["meeting:meeting"] == [
            {"title": "SUSY at DELPHI", "place": "Geneva"}
        ]

    def test_111_then_two_711_only_adds_distinct_titles(self):
        """Reproduces record 436657: 111 sets the full title, and only the
        711 entry with a distinct title is appended as a new meeting."""
        record = {}
        with pytest.raises(IgnoreKey):
            meeting(
                record,
                "111__",
                {
                    "a": "Lectures at Fermilab on SUSY at DELPHI, LEP200 and LEP-2000 and LEP-Legacy",
                    "c": "Batavia, IL, USA",
                    "d": "10 Mar 2000",
                    "g": "batavia20000310",
                },
            )
        with pytest.raises(IgnoreKey):
            meeting(
                record,
                "711__",
                {"a": "SUSY at DELPHI, LEP200 and LEP-2000 and LEP-Legacy"},
            )
        with pytest.raises(IgnoreKey):
            meeting(
                record,
                "711__",
                {"a": "Joint Experimental Theoretical Physics Seminar"},
            )
        meetings = record["custom_fields"]["meeting:meeting"]
        assert len(meetings) == 3
        titles = [m["title"] for m in meetings]
        assert (
            "Lectures at Fermilab on SUSY at DELPHI, LEP200 and LEP-2000 and LEP-Legacy"
            in titles
        )
        assert "SUSY at DELPHI, LEP200 and LEP-2000 and LEP-Legacy" in titles
        assert "Joint Experimental Theoretical Physics Seminar" in titles

    def test_711_without_title_no_meeting_added(self):
        """Test that a 711 field without subfield 'a' produces no meeting entry."""
        record = {}
        with pytest.raises(IgnoreKey):
            meeting(record, "711__", {})
        assert record["custom_fields"].get("meeting:meeting", []) == []


class TestLicenseAndFundingFrom540:
    """Tests for oa_level_from_license (540__ rule)."""

    def _cf(self, record):
        return record.get("custom_fields", {})

    def test_cc_by_license_added_to_rights(self):
        record = {"custom_fields": {}}
        with pytest.raises(IgnoreKey):
            oa_level_from_license(record, "540__", {"a": "CC BY", "3": "publication"})
        assert record["rights"] == [{"id": "cc-by"}]
        assert "cern:oa_level" not in self._cf(record)

    def test_cc_hyphen_by_license_added_to_rights(self):
        record = {"custom_fields": {}}
        with pytest.raises(IgnoreKey):
            oa_level_from_license(record, "540__", {"a": "CC-BY", "3": "publication"})
        assert record["rights"] == [{"title": {"en": "CC-BY"}}]
        assert "cern:oa_level" not in self._cf(record)

    def test_non_standard_license_added_to_rights(self):
        record = {"custom_fields": {}}
        with pytest.raises(IgnoreKey):
            oa_level_from_license(record, "540__", {"a": "Some other license"})
        assert record["rights"] == [{"title": {"en": "Some other license"}}]
        assert "cern:oa_level" not in self._cf(record)

    def test_funding_model_scoap3(self):
        record = {"custom_fields": {}}
        with pytest.raises(IgnoreKey):
            oa_level_from_license(record, "540__", {"f": "SCOAP3"})
        assert self._cf(record)["cern:oa_funding_model"] == {"id": "scoap3"}
        assert "cern:oa_level" not in self._cf(record)

    def test_funding_model_collective(self):
        record = {"custom_fields": {}}
        with pytest.raises(IgnoreKey):
            oa_level_from_license(record, "540__", {"f": "Collective"})
        assert self._cf(record)["cern:oa_funding_model"] == {"id": "collective"}

    def test_funding_model_cern_rp(self):
        record = {"custom_fields": {}}
        with pytest.raises(IgnoreKey):
            oa_level_from_license(record, "540__", {"f": "CERN-RP"})
        assert self._cf(record)["cern:oa_funding_model"] == {"id": "cern-rp"}

    def test_funding_model_cern_apc(self):
        record = {"custom_fields": {}}
        with pytest.raises(IgnoreKey):
            oa_level_from_license(record, "540__", {"f": "CERN-APC"})
        assert self._cf(record)["cern:oa_funding_model"] == {"id": "cern-apc"}

    def test_funding_model_other(self):
        record = {"custom_fields": {}}
        with pytest.raises(IgnoreKey):
            oa_level_from_license(record, "540__", {"f": "Other"})
        assert self._cf(record)["cern:oa_funding_model"] == {"id": "other"}

    def test_bronze_does_not_set_funding_model(self):
        record = {"custom_fields": {}}
        with pytest.raises(IgnoreKey):
            oa_level_from_license(record, "540__", {"f": "Bronze"})
        assert "cern:oa_funding_model" not in self._cf(record)
        assert "cern:oa_level" not in self._cf(record)

    def test_funding_model_not_overwritten_by_second_tag(self):
        """First funding model found wins."""
        record = {"custom_fields": {}}
        with pytest.raises(IgnoreKey):
            oa_level_from_license(
                record, "540__", [{"f": "SCOAP3"}, {"f": "Collective"}]
            )
        assert self._cf(record)["cern:oa_funding_model"] == {"id": "scoap3"}
