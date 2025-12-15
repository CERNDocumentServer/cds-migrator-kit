# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Tests for IT migration rules."""

import pytest
from dojson.errors import IgnoreKey

from cds_migrator_kit.errors import MissingRequiredField, UnexpectedValue
from cds_migrator_kit.rdm.records.transform.xml_processing.rules.it import (
    access_grants,
    additional_descriptions,
    additional_titles,
    collection,
    conference_title,
    corporate_author,
    imprint,
    imprint_dates,
    related_works,
    resource_type,
    subjects,
    supervisor,
    translated_description,
)


class TestResourceTypePrecedence:
    """Test resource_type function precedence logic.

    Precedence order (highest to lowest priority):
    conferencepaper > bookchapter > itcerntalk > slides > article >
    preprint > intnotetspubl > intnoteitpubl > note
    """

    def test_resource_type_no_existing_value(self):
        """Test resource_type when no existing value exists."""
        record = {}
        result = resource_type(record, "980__", {"a": "conferencepaper"})
        assert result == {"id": "publication-conferencepaper"}

    def test_resource_type_higher_priority_replaces(self):
        """Test that higher priority resource type replaces existing one."""
        # Start with 'note' (lowest priority)
        record = {"resource_type": {"id": "publication-technicalnote"}}

        # 'article' should replace 'note' (higher priority)
        result = resource_type(record, "980__", {"a": "article"})
        assert result == {"id": "publication-article"}

    def test_resource_type_lower_priority_ignored(self):
        """Test that lower priority resource type is ignored."""
        # Start with 'conferencepaper' (highest priority)
        record = {"resource_type": {"id": "publication-conferencepaper"}}

        # 'note' should be ignored (lower priority)
        with pytest.raises(IgnoreKey):
            resource_type(record, "980__", {"a": "note"})

    def test_resource_type_conferencepaper_over_bookchapter(self):
        """Test conferencepaper (priority 1) beats bookchapter (priority 2)."""
        record = {"resource_type": {"id": "publication-section"}}  # bookchapter

        result = resource_type(record, "980__", {"a": "conferencepaper"})
        assert result == {"id": "publication-conferencepaper"}

    def test_resource_type_bookchapter_over_itcerntalk(self):
        """Test bookchapter (priority 2) beats itcerntalk (priority 3)."""
        record = {"resource_type": {"id": "presentation"}}  # itcerntalk

        result = resource_type(record, "980__", {"a": "bookchapter"})
        assert result == {"id": "publication-section"}

    def test_resource_type_itcerntalk_over_article(self):
        """Test itcerntalk (priority 3) beats article (priority 5)."""
        record = {"resource_type": {"id": "publication-article"}}  # article

        result = resource_type(record, "980__", {"a": "itcerntalk"})
        assert result == {"id": "presentation"}

    def test_resource_type_slides_over_article(self):
        """Test slides (priority 4) beats article (priority 5)."""
        record = {"resource_type": {"id": "publication-article"}}  # article

        result = resource_type(record, "980__", {"a": "slides"})
        assert result == {"id": "presentation"}

    def test_resource_type_article_over_preprint(self):
        """Test article (priority 5) beats preprint (priority 6)."""
        record = {"resource_type": {"id": "publication-preprint"}}  # preprint

        result = resource_type(record, "980__", {"a": "article"})
        assert result == {"id": "publication-article"}

    def test_resource_type_preprint_over_intnotetspubl(self):
        """Test preprint (priority 6) beats intnotetspubl (priority 7)."""
        record = {"resource_type": {"id": "publication-technicalnote"}}  # intnotetspubl

        result = resource_type(record, "980__", {"a": "preprint"})
        assert result == {"id": "publication-preprint"}

    def test_resource_type_intnotetspubl_over_intnoteitpubl(self):
        """Test intnotetspubl (priority 7) beats intnoteitpubl (priority 8)."""
        record = {"resource_type": {"id": "publication-technicalnote"}}  # intnoteitpubl

        result = resource_type(record, "980__", {"a": "intnotetspubl"})
        assert result == {"id": "publication-technicalnote"}

    def test_resource_type_intnoteitpubl_over_note(self):
        """Test intnoteitpubl (priority 8) beats note (priority 9)."""
        record = {"resource_type": {"id": "publication-technicalnote"}}  # note

        result = resource_type(record, "980__", {"a": "intnoteitpubl"})
        assert result == {"id": "publication-technicalnote"}

    def test_resource_type_multiple_candidates_highest_wins(self):
        """Test when both value_a and value_b exist, highest priority wins."""
        record = {}

        # Both 'note' and 'conferencepaper' present, conferencepaper should win
        result = resource_type(record, "980__", {"a": "note", "b": "conferencepaper"})
        assert result == {"id": "publication-conferencepaper"}

    def test_resource_type_multiple_candidates_from_middle(self):
        """Test multiple candidates with middle-range priorities."""
        record = {}

        # Both 'article' and 'slides' present, slides should win (higher priority)
        result = resource_type(record, "980__", {"a": "article", "b": "slides"})
        assert result == {"id": "presentation"}

    def test_resource_type_full_precedence_chain(self):
        """Test full precedence chain with multiple updates."""
        record = {}

        # Start with 'note' (lowest priority)
        result = resource_type(record, "980__", {"a": "note"})
        assert result == {"id": "publication-technicalnote"}
        record["resource_type"] = result

        # Update with 'article' (should replace)
        result = resource_type(record, "980__", {"a": "article"})
        assert result == {"id": "publication-article"}
        record["resource_type"] = result

        # Try to update with 'preprint' (should be ignored - lower priority)
        with pytest.raises(IgnoreKey):
            resource_type(record, "980__", {"a": "preprint"})

        # Update with 'slides' (should replace)
        result = resource_type(record, "980__", {"a": "slides"})
        assert result == {"id": "presentation"}
        record["resource_type"] = result

        # Update with 'conferencepaper' (should replace - highest priority)
        result = resource_type(record, "980__", {"a": "conferencepaper"})
        assert result == {"id": "publication-conferencepaper"}
        record["resource_type"] = result

        # Try to update with anything else (should all be ignored)
        with pytest.raises(IgnoreKey):
            resource_type(record, "980__", {"a": "note"})
        with pytest.raises(IgnoreKey):
            resource_type(record, "980__", {"a": "bookchapter"})

    def test_resource_type_case_insensitive(self):
        """Test that resource_type matching is case-insensitive."""
        record = {}

        # Test uppercase
        result = resource_type(record, "980__", {"a": "CONFERENCEPAPER"})
        assert result == {"id": "publication-conferencepaper"}

        # Test mixed case
        record = {}
        result = resource_type(record, "980__", {"a": "ConferencePaper"})
        assert result == {"id": "publication-conferencepaper"}

    def test_resource_type_ignore_publarda(self):
        """Test that 'publarda' is ignored."""
        record = {}

        with pytest.raises(IgnoreKey):
            resource_type(record, "980__", {"a": "publarda"})

    def test_resource_type_empty_values_ignored(self):
        """Test that empty values are ignored."""
        record = {}

        with pytest.raises(IgnoreKey):
            resource_type(record, "980__", {"a": ""})

        with pytest.raises(IgnoreKey):
            resource_type(record, "980__", {})

    def test_resource_type_with_multiple_candidates_same_call(self):
        """Test when both candidates in same call map to different IDs."""
        record = {}

        # Both 'bookchapter' and 'article' present, bookchapter should win (higher priority)
        result = resource_type(record, "980__", {"a": "article", "b": "bookchapter"})
        assert result == {"id": "publication-section"}  # bookchapter maps to this

    def test_resource_type_all_precedence_levels(self):
        """Comprehensive test of all precedence levels."""
        precedence_map = [
            ("conferencepaper", {"id": "publication-conferencepaper"}),
            ("bookchapter", {"id": "publication-section"}),
            ("itcerntalk", {"id": "presentation"}),
            ("slides", {"id": "presentation"}),
            ("article", {"id": "publication-article"}),
            ("preprint", {"id": "publication-preprint"}),
            ("intnotetspubl", {"id": "publication-technicalnote"}),
            ("intnoteitpubl", {"id": "publication-technicalnote"}),
            ("note", {"id": "publication-technicalnote"}),
        ]

        # Test each can be set initially
        for resource_value, expected_mapping in precedence_map:
            record = {}
            result = resource_type(record, "980__", {"a": resource_value})
            assert result == expected_mapping

    def test_resource_type_bookchapter_cannot_override_conferencepaper(self):
        """Test specific case: bookchapter cannot override conferencepaper."""
        record = {"resource_type": {"id": "publication-conferencepaper"}}

        with pytest.raises(IgnoreKey):
            resource_type(record, "980__", {"a": "bookchapter"})

    def test_resource_type_note_cannot_override_anything(self):
        """Test that 'note' (lowest priority) cannot override any existing type."""
        all_types = [
            ("conferencepaper", {"id": "publication-conferencepaper"}),
            ("bookchapter", {"id": "publication-section"}),
            ("article", {"id": "publication-article"}),
            ("preprint", {"id": "publication-preprint"}),
        ]

        for existing_type, existing_mapping in all_types:
            record = {"resource_type": existing_mapping}
            with pytest.raises(IgnoreKey):
                resource_type(record, "980__", {"a": "note"})

    def test_resource_type_conferencepaper_overrides_everything(self):
        """Test that 'conferencepaper' (highest priority) overrides all existing types."""
        all_types = [
            ("bookchapter", {"id": "publication-section"}),
            ("article", {"id": "publication-article"}),
            ("preprint", {"id": "publication-preprint"}),
            ("note", {"id": "publication-technicalnote"}),
        ]

        for existing_type, existing_mapping in all_types:
            record = {"resource_type": existing_mapping}
            result = resource_type(record, "980__", {"a": "conferencepaper"})
            assert result == {"id": "publication-conferencepaper"}


class TestAccessGrants:
    """Test access_grants function."""

    def test_access_grants_with_d_field(self):
        """Test access_grants with 'd' field (user email)."""
        record = {}
        result = access_grants(record, "5061__", {"d": "user@cern.ch"})
        assert result == [{"user@cern.ch": "view"}]

    def test_access_grants_with_m_field(self):
        """Test access_grants with 'm' field (group name)."""
        record = {}
        result = access_grants(record, "5061__", {"m": "it-group"})
        assert result == [{"it-group": "view"}]

    def test_access_grants_with_a_field(self):
        """Test access_grants with 'a' field."""
        record = {}
        result = access_grants(record, "5061__", {"a": "admin-group"})
        assert result == [{"admin-group": "view"}]

    def test_access_grants_priority_d_over_m(self):
        """Test that 'd' field takes priority over 'm'."""
        record = {}
        result = access_grants(record, "5061__", {"d": "user@cern.ch", "m": "group"})
        assert result == [{"user@cern.ch": "view"}]

    def test_access_grants_priority_m_over_a(self):
        """Test that 'm' field takes priority over 'a'."""
        record = {}
        result = access_grants(record, "5061__", {"m": "group", "a": "admin"})
        assert result == [{"group": "view"}]

    def test_access_grants_empty_value_ignored(self):
        """Test that empty values are ignored."""
        record = {}
        with pytest.raises(IgnoreKey):
            access_grants(record, "5061__", {"d": ""})

    def test_access_grants_no_fields_ignored(self):
        """Test that missing fields raise IgnoreKey."""
        record = {}
        with pytest.raises(IgnoreKey):
            access_grants(record, "5061__", {})


class TestCorporateAuthor:
    """Test corporate_author function."""

    def test_corporate_author_basic(self):
        """Test basic corporate author translation."""
        record = {}
        result = corporate_author(record, "110__", {"a": "CERN IT Department"})
        assert result == [
            {
                "person_or_org": {
                    "type": "organizational",
                    "name": "CERN IT Department",
                    "family_name": "CERN IT Department",
                }
            }
        ]

    def test_corporate_author_cern_geneva_normalized(self):
        """Test that 'CERN. Geneva' is normalized to 'CERN'."""
        record = {}
        result = corporate_author(record, "110__", {"a": "CERN. Geneva"})
        assert result == [
            {
                "person_or_org": {
                    "type": "organizational",
                    "name": "CERN",
                    "family_name": "CERN",
                }
            }
        ]

    def test_corporate_author_missing_field_a(self):
        """Test that missing 'a' field raises IgnoreKey."""
        record = {}
        with pytest.raises(IgnoreKey):
            corporate_author(record, "110__", {})


class TestCollection:
    """Test collection function."""

    def test_collection_article_ignored(self):
        """Test that 'article' collection is ignored."""
        record = {}
        with pytest.raises(IgnoreKey):
            collection(record, "690C_", {"a": "article"})

    def test_collection_cern_ignored(self):
        """Test that 'cern' collection is ignored."""
        record = {}
        with pytest.raises(IgnoreKey):
            collection(record, "690C_", {"a": "CERN"})

    def test_collection_yellow_report_adds_subject(self):
        """Test that 'yellow report' adds to subjects."""
        record = {}
        with pytest.raises(IgnoreKey):
            collection(record, "690C_", {"a": "yellow report"})
        assert {"subject": "collection:YELLOW REPORT"} in record["subjects"]

    def test_collection_yellowrepcontrib_adds_subject(self):
        """Test that 'yellowrepcontrib' adds to subjects."""
        record = {}
        with pytest.raises(IgnoreKey):
            collection(record, "690C_", {"a": "yellowrepcontrib"})
        assert {"subject": "collection:YELLOWREPCONTRIB"} in record["subjects"]

    def test_collection_publarda_adds_project(self):
        """Test that 'publarda' adds ARDA project."""
        record = {}
        with pytest.raises(IgnoreKey):
            collection(record, "690C_", {"a": "publarda"})
        assert "ARDA" in record["custom_fields"]["cern:projects"]

    def test_collection_publarda_no_duplicates(self):
        """Test that 'publarda' doesn't create duplicate ARDA projects."""
        record = {"custom_fields": {"cern:projects": ["ARDA"]}}
        with pytest.raises(IgnoreKey):
            collection(record, "690C_", {"a": "publarda"})
        # Should still have only one ARDA
        assert record["custom_fields"]["cern:projects"].count("ARDA") == 1


class TestAdditionalDescriptions:
    """Test additional_descriptions function."""

    def test_additional_descriptions_500_field(self):
        """Test 500__ field creates 'other' type description."""
        record = {}
        result = additional_descriptions(
            record, "500__", {"a": "This is a description"}
        )
        assert result == [
            {
                "description": "This is a description",
                "type": {"id": "other"},
            }
        ]

    def test_additional_descriptions_935_field(self):
        """Test 935__ field creates 'technical-info' type description."""
        record = {}
        result = additional_descriptions(
            record, "935__", {"a": "Technical information here"}
        )
        assert result == [
            {
                "description": "Technical information here",
                "type": {"id": "technical-info"},
            }
        ]

    def test_additional_descriptions_empty_text_ignored(self):
        """Test that empty description text is ignored."""
        record = {}
        with pytest.raises(IgnoreKey):
            additional_descriptions(record, "500__", {"a": ""})


class TestSubjects:
    """Test subjects function."""

    def test_subjects_talk_added(self):
        """Test that 'Talk' is added as subject."""
        record = {"subjects": []}
        with pytest.raises(IgnoreKey):
            subjects(record, "6931_", {"a": "Talk"})
        assert {"subject": "Talk"} in record["subjects"]

    def test_subjects_lecture_added(self):
        """Test that 'Lecture' is added as subject."""
        record = {"subjects": []}
        with pytest.raises(IgnoreKey):
            subjects(record, "6931_", {"a": "Lecture"})
        assert {"subject": "Lecture"} in record["subjects"]

    def test_subjects_desy_ignored(self):
        """Test that DESY subjects are ignored."""
        record = {}
        with pytest.raises(IgnoreKey):
            subjects(record, "694__", {"a": "Some subject", "9": "DESY"})

    def test_subjects_jacow_added(self):
        """Test that JACoW subjects are added."""
        record = {"subjects": []}
        with pytest.raises(IgnoreKey):
            subjects(record, "695__", {"a": "Conference", "9": "JACoW"})
        assert {"subject": "Conference"} in record["subjects"]
        assert {"subject": "JACoW"} in record["subjects"]

    def test_subjects_xx_ignored(self):
        """Test that 'XX' subject is ignored."""
        record = {}
        with pytest.raises(IgnoreKey):
            subjects(record, "6931_", {"a": "XX"})


class TestSupervisor:
    """Test supervisor function."""

    def test_supervisor_valid(self):
        """Test valid supervisor translation."""
        record = {}
        result = supervisor(record, "906__", {"p": "John Doe"})
        assert result == [
            {
                "person_or_org": {
                    "type": "personal",
                    "name": "John Doe",
                    "family_name": "John Doe",
                },
                "role": {"id": "supervisor"},
            }
        ]

    def test_supervisor_missing_field_p(self):
        """Test that missing 'p' field raises MissingRequiredField."""
        record = {}
        with pytest.raises(MissingRequiredField):
            supervisor(record, "906__", {})

    def test_supervisor_empty_field_p(self):
        """Test that empty 'p' field raises MissingRequiredField."""
        record = {}
        with pytest.raises(MissingRequiredField):
            supervisor(record, "906__", {"p": ""})


class TestImprint:
    """Test imprint function."""

    def test_imprint_edition(self):
        """Test that edition is added to imprint."""
        record = {}
        with pytest.raises(IgnoreKey):
            imprint(record, "250__", {"a": "2nd edition"})
        assert record["custom_fields"]["imprint:imprint"]["edition"] == "2nd edition"

    def test_imprint_updates_existing(self):
        """Test that edition updates existing imprint."""
        record = {"custom_fields": {"imprint:imprint": {"place": "Geneva"}}}
        with pytest.raises(IgnoreKey):
            imprint(record, "250__", {"a": "1st edition"})
        assert record["custom_fields"]["imprint:imprint"]["edition"] == "1st edition"
        assert record["custom_fields"]["imprint:imprint"]["place"] == "Geneva"


class TestImprintDates:
    """Test imprint_dates function."""

    def test_imprint_dates_basic(self):
        """Test basic publication date parsing."""
        record = {}
        with pytest.raises(IgnoreKey):
            imprint_dates(record, "269__", {"c": "2021"})
        assert record["publication_date"] == "2021"

    def test_imprint_dates_with_place(self):
        """Test imprint place is added."""
        record = {}
        with pytest.raises(IgnoreKey):
            imprint_dates(record, "269__", {"a": "Geneva.", "c": "2021"})
        assert record["custom_fields"]["imprint:imprint"]["place"] == "Geneva"

    def test_imprint_dates_with_publisher(self):
        """Test publisher is added when not already set."""
        record = {}
        with pytest.raises(IgnoreKey):
            imprint_dates(record, "269__", {"b": "CERN", "c": "2021"})
        assert record["publisher"] == "CERN"

    def test_imprint_dates_publisher_not_overwritten(self):
        """Test publisher is not overwritten if already set."""
        record = {"publisher": "Existing Publisher"}
        with pytest.raises(IgnoreKey):
            imprint_dates(record, "269__", {"b": "CERN", "c": "2021"})
        assert record["publisher"] == "Existing Publisher"

    def test_imprint_dates_with_question_mark(self):
        """Test publication date with question mark creates dates entry."""
        record = {}
        with pytest.raises(IgnoreKey):
            imprint_dates(record, "269__", {"c": "2021?"})
        assert record["publication_date"] == "2021"
        assert len(record["dates"]) == 1
        assert record["dates"][0]["type"]["id"] == "created"
        assert "indeterminate" in record["dates"][0]["description"]

    def test_imprint_dates_no_pub_date_ignored(self):
        """Test that missing publication date raises IgnoreKey."""
        record = {}
        with pytest.raises(IgnoreKey):
            imprint_dates(record, "269__", {"a": "Geneva"})

    def test_imprint_dates_invalid_date_raises_error(self):
        """Test that invalid date raises UnexpectedValue."""
        record = {}
        with pytest.raises(UnexpectedValue):
            imprint_dates(record, "269__", {"c": "invalid-date"})


class TestConferenceTitle:
    """Test conference_title function."""

    def test_conference_title_added(self):
        """Test that conference title is added to meeting."""
        record = {}
        with pytest.raises(IgnoreKey):
            conference_title(record, "595__", {"d": "Annual Physics Conference"})
        assert (
            record["custom_fields"]["meeting:meeting"]["title"]
            == "Annual Physics Conference"
        )

    def test_conference_title_empty_ignored(self):
        """Test that empty conference title is ignored."""
        record = {}
        with pytest.raises(IgnoreKey):
            conference_title(record, "595__", {})


class TestTranslatedDescription:
    """Test translated_description function."""

    def test_translated_description_basic(self):
        """Test basic translated description."""
        record = {}
        result = translated_description(
            record, "590__", {"a": "Title", "b": "Description"}
        )
        assert result[0]["description"] == "<h2>Title</h2><p>Description</p>"
        assert result[0]["type"]["id"] == "other"
        assert result[0]["lang"]["id"] == "fra"

    def test_translated_description_html_cleanup(self):
        """Test that HTML comments are removed."""
        record = {}
        result = translated_description(
            record, "590__", {"a": "<!--HTML-->Title", "b": "<!--HTML-->Description"}
        )
        assert result[0]["description"] == "<h2>Title</h2><p>Description</p>"

    def test_translated_description_short_text_no_html_tags(self):
        """Test that very short text doesn't get HTML tags."""
        record = {}
        result = translated_description(record, "590__", {"a": "AB", "b": "CD"})
        # Short text (<=3 chars) doesn't trigger HTML formatting
        assert result[0]["description"] == "AB"
        assert result[0]["type"]["id"] == "other"
        assert result[0]["lang"]["id"] == "fra"

    def test_translated_description_only_a_field(self):
        """Test with only 'a' field."""
        record = {}
        result = translated_description(
            record, "590__", {"a": "Long title here", "b": ""}
        )
        assert result[0]["description"] == "<h2>Long title here</h2><p></p>"


class TestRelatedWorks:
    """Test related_works function."""

    def test_related_works_continue_relation(self):
        """Test 'continue' creates 'iscontinuedby' relation."""
        record = {}
        result = related_works(record, "785__", {"i": "Continued by", "w": "12345"})
        assert result == [
            {
                "identifier": "12345",
                "scheme": "cds",
                "relation_type": {"id": "iscontinuedby"},
                "resource_type": {"id": "other"},
            }
        ]

    def test_related_works_references_relation(self):
        """Test other relations create 'references'."""
        record = {}
        result = related_works(record, "785__", {"i": "Related to", "w": "67890"})
        assert result == [
            {
                "identifier": "67890",
                "scheme": "cds",
                "relation_type": {"id": "references"},
                "resource_type": {"id": "other"},
            }
        ]

    def test_related_works_duplicate_ignored(self):
        """Test that duplicate identifiers are ignored."""
        record = {
            "related_identifiers": [
                {
                    "identifier": "12345",
                    "scheme": "cds",
                    "relation_type": {"id": "references"},
                    "resource_type": {"id": "other"},
                }
            ]
        }
        with pytest.raises(IgnoreKey):
            related_works(record, "785__", {"i": "Related to", "w": "12345"})


class TestAdditionalTitles:
    """Test additional_titles function."""

    def test_additional_titles_basic(self):
        """Test basic additional title."""
        record = {}
        result = additional_titles(record, "246_3", {"a": "Alternative Title"})
        assert result == [
            {
                "title": "Alternative Title",
                "type": {"id": "alternative-title"},
            }
        ]

    def test_additional_titles_empty_ignored(self):
        """Test that empty title is ignored."""
        record = {}
        with pytest.raises(IgnoreKey):
            additional_titles(record, "246_3", {"a": ""})
