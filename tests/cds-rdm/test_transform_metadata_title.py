# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Tests for the metadata.title conference-proceedings fallback in _metadata()."""

import pytest

from cds_migrator_kit.rdm.records.transform.transform import CDSToRDMRecordEntry


@pytest.fixture
def entry():
    """Transform entry instance (no DB/app context required)."""
    return CDSToRDMRecordEntry()


def _dump():
    """Minimal record dump for _publication_date()."""
    return {"files": []}


def _json_entry(**overrides):
    """Minimal json_entry with a resource_type and a creation date set."""
    base = {
        "recid": 123,
        "resource_type": {"id": "publication-conferenceproceeding"},
        "status_week_date": "2020-01-01",
    }
    base.update(overrides)
    return base


class TestMetadataTitleFromMeeting:
    """Test the 111__a -> metadata.title fallback for conference proceedings."""

    def test_title_falls_back_to_meeting_title_when_missing(self, entry):
        """Test that a missing title is filled from the meeting:meeting title
        when resource_type is publication-conferenceproceeding."""
        json_entry = _json_entry(
            custom_fields={"meeting:meeting": [{"title": "Some Conference"}]}
        )
        metadata = entry._metadata(json_entry, _dump())
        assert metadata["title"] == "Some Conference"

    def test_title_not_overridden_when_already_present(self, entry):
        """Test that an existing title is not replaced by the meeting title."""
        json_entry = _json_entry(
            title="Real Title",
            custom_fields={"meeting:meeting": [{"title": "Some Conference"}]},
        )
        metadata = entry._metadata(json_entry, _dump())
        assert metadata["title"] == "Real Title"

    def test_title_not_filled_for_other_resource_types(self, entry):
        """Test that the meeting-title fallback only applies to
        publication-conferenceproceeding, not other resource types."""
        json_entry = _json_entry(
            resource_type={"id": "publication-article"},
            custom_fields={"meeting:meeting": [{"title": "Some Conference"}]},
        )
        metadata = entry._metadata(json_entry, _dump())
        assert "title" not in metadata

    def test_title_missing_without_meeting_custom_field(self, entry):
        """Test that title stays unset when there is no meeting:meeting entry
        to fall back to, even for conference proceedings."""
        json_entry = _json_entry()
        metadata = entry._metadata(json_entry, _dump())
        assert "title" not in metadata

    def test_title_uses_first_meeting_entry_with_a_title(self, entry):
        """Test that the fallback skips meeting entries without a title and
        uses the first one that has one."""
        json_entry = _json_entry(
            custom_fields={
                "meeting:meeting": [
                    {"place": "Geneva"},
                    {"title": "Second Meeting Title"},
                ]
            }
        )
        metadata = entry._metadata(json_entry, _dump())
        assert metadata["title"] == "Second Meeting Title"
