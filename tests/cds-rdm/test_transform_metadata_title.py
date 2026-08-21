# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Tests for the metadata.title conference-proceedings fallback in _metadata()."""

import pytest

from cds_migrator_kit.rdm.records.transform.entities.record import RecordEntry


@pytest.fixture
def entry():
    """RecordEntry double for calling _metadata() in isolation.

    Bypasses the constructor (which needs a real dump/dojson_entry) -
    this module only exercises _metadata() directly.
    """
    record_entry = RecordEntry.__new__(RecordEntry)
    record_entry.migration_logger = None
    record_entry.affiliations_mapping = None
    return record_entry


def _raw_dump_entry():
    """Minimal raw dump entry for _publication_date()."""
    return {"files": []}


def _dojson_entry(**overrides):
    """Minimal dojson_entry with a resource_type and a creation date set."""
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
        dojson_entry = _dojson_entry(
            custom_fields={"meeting:meeting": [{"title": "Some Conference"}]}
        )
        metadata = entry._metadata(dojson_entry, _raw_dump_entry())
        assert metadata["title"] == "Some Conference"

    def test_title_not_overridden_when_already_present(self, entry):
        """Test that an existing title is not replaced by the meeting title."""
        dojson_entry = _dojson_entry(
            title="Real Title",
            custom_fields={"meeting:meeting": [{"title": "Some Conference"}]},
        )
        metadata = entry._metadata(dojson_entry, _raw_dump_entry())
        assert metadata["title"] == "Real Title"

    def test_title_not_filled_for_other_resource_types(self, entry):
        """Test that the meeting-title fallback only applies to
        publication-conferenceproceeding, not other resource types."""
        dojson_entry = _dojson_entry(
            resource_type={"id": "publication-article"},
            custom_fields={"meeting:meeting": [{"title": "Some Conference"}]},
        )
        metadata = entry._metadata(dojson_entry, _raw_dump_entry())
        assert "title" not in metadata

    def test_title_missing_without_meeting_custom_field(self, entry):
        """Test that title stays unset when there is no meeting:meeting entry
        to fall back to, even for conference proceedings."""
        dojson_entry = _dojson_entry()
        metadata = entry._metadata(dojson_entry, _raw_dump_entry())
        assert "title" not in metadata

    def test_title_uses_first_meeting_entry_with_a_title(self, entry):
        """Test that the fallback skips meeting entries without a title and
        uses the first one that has one."""
        dojson_entry = _dojson_entry(
            custom_fields={
                "meeting:meeting": [
                    {"place": "Geneva"},
                    {"title": "Second Meeting Title"},
                ]
            }
        )
        metadata = entry._metadata(dojson_entry, _raw_dump_entry())
        assert metadata["title"] == "Second Meeting Title"
