# -*- coding: utf-8 -*-
#
# This file is part of CDS.
# Copyright (C) 2025 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos migration tests."""

from os.path import dirname, join

import pytest

from cds_migrator_kit.errors import (
    MissingRequiredField,
    UnexpectedValue,
)
from cds_migrator_kit.transform.dumper import CDSRecordDump
from cds_migrator_kit.videos.weblecture_migration.transform import (
    videos_migrator_marc21,
)
from cds_migrator_kit.videos.weblecture_migration.transform.transform import (
    CDSToVideosRecordEntry,
)
from tests.helpers import add_tag_to_marcxml, load_json, remove_tag_from_marcxml


def load_and_dump_revision(entry_data, migrator_model=videos_migrator_marc21):
    """Helper function to dump and apply rules"""
    dump = CDSRecordDump(data=entry_data, dojson_model=migrator_model)
    dump.prepare_revisions()
    _, res = dump.latest_revision
    return res


@pytest.fixture()
def dumpdir():
    """Get data directory."""
    return join(dirname(__file__), "data/dump")


def test_transform_rules_reqired_metadata(dumpdir, base_app):
    """Test migration rules."""
    with base_app.app_context():
        data = load_json(dumpdir, "lecture.json")
        res = load_and_dump_revision(data[0])

        assert res["legacy_recid"] == 2233152
        assert res["recid"] == "2233152"
        assert res["language"] == "en"
        assert res["contributors"] == [
            {
                "name": "Brodski, Michael",
                "role": "Speaker",
                "affiliations": ["Rheinisch-Westfaelische Tech. Hoch. (DE)"],
            },
            {"name": "Dupont, Niels", "role": "Speaker", "affiliations": ["CERN"]},
            {"name": "Esposito, William", "role": "Speaker", "affiliations": ["CERN"]},
        ]
        assert res["title"] == {
            "title": "Glimos Instructions for CMS Underground Guiding - in english"
        }
        assert "2016-10-24" in res["date"]
        assert res["description"].startswith(
            "<!--HTML--><p>In this <strong>presentation in english</strong>"
        )


def test_transform_required_metadata(dumpdir, base_app):
    """Test migration transform."""
    with base_app.app_context():
        data = load_json(dumpdir, "lecture.json")
        res = load_and_dump_revision(data[0])

        # Transform record
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        assert metadata["title"] == {
            "title": "Glimos Instructions for CMS Underground Guiding - in english"
        }
        assert metadata["date"] == "2016-10-24"
        # It should be same with the title
        assert metadata["description"].startswith(
            "<!--HTML--><p>In this <strong>presentation in english</strong>"
        )
        assert metadata["contributors"] == [
            {
                "name": "Brodski, Michael",
                "role": "Speaker",
                "affiliations": ["Rheinisch-Westfaelische Tech. Hoch. (DE)"],
            },
            {"name": "Dupont, Niels", "role": "Speaker", "affiliations": ["CERN"]},
            {"name": "Esposito, William", "role": "Speaker", "affiliations": ["CERN"]},
        ]
        assert metadata["language"] == "en"


def test_transform_description(dumpdir, base_app):
    """Test that the description field `520` is correctly transformed."""
    with base_app.app_context():
        # Load test data
        data = load_json(dumpdir, "lecture.json")

        # Remove the 520 tag (description) from MARCXML
        modified_data = data[0]
        record_marcxml = modified_data["record"][-1]["marcxml"]
        modified_data["record"][-1]["marcxml"] = remove_tag_from_marcxml(
            record_marcxml, "520"
        )

        res = load_and_dump_revision(modified_data)

        # Ensure json_converted_record don't have the description
        assert "description" not in res

        # Transform record
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)

        # Ensure description exists and matches the title
        assert metadata["description"] == metadata["title"]["title"]


def test_transform_date(dumpdir, base_app):
    """Test that the date field is correctly transformed."""
    with base_app.app_context():
        # Load test data
        data = load_json(dumpdir, "lecture.json")

        # Test case: Fail due to multiple dates
        modified_data = data[0]
        record_marcxml = modified_data["record"][-1]["marcxml"]
        modified_data["record"][-1]["marcxml"] = add_tag_to_marcxml(
            record_marcxml, "518", {"d": "2025-02-06"}
        )
        res = load_and_dump_revision(modified_data)

        # Transform record
        record_entry = CDSToVideosRecordEntry()
        with pytest.raises(UnexpectedValue):
            record_entry._metadata(res)

        # Test case: Fail due to missing dates
        record_marcxml = modified_data["record"][-1]["marcxml"]
        record_marcxml = remove_tag_from_marcxml(record_marcxml, "518")
        modified_data["record"][-1]["marcxml"] = remove_tag_from_marcxml(
            record_marcxml, "269"
        )

        res = load_and_dump_revision(modified_data)

        # Transform record
        with pytest.raises(MissingRequiredField):
            record_entry._metadata(res)


def test_transform_contributor(dumpdir, base_app):
    """Test that the date field is correctly transformed."""
    with base_app.app_context():
        # Load test data
        data = load_json(dumpdir, "lecture.json")

        # Test case: Return event contributor due to missing contributor
        modified_data = data[0]
        record_marcxml = modified_data["record"][-1]["marcxml"]
        record_marcxml = remove_tag_from_marcxml(record_marcxml, "700")
        res = load_and_dump_revision(modified_data)

        # Transform record
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        assert metadata["contributors"] == [
            {
                "name": "Brodski, Michael",
                "role": "Speaker",
                "affiliations": ["Rheinisch-Westfaelische Tech. Hoch. (DE)"],
            },
            {"name": "Dupont, Niels", "role": "Speaker", "affiliations": ["CERN"]},
            {"name": "Esposito, William", "role": "Speaker", "affiliations": ["CERN"]},
        ]

        # Test case: Return "Unknown" due to missing contributor
        modified_data["record"][-1]["marcxml"] = remove_tag_from_marcxml(
            record_marcxml, "906"
        )
        res = load_and_dump_revision(modified_data)

        # Transform record
        metadata = record_entry._metadata(res)
        assert metadata["contributors"] == [{"name": "Unknown, Unknown"}]


def test_transform_digitized(dumpdir, base_app):
    """Test digitized field is correctly transformed."""
    with base_app.app_context():
        # Load test data
        data = load_json(dumpdir, "lecture.json")

        # Get digitized record and apply rules
        entry_data = data[1]
        res = load_and_dump_revision(entry_data)

        digitized = [
            item["digitized"] for item in res["url_files"] if "digitized" in item
        ]

        # Check length
        assert len(digitized) == 3, f"Expected 3 digitized items, got {len(digitized)}"

        # Check all URLs contain "digital-memory"
        for item in digitized:
            assert (
                "digital-memory" in item["url"]
            ), f"URL {item['url']} does not contain 'digital-memory'"

        # Transform record it should fail (no valid date, it has date range)
        record_entry = CDSToVideosRecordEntry()
        with pytest.raises(MissingRequiredField):
            record_entry._metadata(res)


def test_transform_files(dumpdir, base_app):
    """Test files field is correctly transformed."""
    with base_app.app_context():
        # Load test data
        data = load_json(dumpdir, "lecture.json")

        # Get record and apply rules
        entry_data = data[1]
        res = load_and_dump_revision(entry_data)

        # Test master paths
        master_paths = [
            item["master_path"] for item in res["files"] if "master_path" in item
        ]
        assert (
            len(master_paths) == 3
        ), f"Expected 3 master_path items, got {len(master_paths)}"
        for path in master_paths:
            assert (
                "/mnt/master_share" in path
            ), f"Path {path} does not contain '/mnt/master_share'"

        # Test file paths (excluding URLs)
        file_paths = [
            item["path"]
            for item in res["files"]
            if "path" in item and "url" not in item
        ]
        assert (
            len(file_paths) == 6
        ), f"Expected 6 only path items, got {len(file_paths)}"
        for path in file_paths:
            assert path.startswith("/"), f"Path {path} does not start with '/'"

        # Test URL files
        url_files = [item for item in res["files"] if "url" in item]
        assert len(url_files) == 6, f"Expected 6 URL file items, got {len(url_files)}"
        for url_file in url_files:
            assert "url" in url_file, f"Missing 'url' key in item: {url_file}"
            assert "path" in url_file, f"Missing 'path' key in item: {url_file}"
            assert (
                "lecturemedia" in url_file["url"]
            ), f"URL {url_file['url']} does not contain 'lecturemedia'"


def test_transform_internal_note(dumpdir, base_app):
    """Test digitized field is correctly transformed."""
    with base_app.app_context():
        # Load test data
        data = load_json(dumpdir, "lecture.json")

        # Get record and apply rules
        entry_data = data[1]
        res = load_and_dump_revision(entry_data)

        # Record has one internal note
        assert "internal_notes" in res
        notes = [item for item in res["internal_notes"]]
        assert notes
        assert "date" not in notes[0]  # note includes date but it's not valid

        # Transform record it should fail (no valid date, it has date range)
        record_entry = CDSToVideosRecordEntry()
        with pytest.raises(MissingRequiredField):
            record_entry._metadata(res)

        # Test case: Add internal note which has a valid date to record
        modified_data = data[1]
        # Remove the current internal note
        record_marcxml = modified_data["record"][-1]["marcxml"]
        record_marcxml = remove_tag_from_marcxml(record_marcxml, "500")
        # Add new internal note with a valid date
        modified_data["record"][-1]["marcxml"] = add_tag_to_marcxml(
            record_marcxml, "500", {"a": "Note, 16 Feb 2001"}
        )
        res = load_and_dump_revision(modified_data)

        # Record has one internal note
        assert "internal_notes" in res
        notes = [item for item in res["internal_notes"]]
        assert notes
        assert "date" in notes[0]  # note has a valid date

        # Transform record without failure (it has a valid date)
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        assert "date" in metadata
        assert "2001-02-16" == metadata["date"]
