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


def test_transform_note(dumpdir, base_app):
    """Test notes are correctly transformed."""
    with base_app.app_context():
        # Load test data
        data = load_json(dumpdir, "lecture.json")

        # Get record and apply rules
        entry_data = data[1]
        res = load_and_dump_revision(entry_data)

        # Record has one internal note
        assert "notes" in res
        notes = [item for item in res["notes"]]
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
        assert "notes" in res
        notes = [item for item in res["notes"]]
        assert notes
        assert "date" in notes[0]  # note has a valid date

        # Transform record without failure (it has a valid date)
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        assert "date" in metadata
        assert "2001-02-16" == metadata["date"]
        assert "note" in metadata


def test_transform_keywords(dumpdir, base_app):
    """Test keywords are correctly transformed."""
    with base_app.app_context():
        # Load test data
        data = load_json(dumpdir, "lecture.json")

        # Add keyword tag
        modified_data = data[0]
        record_marcxml = modified_data["record"][-1]["marcxml"]
        modified_data["record"][-1]["marcxml"] = add_tag_to_marcxml(
            record_marcxml, "653", {"a": "keyword_test"}, ind1="1"
        )

        # Extract record
        res = load_and_dump_revision(modified_data)

        # Transform record
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        assert "keywords" in metadata
        assert len(metadata["keywords"]) == 1
        assert metadata["keywords"][0]["name"] == "keyword_test"


def test_transform_accelerator_experiment(dumpdir, base_app):
    """Test accelerator_experiment field is correctly transformed."""
    with base_app.app_context():
        # Load test data
        data = load_json(dumpdir, "lecture.json")
        modified_data = data[0]

        # Test case: No accelerator_experiment
        res = load_and_dump_revision(modified_data)
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        assert "accelerator_experiment" not in metadata

        # Test case: Add accelerator_experiment tag
        record_marcxml = modified_data["record"][-1]["marcxml"]
        modified_data["record"][-1]["marcxml"] = add_tag_to_marcxml(
            record_marcxml,
            "693",
            {
                "a": "accelerator_test",
                "p": "project_test",
                "e": "experiment_test",
                "s": "study_test",
                "f": "facility_test",
            },
        )

        # Extract record
        res = load_and_dump_revision(modified_data)

        # Transform record
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        assert "accelerator_experiment" in metadata
        accelerator_experiment = metadata["accelerator_experiment"]
        assert len(accelerator_experiment) == 5
        assert accelerator_experiment["study"] == "study_test"
        assert accelerator_experiment["accelerator"] == "accelerator_test"
        assert accelerator_experiment["project"] == "project_test"
        assert accelerator_experiment["experiment"] == "experiment_test"
        assert accelerator_experiment["facility"] == "facility_test"

        # Test case: Fail due to multiple 693(accelerator_experiment) tags
        record_marcxml = modified_data["record"][-1]["marcxml"]
        modified_data["record"][-1]["marcxml"] = add_tag_to_marcxml(
            record_marcxml, "693", {"a": "accelerator"}
        )

        # Extract record
        res = load_and_dump_revision(modified_data)
        # Transform record
        record_entry = CDSToVideosRecordEntry()
        with pytest.raises(UnexpectedValue):
            record_entry._metadata(res)

        # Test case: remove the 693 tags and add another
        record_marcxml = modified_data["record"][-1]["marcxml"]
        record_marcxml = remove_tag_from_marcxml(record_marcxml, "693")
        modified_data["record"][-1]["marcxml"] = add_tag_to_marcxml(
            record_marcxml,
            "693",
            {
                "p": "project_test",
                "e": "experiment_test",
            },
        )
        # Extract and transform record
        res = load_and_dump_revision(modified_data)
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        assert "accelerator_experiment" in metadata
        accelerator_experiment = metadata["accelerator_experiment"]
        assert len(accelerator_experiment) == 2
        assert "study" not in accelerator_experiment
        assert "accelerator" not in accelerator_experiment
        assert "facility" not in accelerator_experiment
        assert accelerator_experiment["project"] == "project_test"
        assert accelerator_experiment["experiment"] == "experiment_test"


def test_transform_location(dumpdir, base_app):
    """Test location are correctly transformed."""
    with base_app.app_context():
        # Load test data
        data = load_json(dumpdir, "lecture.json")

        # Get record and apply rules
        entry_data = data[0]
        res = load_and_dump_revision(entry_data)

        # Record has indico_information
        assert "indico_information" in res
        indico_information = res["indico_information"]
        assert indico_information["title"]
        assert indico_information["event_id"]
        assert indico_information["location"]
        assert indico_information["start_date"]

        # Transform record
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        assert "location" in metadata
        assert metadata["location"] == "CERN - 513-R-068"

        # Test case: Add location to tag 518
        modified_data = data[0]
        record_marcxml = modified_data["record"][-1]["marcxml"]
        # Add new location
        modified_data["record"][-1]["marcxml"] = add_tag_to_marcxml(
            record_marcxml, "518", {"r": "New location"}
        )
        res = load_and_dump_revision(modified_data)

        # Check 518 location is there as lecture_infos
        assert "lecture_infos" in res
        assert "location" in res["lecture_infos"][0]

        # Transform record
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        assert "location" in metadata
        # Check the location comes from indico_information
        assert metadata["location"] == "CERN - 513-R-068"

        # Test case: remove 111 tag (indico_information)
        record_marcxml = modified_data["record"][-1]["marcxml"]
        modified_data["record"][-1]["marcxml"] = remove_tag_from_marcxml(
            record_marcxml, "111"
        )
        res = load_and_dump_revision(modified_data)
        assert "indico_information" not in res

        # Transform record
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        assert "location" in metadata
        # Check location is comes from lecture_infos
        assert metadata["location"] == "New location"


def test_transform_document_contact(dumpdir, base_app):
    """Test document contact correctly transformed."""
    with base_app.app_context():
        # Load test data
        data = load_json(dumpdir, "lecture.json")

        # Add keyword tag
        modified_data = data[0]
        record_marcxml = modified_data["record"][-1]["marcxml"]
        modified_data["record"][-1]["marcxml"] = add_tag_to_marcxml(
            record_marcxml, "270", {"p": "Contact name"}
        )

        # Extract record
        res = load_and_dump_revision(modified_data)
        assert res["contributors"][3]["name"] == "Contact name"
        assert res["contributors"][3]["role"] == "ContactPerson"

        # Transform record
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        assert metadata["contributors"][3]["name"] == "Contact name"
        assert metadata["contributors"][3]["role"] == "ContactPerson"

        # Test case: Add empty 270 tag
        modified_data["record"][-1]["marcxml"] = add_tag_to_marcxml(
            record_marcxml, "270", {"p": ""}
        )
        res = load_and_dump_revision(modified_data)
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        roles = [contributor["role"] for contributor in metadata["contributors"]]
        assert "ContactPerson" not in roles
