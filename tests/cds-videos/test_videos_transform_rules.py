# -*- coding: utf-8 -*-
#
# This file is part of CDS.
# Copyright (C) 2025 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos migration tests."""

import copy
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
            {"name": "CERN", "role": "Producer"},
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
            {"name": "CERN", "role": "Producer"},
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
        record_marcxml = remove_tag_from_marcxml(record_marcxml, "110")
        modified_data["record"][-1]["marcxml"] = remove_tag_from_marcxml(
            record_marcxml, "700"
        )
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
        record_marcxml = modified_data["record"][-1]["marcxml"]
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
        assert len(res["keywords"]) == 1

        # Transform record subject category and subject indicator will be added as keyword
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        assert "keywords" in metadata
        assert len(metadata["keywords"]) == 6
        keywords = [keyword["name"] for keyword in metadata["keywords"]]
        # Tag 690 subject indicators
        assert "TALK" in keywords
        assert "movingimages" in keywords
        assert "CERN" in keywords


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

        # Test case: Multiple 693 tags -> concatenate
        record_marcxml = modified_data["record"][-1]["marcxml"]
        modified_data["record"][-1]["marcxml"] = add_tag_to_marcxml(
            record_marcxml,
            "693",
            {
                "a": "accelerator_test",
                "e": "experiment_test2",
                "p": "project_test2",
                "s": "study_test",
            },
        )

        # Extract record
        res = load_and_dump_revision(modified_data)

        # Transform record
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        accelerator_experiment = metadata["accelerator_experiment"]

        # Should contain concatenated values
        assert accelerator_experiment["accelerator"] == "accelerator_test"
        assert (
            accelerator_experiment["experiment"] == "experiment_test, experiment_test2"
        )
        assert accelerator_experiment["project"] == "project_test, project_test2"
        assert accelerator_experiment["study"] == "study_test"
        assert accelerator_experiment["facility"] == "facility_test"

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

        # Add document contact
        modified_data = data[0]
        record_marcxml = modified_data["record"][-1]["marcxml"]
        modified_data["record"][-1]["marcxml"] = add_tag_to_marcxml(
            record_marcxml, "270", {"p": "Contact name"}
        )

        # Extract record
        res = load_and_dump_revision(modified_data)
        contact_contributor = next(
            c for c in res["contributors"] if c["role"] == "ContactPerson"
        )
        assert contact_contributor["name"] == "Contact name"

        # Transform record
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        contact_contributor_meta = next(
            c for c in metadata["contributors"] if c["role"] == "ContactPerson"
        )
        assert contact_contributor_meta["name"] == "Contact name"

        # Test case: Add empty 270 tag
        modified_data["record"][-1]["marcxml"] = add_tag_to_marcxml(
            record_marcxml, "270", {"p": ""}
        )
        res = load_and_dump_revision(modified_data)
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        roles = [contributor["role"] for contributor in metadata["contributors"]]
        assert "ContactPerson" not in roles


def test_transform_collaboration(dumpdir, base_app):
    """Test collaboration correctly transformed."""
    with base_app.app_context():
        # Load test data
        data = load_json(dumpdir, "lecture.json")

        # Add collaboration
        modified_data = data[0]
        record_marcxml = modified_data["record"][-1]["marcxml"]
        modified_data["record"][-1]["marcxml"] = add_tag_to_marcxml(
            record_marcxml,
            "710",
            {"g": "Collaboration name", "a": "Corporate name", "5": "Department"},
        )

        # Extract record
        res = load_and_dump_revision(modified_data)
        assert len(res["contributors"]) == 6
        assert res["contributors"][5]["name"] == "Collaboration name"
        assert res["contributors"][5]["role"] == "ResearchGroup"
        assert "_curation" in res
        assert res["_curation"]["department"] == "Department"

        # Transform record
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        contributor_roles = [
            contributor.get("role") for contributor in metadata["contributors"]
        ]
        assert "ResearchGroup" in contributor_roles
        assert "Producer" in contributor_roles
        assert "department" in metadata["_curation"]


# TODO Setup cds-videos and test it's minted correctly
def test_report_number(dumpdir, base_app):
    """Test report_number correctly transformed."""
    with base_app.app_context():
        # Load test data
        data = load_json(dumpdir, "lecture.json")
        modified_data = data[1]

        # Add report number (tag 088)
        record_marcxml = modified_data["record"][-1]["marcxml"]
        record_marcxml = add_tag_to_marcxml(
            record_marcxml, "088", {"a": "Report Number"}
        )
        modified_data["record"][-1]["marcxml"] = add_tag_to_marcxml(
            record_marcxml, "088", {"a": "Second Report Number"}
        )

        # Extract record
        res = load_and_dump_revision(modified_data)
        assert "report_number" in res
        assert len(res["report_number"]) == 2


def test_transform_system_control_number(dumpdir, base_app):
    """Test related_identifiers correctly transformed."""
    with base_app.app_context():
        # Load test data
        data = load_json(dumpdir, "lecture.json")
        modified_data = data[1]

        # Extract record
        res = load_and_dump_revision(modified_data)

        assert "related_identifiers" in res
        assert len(res["related_identifiers"]) == 8

        # Add valid date and transform
        record_marcxml = modified_data["record"][-1]["marcxml"]
        modified_data["record"][-1]["marcxml"] = add_tag_to_marcxml(
            record_marcxml, "518", {"d": "2025-05-26"}
        )
        # Remove presented at, it'll test later
        record_marcxml = modified_data["record"][-1]["marcxml"]
        modified_data["record"][-1]["marcxml"] = remove_tag_from_marcxml(
            record_marcxml, "962"
        )
        res = load_and_dump_revision(modified_data)

        # Transform record
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        assert "related_identifiers" in metadata
        indico_identifiers = [
            item
            for item in metadata["related_identifiers"]
            if item["scheme"] == "Indico"
        ]
        assert len(indico_identifiers) == 5
        # During transform `URL` added as related identifier
        assert len(metadata["related_identifiers"]) == 6


def test_transform_corporate_author(dumpdir, base_app):
    """Test corporate_author correctly transformed."""
    with base_app.app_context():
        # Load test data
        data = load_json(dumpdir, "lecture.json")

        # Extract record
        res = load_and_dump_revision(data[0])
        contributor_roles = [
            contributor.get("role") for contributor in res["contributors"]
        ]
        assert "Producer" in contributor_roles

        # Transform record
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        contributor_roles = [
            contributor.get("role") for contributor in metadata["contributors"]
        ]
        assert "Producer" in contributor_roles


def test_transform_subject_category(dumpdir, base_app):
    """Test subject_category are correctly transformed."""
    with base_app.app_context():
        # Load test data
        data = load_json(dumpdir, "lecture.json")

        modified_data = data[0]
        # Remove 490 it's also transformed as keyword
        record_marcxml = modified_data["record"][-1]["marcxml"]
        record_marcxml = remove_tag_from_marcxml(record_marcxml, "490")
        # Remove 690 it's also transformed as keyword
        modified_data["record"][-1]["marcxml"] = remove_tag_from_marcxml(
            record_marcxml, "690"
        )

        # Extract record
        res = load_and_dump_revision(modified_data)
        assert not res["keywords"]  # Should be empty

        # Transform: subject category will be keyword
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        assert "keywords" in metadata
        assert len(metadata["keywords"]) == 2


def test_additional_titles(dumpdir, base_app):
    """Test additional_titles correctly transformed."""
    with base_app.app_context():
        # Load test data
        data = load_json(dumpdir, "lecture.json")
        modified_data = data[0]

        # Add additional title (tag 246) with volume
        record_marcxml = modified_data["record"][-1]["marcxml"]
        modified_data["record"][-1]["marcxml"] = add_tag_to_marcxml(
            record_marcxml,
            "246",
            {
                "a": "Title",
                "b": "Title remainder",
                "i": "Titre français",
                "n": "Part 1",
                "p": "Volume",
            },
        )

        # Extract record
        res = load_and_dump_revision(modified_data)
        assert "additional_titles" in res
        assert len(res["additional_titles"]) == 1

        # Transform: volume will be in curation
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        assert "additional_titles" in metadata
        assert len(metadata["additional_titles"]) == 1
        additional_title = metadata["additional_titles"][0]
        assert additional_title["lang"] == "fr"
        assert additional_title["title"] == "Title : Title remainder"
        assert additional_title["type"] == "TranslatedTitle"
        volumes = metadata["_curation"]["volumes"]
        assert len(volumes) == 1
        assert volumes[0] == "Part 1 : Volume"


def test_additional_descriptions(dumpdir, base_app):
    """Test additional_descriptions correctly transformed."""
    with base_app.app_context():
        # Load test data
        data = load_json(dumpdir, "lecture.json")
        modified_data = data[0]

        # Remove 490 it's also transformed as additional description
        record_marcxml = modified_data["record"][-1]["marcxml"]
        record_marcxml = remove_tag_from_marcxml(record_marcxml, "490")

        # Add additional title (tag 246) with volume
        modified_data["record"][-1]["marcxml"] = add_tag_to_marcxml(
            record_marcxml, "590", {"a": "French Description"}
        )

        # Extract record
        res = load_and_dump_revision(modified_data)
        assert "additional_descriptions" in res
        assert len(res["additional_descriptions"]) == 1

        # Transform: volume will be in curation
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        assert "additional_descriptions" in metadata
        assert len(metadata["additional_descriptions"]) == 1
        additional_description = metadata["additional_descriptions"][0]
        assert additional_description["description"] == "French Description"
        assert additional_description["type"] == "Other"
        assert additional_description["lang"] == "fr"


def test_license(dumpdir, base_app):
    """Test license correctly transformed."""
    with base_app.app_context():
        # Load test data
        data = load_json(dumpdir, "lecture.json")
        modified_data = data[1]

        # Add valid date to not fail transform
        record_marcxml = modified_data["record"][-1]["marcxml"]
        modified_data["record"][-1]["marcxml"] = add_tag_to_marcxml(
            record_marcxml, "518", {"d": "2025-05-26"}
        )

        # Extract record
        res = load_and_dump_revision(modified_data)
        assert "license" in res
        assert res["license"][0]["license"] == "CC-BY-3.0"
        assert res["license"][0]["material"] == "Report"

        # Transform
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        assert "license" in metadata
        assert metadata["license"][0]["license"] == "CC-BY-3.0"
        assert metadata["license"][0]["material"] == "Report"


def test_copyright(dumpdir, base_app):
    """Test copyright correctly transformed."""
    with base_app.app_context():
        # Load test data
        data = load_json(dumpdir, "lecture.json")

        # Extract record
        res = load_and_dump_revision(data[0])
        assert "copyright" in res
        assert res["copyright"]["holder"] == "2016 © CERN."
        assert res["copyright"]["year"] == "2016"
        assert res["copyright"]["url"] == "http://copyright.web.cern.ch"

        # Transform
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        assert "copyright" in metadata
        assert metadata["copyright"]["holder"] == "2016 © CERN."
        assert metadata["copyright"]["year"] == "2016"
        assert metadata["copyright"]["url"] == "http://copyright.web.cern.ch"


def test_related_identifiers(dumpdir, base_app):
    """
    Test related_identifiers correctly transformed.

    962: Presented at -> related_identifiers with scheme CDS
    773: Published in -> related_identifiers with scheme URL or CDS
    787: Related document -> related_identifiers with scheme CDS
    Indico IDs and links.
    """
    with base_app.app_context():
        # Load test data
        data = load_json(dumpdir, "lecture.json")
        modified_data = data[1]

        # Add valid date to not fail transform
        record_marcxml = modified_data["record"][-1]["marcxml"]
        modified_data["record"][-1]["marcxml"] = add_tag_to_marcxml(
            record_marcxml, "518", {"d": "2025-05-28"}
        )
        # Remove system control number, it's tested already
        record_marcxml = modified_data["record"][-1]["marcxml"]
        modified_data["record"][-1]["marcxml"] = remove_tag_from_marcxml(
            record_marcxml, "035"
        )

        # Extract record
        res = load_and_dump_revision(modified_data)
        assert "related_identifiers" in res
        identifier = res["related_identifiers"][0]
        assert identifier["identifier"] == "515422"
        assert identifier["scheme"] == "CDS"
        assert identifier["relation_type"] == "IsPartOf"

        # Transform record
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        assert "related_identifiers" in metadata
        # During transform `URL` added as related identifier
        assert len(metadata["related_identifiers"]) == 2

        # Add published in
        record_marcxml = modified_data["record"][-1]["marcxml"]
        modified_data["record"][-1]["marcxml"] = add_tag_to_marcxml(
            record_marcxml, "773", {"a": "111111"}
        )
        # Add related document
        record_marcxml = modified_data["record"][-1]["marcxml"]
        modified_data["record"][-1]["marcxml"] = add_tag_to_marcxml(
            record_marcxml, "787", {"w": "489562", "i": "Conference paper"}, ind1="0"
        )
        # Add 962 with only material
        record_marcxml = modified_data["record"][-1]["marcxml"]
        modified_data["record"][-1]["marcxml"] = add_tag_to_marcxml(
            record_marcxml, "962", {"n": "number123456"}
        )

        # Extract record
        res = load_and_dump_revision(modified_data)
        assert "related_identifiers" in res
        published_in = next(
            (
                ri
                for ri in res["related_identifiers"]
                if ri.get("identifier") == "111111"
                and ri.get("scheme") == "CDS"
                and ri.get("relation_type") == "IsVariantFormOf"
            ),
            None,
        )
        assert published_in

        related_document = next(
            (
                ri
                for ri in res["related_identifiers"]
                if ri.get("identifier") == "489562"
                and ri.get("scheme") == "CDS"
                and ri.get("relation_type") == "IsVariantFormOf"
                and ri.get("resource_type") == "ConferencePaper"
            ),
            None,
        )
        assert related_document

        presented_at = next(
            (
                ri
                for ri in res["related_identifiers"]
                if ri.get("identifier")
                == "https://cds.cern.ch/search?f=111__g&p=number123456"
                and ri.get("scheme") == "URL"
            ),
            None,
        )
        assert presented_at

        # Transform record
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        assert "related_identifiers" in metadata
        # During transform `URL` added as related identifier
        assert len(metadata["related_identifiers"]) == 5


def test_legacy_indico_id_transform(dumpdir, base_app):
    """Test converting legacy Indico IDs to new format."""
    with base_app.app_context():
        # Load test data
        data = load_json(dumpdir, "lecture.json")

        # Extract record
        res = load_and_dump_revision(data[1])

        # Assertions
        assert "related_identifiers" in res
        related_ids = [item for item in res["related_identifiers"] if item]
        assert len(related_ids) == 6

        indico_ids = [
            item["identifier"] for item in related_ids if item["scheme"] == "Indico"
        ]
        for identifier in indico_ids:
            assert not identifier.startswith("a")


def test_transform_curation(dumpdir, base_app):
    """Test tags 852, 340, 595, 964, 853, 336 transformed to _curation."""
    with base_app.app_context():
        # Load test data
        data = load_json(dumpdir, "lecture.json")

        # Add `streaming video` to physical medium
        modified_data = data[1]
        record_marcxml = modified_data["record"][-1]["marcxml"]
        record_marcxml = add_tag_to_marcxml(
            record_marcxml, "340", {"a": "Streaming video"}
        )
        # Add valid date to not fail transform
        record_marcxml = add_tag_to_marcxml(record_marcxml, "518", {"d": "2025-05-26"})
        # Add tag 583
        record_marcxml = add_tag_to_marcxml(
            record_marcxml, "583", {"a": "curation", "c": "Decembre 2020"}
        )
        # Add tag 336
        modified_data["record"][-1]["marcxml"] = add_tag_to_marcxml(
            record_marcxml, "336", {"a": "Multiple videos identified"}
        )

        # Extract record
        res = load_and_dump_revision(modified_data)

        # Assertions
        assert "_curation" in res
        curation = res["_curation"]
        assert "physical_location" in curation
        assert "physical_medium" in curation
        assert "internal_note" in curation
        assert "legacy_marc_fields" in curation
        legacy_marc_fields = curation["legacy_marc_fields"]
        assert "964" in legacy_marc_fields
        assert "583" in legacy_marc_fields
        assert "336" in legacy_marc_fields

        physical_location = curation["physical_location"]
        assert len(physical_location) == 2
        assert physical_location[0] == "852__c:CERN Central Library"
        assert physical_location[1] == "852__h:Acad. Train. 392"

        physical_medium = curation["physical_medium"]
        assert "340__a:Streaming video" not in physical_medium
        assert len(physical_medium) == 1
        assert physical_medium[0] == "340__a:paper"

        internal_note = curation["internal_note"]
        assert len(internal_note) == 1
        assert internal_note[0] == "595__a:OA"

        tag_964 = curation["legacy_marc_fields"]["964"]
        assert len(tag_964) == 1
        assert tag_964[0] == "964__a:0002"

        tag_583 = curation["legacy_marc_fields"]["583"]
        assert len(tag_583) == 2
        assert tag_583[0] == "583__a:curation"
        assert tag_583[1] == "583__c:Decembre 2020"

        tag_336 = curation["legacy_marc_fields"]["336"]
        assert len(tag_336) == 1
        assert tag_336[0] == "336__a:Multiple videos identified"

        # Transform and test digitized
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        assert "_curation" in metadata
        curation = metadata["_curation"]
        assert "digitized" in curation
        assert len(curation["digitized"]) == 3
        assert "digital-memory" in curation["digitized"][0]["url"]
        assert "digital-memory" in curation["digitized"][1]["url"]
        assert "digital-memory" in curation["digitized"][2]["url"]


def test_doi(dumpdir, base_app):
    """Test DOI correctly transformed."""
    with base_app.app_context():
        # Load test data
        data = load_json(dumpdir, "lecture.json")

        modified_data = data[0]

        # Add a DOI
        record_marcxml = modified_data["record"][-1]["marcxml"]
        modified_data["record"][-1]["marcxml"] = add_tag_to_marcxml(
            record_marcxml, "024", {"a": "10.17181/CERN"}, ind1="7"
        )

        # Extract record
        res = load_and_dump_revision(modified_data)
        assert "doi" in res

        # Transform
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        assert "doi" in metadata
        assert metadata["doi"] == "10.17181/CERN"

        # Test case: add DOI with another prefix
        modified_data = data[0]

        # Add a DOI
        record_marcxml = modified_data["record"][-1]["marcxml"]
        modified_data["record"][-1]["marcxml"] = add_tag_to_marcxml(
            record_marcxml, "024", {"a": "10.19181/CERN"}, ind1="7"
        )

        # Extract record
        res = load_and_dump_revision(modified_data)

        # Transform
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        assert "doi" not in metadata
        assert "alternate_identifiers" in metadata
        identifier = metadata["alternate_identifiers"][0]
        assert identifier["value"] == "10.19181/CERN"
        assert identifier["scheme"] == "DOI"


def test_collection_tags(dumpdir, base_app):
    """Test collections correctly transformed."""
    with base_app.app_context():
        # Load test data
        data = load_json(dumpdir, "lecture.json")

        # Extract record
        res = load_and_dump_revision(data[0])
        assert "collections" in res

        # Transform
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)

        assert "collections" in metadata
        tags = metadata["collections"]
        assert "Indico" not in tags
        assert "Lectures::E-learning modules" in tags
        assert "Lectures" in tags


def test_additional_languages(dumpdir, base_app):
    """Test additional languages correctly transformed."""
    with base_app.app_context():
        # Load test data
        data = load_json(dumpdir, "lecture.json")

        modified_data = data[0]

        # Remove current language
        record_marcxml = modified_data["record"][-1]["marcxml"]
        modified_data["record"][-1]["marcxml"] = remove_tag_from_marcxml(
            record_marcxml, "041"
        )
        # Add multiple language
        record_marcxml = modified_data["record"][-1]["marcxml"]
        modified_data["record"][-1]["marcxml"] = add_tag_to_marcxml(
            record_marcxml, "041", {"a": ["eng", "fre"]}
        )

        # Extract record
        res = load_and_dump_revision(modified_data)
        assert "language" in res
        assert "additional_languages" in res

        # Transform
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)

        assert "language" in metadata
        assert metadata["language"] == "en"
        assert "additional_languages" in metadata
        assert len(metadata["additional_languages"]) == 1
        assert metadata["additional_languages"][0] == "fr"


def test_producer(dumpdir, base_app):
    """Test imprint correctly transformed."""
    with base_app.app_context():
        # Load test data
        data = load_json(dumpdir, "lecture.json")

        modified_data = data[0]

        # Add a imprint with a different value than CERN Geneva
        record_marcxml = modified_data["record"][-1]["marcxml"]
        modified_data["record"][-1]["marcxml"] = add_tag_to_marcxml(
            record_marcxml, "269", {"a": "Paris", "b": "Lawrence Berkeley Nat. Lab."}
        )

        # Extract record
        res = load_and_dump_revision(modified_data)

        # Transform
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        contributors = metadata["contributors"]
        producers = [item for item in contributors if item["role"] == "Producer"]
        assert {
            "name": "Paris Lawrence Berkeley Nat. Lab.",
            "role": "Producer",
        } in producers


def test_series(dumpdir, base_app):
    """Test series correctly transformed."""
    with base_app.app_context():
        # Load test data
        data = load_json(dumpdir, "lecture.json")
        modified_data = data[1]

        # Add valid date to not fail transform
        record_marcxml = modified_data["record"][-1]["marcxml"]
        modified_data["record"][-1]["marcxml"] = add_tag_to_marcxml(
            record_marcxml, "518", {"d": "2025-05-26"}
        )

        # Extract record
        res = load_and_dump_revision(modified_data)

        # Transform
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)

        additional_descriptions = metadata["additional_descriptions"]
        assert {
            "description": "CERN Academic Training Lecture,392",
            "type": "SeriesInformation",
        } in additional_descriptions
        assert {
            "description": "Regular Lecture Programme",
            "type": "SeriesInformation",
        } in additional_descriptions

        keywords = metadata["keywords"]
        assert {"name": "CERN Academic Training Lecture"} in keywords
        assert {"name": "Regular Lecture Programme"} in keywords

        # Test case: add CAS as Series
        record_marcxml = modified_data["record"][-1]["marcxml"]
        modified_data["record"][-1]["marcxml"] = add_tag_to_marcxml(
            record_marcxml, "490", {"a": "CERN Accelerator School"}
        )
        res = load_and_dump_revision(modified_data)
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        collections = metadata["collections"]
        assert "Lectures::CERN Accelerator School" in collections


def test_restrictions(dumpdir, base_app):
    """Test restrictions correctly transformed."""
    with base_app.app_context():
        # Load test data
        data = load_json(dumpdir, "lecture.json")
        modified_data = copy.deepcopy(data[0])

        # Add dummy group and email restriction
        record_marcxml = modified_data["record"][-1]["marcxml"]
        record_marcxml = add_tag_to_marcxml(
            record_marcxml,
            tag="506",
            subfields={
                "a": "Restricted",
                "d": [
                    "dummy-group1 [CERN]",
                    "dummy-group2 [CERN]",
                    "dummy-group3 [CERN]",
                ],
                "f": "group",
                "2": "CDS Invenio",
                "5": "SzGeCERN",
            },
            ind1="1",
        )
        record_marcxml = add_tag_to_marcxml(
            record_marcxml,
            tag="506",
            subfields={
                "a": "Restricted",
                "d": [
                    "dummyemail1@example.com",
                    "dummyemail2@example.com",
                    "dummyemail3@example.com",
                ],
                "f": "email",
                "2": "CDS Invenio",
                "5": "SzGeCERN",
            },
            ind1="1",
        )
        modified_data["record"][-1]["marcxml"] = record_marcxml

        # Extract and transform record
        res = load_and_dump_revision(modified_data)
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)

        # Check `_access["read"]``
        assert "_access" in metadata
        assert "read" in metadata["_access"]
        assert sorted(metadata["_access"]["read"]) == sorted(
            [
                "dummy-group1@cern.ch",
                "dummy-group2@cern.ch",
                "dummy-group3@cern.ch",
                "dummyemail1@example.com",
                "dummyemail2@example.com",
                "dummyemail3@example.com",
            ]
        )
        # Check  `_access["update"]`
        assert (
            base_app.config["WEBLECTURES_MIGRATION_SYSTEM_USER"]
            in metadata["_access"]["update"]
        )

        # Check "Lectures::Restricted General Talks" is added to collections
        assert "collections" in metadata
        assert "Lectures::Restricted General Talks" in metadata["collections"]

        # Test case: use cern-accounts due to missing restrictions
        modified_data = copy.deepcopy(data[0])

        # Add restriction without groups or emails
        record_marcxml = modified_data["record"][-1]["marcxml"]
        modified_data["record"][-1]["marcxml"] = add_tag_to_marcxml(
            record_marcxml,
            tag="506",
            subfields={
                "a": "Restricted",
            },
            ind1="1",
        )
        # Extract and transform record
        res = load_and_dump_revision(modified_data)
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)

        # Check `_access["read"]``
        assert "_access" in metadata
        assert "read" in metadata["_access"]
        assert metadata["_access"]["read"] == ["cern-accounts@cern.ch"]


def test_transform_affiliation(dumpdir, base_app):
    """Test affiliation correctly transformed."""
    with base_app.app_context():
        data = load_json(dumpdir, "lecture.json")

        modified_data = data[0]
        # Add affiliation
        record_marcxml = modified_data["record"][-1]["marcxml"]
        modified_data["record"][-1]["marcxml"] = add_tag_to_marcxml(
            record_marcxml, "901", {"u": "Affiliation"}
        )

        # Extract record
        res = load_and_dump_revision(modified_data)
        # Transform and expect a Producer contributor from affiliation
        record_entry = CDSToVideosRecordEntry()
        metadata = record_entry._metadata(res)
        contributors = metadata["contributors"]
        assert {"name": "Affiliation", "role": "Producer"} in contributors
