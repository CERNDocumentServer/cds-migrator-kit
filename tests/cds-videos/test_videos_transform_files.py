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


@pytest.fixture()
def entry_files_composite():
    """Get test files for composite."""
    return [{"master_path": "/master_data/2025/2"}, {"path": "/2025/2/additional.txt"}]


@pytest.fixture()
def entry_files_single():
    """Get test files for single main video."""
    return [{"master_path": "/master_data/2025/1"}, {"path": "/2025/1/1_en.vtt"}]


def test_transform_files_composite(entry_files_composite, base_app):
    """Test migration tramsform files."""
    with base_app.app_context():
        # Load test data
        entry = {}
        # Fake entries to test file transform
        entry["files"] = entry_files_composite
        entry["legacy_recid"] = "2"

        record_entry = CDSToVideosRecordEntry()
        transformed_files = record_entry._media_files(entry)

        assert transformed_files["master_video"].endswith("composite-1080p-quality.mp4")

        assert len(transformed_files["additional_files"]) == 3
        assert "presenter" in transformed_files["additional_files"][0]
        assert "presentation" in transformed_files["additional_files"][1]

        assert len(transformed_files["frames"]) == 0
        assert len(transformed_files["subformats"]) == 2
        assert transformed_files["subformats"][1]["path"].endswith("mp4")
        assert transformed_files["subformats"][1]["quality"] == "360p"


def test_transform_files_single(entry_files_single, base_app):
    """Test migration tramsform files."""
    with base_app.app_context():
        # Load test data
        entry = {}
        # Fake entries to test file transform
        entry["files"] = entry_files_single
        entry["legacy_recid"] = "1"

        record_entry = CDSToVideosRecordEntry()
        transformed_files = record_entry._media_files(entry)

        assert transformed_files["master_video"]
        assert len(transformed_files["additional_files"]) == 1
        assert len(transformed_files["frames"]) == 10
        assert len(transformed_files["subformats"]) == 2  # master, 360
        assert transformed_files["subformats"][1]["path"].endswith("mp4")
        assert transformed_files["subformats"][1]["quality"] == "360p"
