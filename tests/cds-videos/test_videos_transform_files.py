# -*- coding: utf-8 -*-
#
# This file is part of CDS.
# Copyright (C) 2025 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos migration tests."""

from pathlib import Path

import pytest
import yaml

from cds_migrator_kit.reports.log import RDMJsonLogger
from cds_migrator_kit.videos.weblecture_migration.load.load import CDSVideosLoad
from cds_migrator_kit.videos.weblecture_migration.transform.transform import (
    CDSToVideosRecordEntry,
    CDSToVideosRecordTransform,
)
from tests.helpers import load_json


@pytest.fixture()
def entry_files_composite():
    """Get test files for composite."""
    return [{"master_path": "/master_data/2025/2"}, {"path": "/2025/2/additional.txt"}]


@pytest.fixture()
def entry_files_single():
    """Get test files for single main video."""
    return [{"master_path": "/master_data/2025/1"}, {"path": "/2025/1/1_en.vtt"}]


def test_transform_files_composite(entry_files_composite, base_app):
    """Test migration transform files."""
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
        assert len(transformed_files["subformats"]) == 1
        assert transformed_files["subformats"][0]["path"].endswith("mp4")
        assert transformed_files["subformats"][0]["quality"] == "360p"


def test_transform_files_single(entry_files_single, base_app):
    """Test migration transform files."""
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
        assert len(transformed_files["subformats"]) == 1  # 360
        assert transformed_files["subformats"][0]["path"].endswith("mp4")
        assert transformed_files["subformats"][0]["quality"] == "360p"


def test_transform_afs_files(base_app):
    """Test afs files correctly transformed."""
    with base_app.app_context():
        # Load stream config
        with open(base_app.config["CDS_MIGRATOR_KIT_STREAM_CONFIG"]) as f:
            stream_config = yaml.safe_load(f)
        files_dump_dir = stream_config["records"]["weblectures"]["transform"][
            "files_dump_dir"
        ]
        dumpdir = stream_config["records"]["weblectures"]["extract"]["dirpath"]
        log_dir = Path(stream_config["records"]["weblectures"]["log_dir"])
        log_dir.mkdir(parents=True, exist_ok=True)

        RDMJsonLogger.initialize(log_dir)
        migration_logger = RDMJsonLogger(collection="weblectures")
        migration_logger.start_log()

        # Load test data
        data = load_json(dumpdir, "lecture.json")

        # Transform
        transform = CDSToVideosRecordTransform(files_dump_dir=files_dump_dir)
        transform_entry = transform._transform(data[2])
        afs_files = transform_entry.get("record", {}).get("json", {}).get("files", [])

        # Check if afs files are transformed correctly
        assert len(afs_files) == 1
        assert afs_files[0].endswith(
            "tests/cds-videos/data/files/afs/g2/25389/AT00000495.pdf"
        )

        # Load entry
        load_entry = CDSVideosLoad(db_uri="", data_dir="", tmp_dir="")
        files = load_entry._get_files(transform_entry)

        # Check if afs files are added as additional files
        additional_files = files.get("additional_files", [])
        assert additional_files[-1].endswith(
            "tests/cds-videos/data/files/afs/g2/25389/AT00000495.pdf"
        )
