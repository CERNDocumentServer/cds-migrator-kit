# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""cds-migrator-kit migration streams runner."""

import logging
from pathlib import Path

import yaml
from invenio_rdm_migrator.streams import Stream

from cds_migrator_kit.videos.weblecture_migration.logger import SubmitterLogger
from .api import CDSVideosMigrationUserAPI
from .transform import users_migrator_marc21


class VideosSubmitterRunner:
    """ETL Runner dedicated to create missing submitter accounts."""

    def _read_config(self, filepath):
        """Read config from file."""
        with open(filepath) as f:
            return yaml.safe_load(f)

    def __init__(self, stream_definition, config_filepath, dry_run):
        """Constructor."""
        config = self._read_config(config_filepath)
        stream_config = config.get(stream_definition.name) or {}
        self.log_dir = Path(stream_config.get("log_dir"))
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self.data_dir = Path(stream_config.get("data_dir"))
        self.data_dir.mkdir(parents=True, exist_ok=True)

        SubmitterLogger.initialize(self.log_dir)

        missing_users_dir = self.data_dir
        self.stream = Stream(
            stream_definition.name,
            extract=stream_definition.extract_cls(**stream_config.get("extract", {})),
            transform=stream_definition.transform_cls(
                dojson_model=users_migrator_marc21
            ),
            load=stream_definition.load_cls(
                dry_run=dry_run,
                missing_users_dir=missing_users_dir,
                logger=logging.getLogger("submitters"),
                user_api_cls=CDSVideosMigrationUserAPI,
            ),
        )

    def run(self):
        """Run stream."""
        self.stream.run()
