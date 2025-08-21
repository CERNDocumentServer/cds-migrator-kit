# -*- coding: utf-8 -*-
#
# Copyright (C) 2022-2025 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""InvenioRDM migration streams runner."""

from pathlib import Path

import yaml
from invenio_rdm_migrator.logging import FailedTxLogger, Logger
from invenio_rdm_migrator.streams import Stream

from cds_migrator_kit.reports.log import (
    MigrationProgressLogger,
    RecordStateLogger,
    StandardLogger,
)


# local version of the invenio-rdm-migrator Runner class
# Skipping the default invenio-rdm-migrator StateDB
# most likely it won't be needed
class Runner:
    """ETL streams runner."""

    def _read_config(self, filepath):
        """Read config from file."""
        with open(filepath) as f:
            return yaml.safe_load(f)

    def __init__(self, stream_definitions, config_filepath, dry_run, collection):
        """Constructor."""
        config = self._read_config(config_filepath)
        self.collection = collection
        self.db_uri = config.get("db_uri")
        # start parsing streams
        self.streams = []
        for definition in stream_definitions:
            if definition.name in config:
                stream_config = config.get(definition.name) or {}
                self.data_dir = Path(stream_config[collection].get("data_dir"))
                self.restricted = stream_config[collection].get("restricted", False)
                self.data_dir.mkdir(parents=True, exist_ok=True)

                self.tmp_dir = Path(stream_config[collection].get("tmp_dir"))
                self.tmp_dir.mkdir(parents=True, exist_ok=True)

                self.log_dir = Path(stream_config[collection].get("log_dir"))
                self.log_dir.mkdir(parents=True, exist_ok=True)

                Logger.initialize(self.log_dir)
                StandardLogger.initialize(self.log_dir)
                FailedTxLogger.initialize(self.log_dir)
                # get will return a None for e.g. files:

                # if loading pass source data dir, else pass tmp to dump new csv files
                data_dir = self.data_dir
                tmp_dir = self.tmp_dir / definition.name
                extract = None
                transform = None

                if definition.extract_cls:
                    extract = definition.extract_cls(
                        **stream_config[collection].get("extract", {})
                    )
                if definition.transform_cls:
                    transform = definition.transform_cls(
                        dry_run=dry_run,
                        collection=collection,
                        **stream_config[collection].get("transform", {}),
                        restricted=self.restricted,
                    )

                self.streams.append(
                    Stream(
                        definition.name,
                        extract,
                        transform,
                        definition.load_cls(
                            db_uri=self.db_uri,
                            data_dir=data_dir,
                            tmp_dir=tmp_dir,
                            dry_run=dry_run,
                            collection=collection,
                            **stream_config[collection].get("load", {}),
                        ),
                    )
                )

    def run(self):
        """Run ETL streams."""
        migration_logger = MigrationProgressLogger(collection=self.collection)
        record_state_logger = RecordStateLogger(collection=self.collection)

        migration_logger.start_log()
        record_state_logger.start_log()
        for stream in self.streams:
            try:
                stream.run(cleanup=True)
            except Exception as e:
                Logger.get_logger().exception(
                    f"Stream {stream.name} failed.", exc_info=1
                )
                migration_logger.add_log(e)
                raise e
            finally:
                migration_logger.finalise()
                record_state_logger.finalise()
