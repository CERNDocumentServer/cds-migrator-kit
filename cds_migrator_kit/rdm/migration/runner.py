# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# Invenio-RDM-Migrator is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""InvenioRDM migration streams runner."""

from pathlib import Path

import yaml
from invenio_rdm_migrator.logging import FailedTxLogger, Logger
from invenio_rdm_migrator.streams import Stream

from cds_migrator_kit.records.log import RDMJsonLogger


# local version of the invenio-rdm-migrator Runner class
# Skipping the default invenio-rdm-migrator StateDB
# most likely it won't be needed
class Runner:
    """ETL streams runner."""

    def _read_config(self, filepath):
        """Read config from file."""
        with open(filepath) as f:
            return yaml.safe_load(f)

    def __init__(self, stream_definitions, config_filepath, dry_run):
        """Constructor."""
        config = self._read_config(config_filepath)

        self.data_dir = Path(config.get("data_dir"))
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.tmp_dir = Path(config.get("tmp_dir"))
        self.tmp_dir.mkdir(parents=True, exist_ok=True)

        self.state_dir = Path(config.get("state_dir"))
        self.state_dir.mkdir(parents=True, exist_ok=True)

        self.log_dir = Path(config.get("log_dir"))
        self.log_dir.mkdir(parents=True, exist_ok=True)

        Logger.initialize(self.log_dir)
        RDMJsonLogger.initialize(self.log_dir)
        FailedTxLogger.initialize(self.log_dir)

        self.db_uri = config.get("db_uri")
        # self.state = StateDB(
        #     db_dir=self.state_dir, validators={"parents": ParentModelValidator}
        # )
        # STATE.initialized_state(
        #     self.state,
        #     cache=config.get("state_cache", True),
        #     search_cache=config.get("state_search_cache", True),
        # )
        # set up secret keys
        # for key in ("old_secret_key", "new_secret_key"):
        #     stored_value = STATE.VALUES.get(key)
        #     if stored_value:
        #         STATE.VALUES.update(key, {"value": bytes(config.get(key), "utf-8")})
        #     else:
        #         STATE.VALUES.add(key, {"value": bytes(config.get(key), "utf-8")})

        # start parsing streams
        self.streams = []
        for definition in stream_definitions:
            if definition.name in config:
                # get will return a None for e.g. files:
                stream_config = config.get(definition.name) or {}
                # merge state objects from stream definition config
                existing_data = stream_config.get("existing_data", {})

                # if loading pass source data dir, else pass tmp to dump new csv files
                data_dir = self.data_dir
                tmp_dir = self.tmp_dir / definition.name
                extract = None
                transform = None
                if not existing_data:
                    if definition.extract_cls:
                        extract = definition.extract_cls(
                            **stream_config.get("extract", {})
                        )
                    if definition.transform_cls:
                        transform = definition.transform_cls(
                            **stream_config.get("transform", {})
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
                            existing_data=existing_data,
                            dry_run=dry_run,
                            **stream_config.get("load", {}),
                        ),
                    )
                )

    def run(self):
        """Run ETL streams."""
        migration_logger = RDMJsonLogger()
        for stream in self.streams:
            try:
                stream.run()
                # on successful stream run, persist state
                # STATE.flush_cache()
                # self.state.save(filename=f"{stream.name}.db")

                migration_logger.save()
            except Exception as e:
                Logger.get_logger().exception(
                    f"Stream {stream.name} failed.", exc_info=1
                )
                migration_logger.save()
                continue
