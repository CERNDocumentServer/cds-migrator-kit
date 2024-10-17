# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# Invenio-RDM-Migrator is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""InvenioRDM migration streams runner."""

from pathlib import Path

from invenio_rdm_migrator.streams import Stream

from cds_migrator_kit.rdm.migration.stats.log import StatsLogger


# local version of the invenio-rdm-migrator Runner class
# Skipping the default invenio-rdm-migrator StateDB
# most likely it won't be needed
class RecordStatsRunner:
    """ETL streams runner."""

    def __init__(self, stream_definition, filepath, config, log_dir, dry_run):
        """Constructor."""
        self.config = config

        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        StatsLogger.initialize(self.log_dir)

        self.stream = Stream(
            stream_definition.name,
            extract=stream_definition.extract_cls(filepath),
            transform=stream_definition.transform_cls(),
            load=stream_definition.load_cls(dry_run=dry_run, config=config),
        )

    def run(self):
        """Run Statistics ETL stream."""
        try:
            self.stream.run()
        except Exception as e:
            StatsLogger.get_logger().exception(
                f"Stream {self.stream.name} failed.", exc_info=1
            )
