# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# Invenio-RDM-Migrator is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""InvenioRDM migration streams runner."""

from pathlib import Path

import yaml
from invenio_rdm_migrator.streams import Stream

from cds_migrator_kit.rdm.affiliations.log import AffiliationsLogger


class PeopleAuthorityRunner:
    """ETL streams runner."""

    def __init__(self, stream_definition, filepath, log_dir, dry_run):
        """Constructor."""
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # PeopleLogger.initialize(self.log_dir)

        self.stream = Stream(
            stream_definition.name,
            extract=stream_definition.extract_cls(filepath),
            transform=stream_definition.transform_cls(),
            load=stream_definition.load_cls(dry_run=dry_run),
        )

    def run(self):
        """Run Statistics ETL stream."""
        self.stream.run()
            # AffiliationsLogger.get_logger().exception(
            #     f"Stream {self.stream.name} failed.", exc_info=1
            # )



class SubmitterRunner:
    """ETL Runner dedicated to create missing submitter accounts."""

    def _read_config(self, filepath):
        """Read config from file."""
        with open(filepath) as f:
            return yaml.safe_load(f)

    def __init__(self, stream_definition, dirpath, missing_users_dir, log_dir, dry_run):
        """Constructor."""
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.stream = Stream(
            stream_definition.name,
            extract=stream_definition.extract_cls(dirpath),
            transform=stream_definition.transform_cls(),
            load=stream_definition.load_cls(dry_run=dry_run, missing_users_dir=missing_users_dir),
        )

    def run(self):
        """Run Statistics ETL stream."""
        self.stream.run()
