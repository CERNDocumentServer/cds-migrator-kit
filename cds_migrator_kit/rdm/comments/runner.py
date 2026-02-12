# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-Migrator-Kit is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""CDS-Migrator-Kit comments runner module."""

from pathlib import Path

from invenio_rdm_migrator.streams import Stream

from cds_migrator_kit.rdm.comments.log import CommentsLogger
from cds_migrator_kit.rdm.users.api import CDSMigrationUserAPI


class CommentsRunner:
    """ETL streams runner."""

    def __init__(self, stream_definition, filepath, dirpath, log_dir, dry_run):
        """Constructor."""
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        CommentsLogger.initialize(self.log_dir)

        self.stream = Stream(
            stream_definition.name,
            extract=stream_definition.extract_cls(filepath),
            transform=stream_definition.transform_cls(),
            load=stream_definition.load_cls(dirpath=dirpath, dry_run=dry_run),
        )

    def run(self):
        """Run comments ETL stream."""
        try:
            self.stream.run()
        except Exception as e:
            CommentsLogger.get_logger().exception(
                f"Stream {self.stream.name} failed.", exc_info=1
            )


class CommenterRunner:
    """ETL streams runner dedicated to pre-create commenters accounts."""

    def __init__(
        self, stream_definition, filepath, missing_users_dir, log_dir, dry_run
    ):
        """Constructor."""
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        CommentsLogger.initialize(self.log_dir)

        self.stream = Stream(
            stream_definition.name,
            extract=stream_definition.extract_cls(filepath),
            transform=stream_definition.transform_cls(),
            load=stream_definition.load_cls(
                dry_run=dry_run,
                missing_users_dir=missing_users_dir,
                logger=CommentsLogger.get_logger(),
                user_api_cls=CDSMigrationUserAPI,
            ),
        )

    def run(self):
        """Run commenters ETL stream."""
        self.stream.run()
