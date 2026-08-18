# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-Migrator-Kit is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""CDS-Migrator-Kit comments runner module."""

import os
from pathlib import Path

import yaml
from invenio_rdm_migrator.streams import Stream

from cds_migrator_kit.rdm.comments.log import CommentsLogger
from cds_migrator_kit.rdm.users.api import CDSMigrationUserAPI


def read_config(filepath):
    """Read config from file."""
    with open(filepath) as f:
        return yaml.safe_load(f)


class CommentsRunner:
    """ETL streams runner."""

    def __init__(
        self, stream_definition, config_filepath, log_dir, collection, dry_run
    ):
        """Constructor."""
        config = read_config(config_filepath)
        collection_config = config["comments"][collection]

        self.log_dir = Path(log_dir)

        self.logger = CommentsLogger(self.log_dir, collection)
        collection_dirpath=collection_config["dir_path"]
        comments_metadata_filepath = os.path.join(collection_dirpath, "comments_metadata.json")

        self.stream = Stream(
            stream_definition.name,
            extract=stream_definition.extract_cls(comments_metadata_filepath),
            transform=stream_definition.transform_cls(),
            load=stream_definition.load_cls(
                dirpath=collection_dirpath,
                dry_run=dry_run,
                logger=self.logger,
                collection=collection,
                reviewers=collection_config.get("reviewers", []),
            ),
        )

    def run(self):
        """Run comments ETL stream."""
        try:
            self.stream.run()
        except Exception as e:
            self.logger.get_logger().exception(
                f"Stream {self.stream.name} failed.", exc_info=1
            )


class CommenterRunner:
    """ETL streams runner dedicated to pre-create commenters accounts."""

    def __init__(
        self, stream_definition, config_filepath, log_dir, collection, dry_run
    ):
        """Constructor."""
        config = read_config(config_filepath)
        collection_config = config["comments"][collection]
        dirpath=collection_config["dir_path"]
        missing_users_dir = os.path.join(dirpath, "users")
        filename = "missing_commentors_from_ldap.json"

        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self.logger = CommentsLogger(self.log_dir).get_logger()

        self.stream = Stream(
            stream_definition.name,
            extract=stream_definition.extract_cls(
                os.path.join(missing_users_dir, filename)
            ),
            transform=stream_definition.transform_cls(),
            load=stream_definition.load_cls(
                dry_run=dry_run,
                missing_users_dir=missing_users_dir,
                missing_ldap_users_filename=filename,
                logger=self.logger,
                user_api_cls=CDSMigrationUserAPI,
            ),
        )

    def run(self):
        """Run commenters ETL stream."""
        try:
            self.stream.run()
        except Exception as e:
            self.logger.exception(f"Stream {self.stream.name} failed.", exc_info=1)
