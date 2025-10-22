# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS Migrator Records loggers."""

import csv
import json
import logging
import os

formatter = logging.Formatter(
    "%(asctime)s - %(name)s - " "%(message)s - \n " "[in %(pathname)s:%(lineno)d]"
)


class VideosJsonLogger:
    """Log videos record migration."""

    @classmethod
    def initialize(cls, log_dir, keep_logs=False):
        """Initialize the videos logger."""
        cls.keep_logs = keep_logs
        # Determine file mode based on keep_logs flag
        log_mode = "a" if keep_logs else "w"

        logger_files = logging.getLogger("files")
        fh = logging.FileHandler(log_dir / "files.log", mode=log_mode)
        fh.setFormatter(formatter)
        logger_files.addHandler(fh)

        logger_flows = logging.getLogger("flows")
        fh = logging.FileHandler(log_dir / "flows.log", mode=log_mode)
        fh.setFormatter(formatter)
        logger_flows.addHandler(fh)

        # Add a new json file for video records redirections
        cls.json_path = log_dir / "record_redirections.json"
        if not keep_logs or not cls.json_path.exists():
            with open(cls.json_path, "w") as json_file:
                json.dump([], json_file)

    @classmethod
    def log_record_redirection(cls, legacy_id, cds_videos_id, legacy_anchor_id=None):
        """Log multi video record redirection to a json file."""
        entry = {
            "legacy_id": legacy_id,
            "cds_videos_id": cds_videos_id,
        }
        if legacy_anchor_id:
            entry["legacy_anchor_id"] = legacy_anchor_id
        if os.path.exists(cls.json_path):
            with open(cls.json_path, "r+") as json_file:
                data = json.load(json_file)
                data.append(entry)
                json_file.seek(0)
                json.dump(data, json_file, indent=4)


class SubmitterLogger:
    """Log submitters migration."""

    @classmethod
    def initialize(cls, log_dir):
        """Initialize the submitter logger."""
        logger_submitters = logging.getLogger("submitters")
        logger_submitters.setLevel(logging.DEBUG)
        fh = logging.FileHandler(log_dir / "submitters.log")
        fh.setFormatter(formatter)
        logger_submitters.addHandler(fh)
