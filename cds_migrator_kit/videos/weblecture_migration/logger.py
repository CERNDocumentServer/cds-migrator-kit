# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS Migrator Records loggers."""

import logging
import os
from pathlib import Path

from flask import current_app

formatter = logging.Formatter(
    "%(asctime)s - %(name)s - " "%(message)s - \n " "[in %(pathname)s:%(lineno)d]"
)


class VideosJsonLogger:
    """Log videos record migration."""

    @classmethod
    def initialize(cls, collection, keep_logs=False):
        """Initialize the videos logger."""
        cls.keep_logs = keep_logs
        # Determine file mode based on keep_logs flag
        log_mode = "a" if keep_logs else "w"

        logs_path = os.path.join(
            current_app.config["CDS_MIGRATOR_KIT_LOGS_PATH"], collection
        )
        log_dir = Path(logs_path)
        log_dir.mkdir(parents=True, exist_ok=True)

        logger_files = logging.getLogger("files")
        fh = logging.FileHandler(log_dir / "files.log", mode=log_mode)
        fh.setFormatter(formatter)
        logger_files.addHandler(fh)

        logger_flows = logging.getLogger("flows")
        fh = logging.FileHandler(log_dir / "flows.log", mode=log_mode)
        fh.setFormatter(formatter)
        logger_flows.addHandler(fh)


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
