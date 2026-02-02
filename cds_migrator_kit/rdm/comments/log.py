# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-Migrator-Kit is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Migrator-Kit comments logger module."""

import logging


class CommentsLogger:
    """Migrator comments logger."""

    @classmethod
    def initialize(cls, log_dir):
        """Constructor."""
        formatter = logging.Formatter(
            fmt="%(asctime)s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        logger = logging.getLogger("comments-migrator")
        fh = logging.FileHandler(log_dir / "info.log")
        logger.setLevel(logging.WARNING)
        logger.addHandler(fh)

        # errors to file
        fh = logging.FileHandler(log_dir / "error.log")
        fh.setLevel(logging.ERROR)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        # info to stream/stdout
        sh = logging.StreamHandler()
        sh.setFormatter(formatter)
        sh.setLevel(logging.INFO)
        logger.addHandler(sh)

    @classmethod
    def get_logger(cls):
        """Get migration logger."""
        return logging.getLogger("comments-migrator")
