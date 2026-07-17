# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM submitter/reviewer accounts migration logger module."""

import logging


class SubmitterLogger:
    """Migrator submitter/reviewer accounts logger."""

    @classmethod
    def initialize(cls, log_dir):
        """Attach file and stream handlers to the submitter-migrator logger."""
        formatter = logging.Formatter(
            fmt="%(asctime)s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        logger = logging.getLogger("submitter-migrator")
        logger.setLevel(logging.INFO)

        # info and above to file
        fh = logging.FileHandler(log_dir / "info.log")
        fh.setLevel(logging.INFO)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        # errors to their own file
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
        return logging.getLogger("submitter-migrator")
