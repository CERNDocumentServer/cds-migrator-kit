# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2024 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS Migrator Records logging handler."""

import logging

cli_logger = logging.getLogger("migrator")
documents_logger = logging.getLogger("documents_logger")
items_logger = logging.getLogger("items_logger")


def migration_exception_handler(exc, output, key, value, rectype=None, **kwargs):
    """Migration exception handling - log to files.

    :param exc: exception
    :param output: generated output version
    :param key: MARC field ID
    :param value: MARC field value
    :return:
    """
    logger = logging.getLogger(f"{rectype}s_logger")
    cli_logger.error(
        "#RECID: #{0} - {1}  MARC FIELD: *{2}*, input value: {3}, -> {4}, ".format(
            output["legacy_recid"], exc.message, key, value, output
        )
    )
    logger.error(
        "MARC: {0}, INPUT VALUE: {1} ERROR: {2}" "".format(key, value, exc.message),
        extra=dict(legacy_id=output["legacy_recid"], status="WARNING", new_pid=None),
    )