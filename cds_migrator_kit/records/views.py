# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS Migrator Records views."""

import json
import logging

from flask import Blueprint, abort, current_app, jsonify, render_template

from .log import JsonLogger, RDMJsonLogger

cli_logger = logging.getLogger("migrator")


blueprint = Blueprint(
    "cds_migrator_kit_records",
    __name__,
    template_folder="templates",
    static_folder="static",
)


@blueprint.route("/")
def index():
    """Render a basic view."""
    return render_template(
        "cds_migrator_kit_records/welcome.html",
    )


@blueprint.route("/results")
def results(rectype=None):
    """Render a basic view."""
    try:

        logger = RDMJsonLogger()
        record_logs = logger.read_log()
        template = "cds_migrator_kit_records/records.html"
        return render_template(
            template,
            record_logs=record_logs,
        )
    except FileNotFoundError as e:
        template = "cds_migrator_kit_records/rectype_missing.html"
        return render_template(
            template,
        )


@blueprint.route("/record/<recid>")
def send_json(recid):
    """Serves static json preview output files."""
    logger = RDMJsonLogger()
    records = logger.load_record_dumps()
    if recid not in records:
        abort(404)
    return jsonify(records[recid])
