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


@blueprint.route("/results/<collection>")
def results(collection=None):
    """Render a basic view."""
    try:
        logger = RDMJsonLogger(collection=collection)
        record_logs = logger.read_log()
        template = "cds_migrator_kit_records/records.html"
        record_logs = list(record_logs)
        critical = 0
        warning = 0
        errored = 0
        migrated = 0
        total = len(record_logs)
        for log in record_logs:
            if log["priority"] == "critical":
                critical += 1
            if log["priority"] == "warning":
                warning += 1
            if log["clean"] == "False":
                errored += 1
            if log["clean"] == "True":
                migrated += 1
        return render_template(
            template,
            record_logs=record_logs,
            total=total if total != 0 else 1,
            critical=critical,
            warning=warning,
            migrated=migrated,
            errored=errored,
            collection=collection,
        )
    except FileNotFoundError as e:
        template = "cds_migrator_kit_records/rectype_missing.html"
        return render_template(
            template,
        )


@blueprint.route("/record/<collection>/<recid>")
def send_json(collection, recid):
    """Serves static json preview output files."""
    logger = RDMJsonLogger(collection=collection)
    records = logger.load_record_dumps()
    if recid not in records:
        abort(404)
    return jsonify(records[recid])
