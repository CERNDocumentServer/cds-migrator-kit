# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS Migrator Records views."""

from __future__ import absolute_import, print_function

import logging

from flask import Blueprint, abort, current_app, jsonify, render_template

from cds_migrator_kit.config import CDS_MIGRATOR_KIT_LOGS_PATH

from .log import JsonLogger

cli_logger = logging.getLogger('migrator')


blueprint = Blueprint(
    'cds_migrator_kit_records',
    __name__,
    template_folder='templates',
    static_folder='static',
)


@blueprint.route("/")
def index():
    """Render a basic view."""
    return render_template(
        "cds_migrator_kit_records/welcome.html",
    )


@blueprint.route("/results")
def results():
    """Render a basic view."""
    return render_template("cds_migrator_kit_records/index.html", rectype=None)


@blueprint.route("/results/<rectype>")
def results_rectype(rectype=None):
    """Render a basic view."""
    try:
        logger = JsonLogger.get_json_logger(rectype)
        logger.load()
        template = "cds_migrator_kit_records/{}.html".format(rectype)
    except FileNotFoundError:
        template = "cds_migrator_kit_records/rectype_missing.html"

    return render_template(
        template,
        stats_sorted_by_key=[
            logger.stats[stat]
            for stat in sorted(logger.stats.keys())
        ],
        stats=logger.stats,
        records=logger.records,
        rectype=rectype
    )


@blueprint.route('/record/<rectype>/<recid>')
def send_json(rectype, recid):
    """Serves static json preview output files."""
    logger = JsonLogger.get_json_logger(rectype)
    logger.load()
    if recid not in logger.records:
        abort(404)
    return jsonify(logger.records[recid])
