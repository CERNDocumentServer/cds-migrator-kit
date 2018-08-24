# -*- coding: utf-8 -*-
#
# Copyright (C) 2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Migration tool kit from old invenio to new flavours."""

from __future__ import absolute_import, print_function

from flask import Blueprint, render_template, send_from_directory

from cds_migrator_kit.modules.migrator.log import JsonLogger

blueprint = Blueprint(
    'cds_migrator_kit',
    __name__,
    template_folder='templates',
    static_folder='static',
)


@blueprint.route("/")
def index():
    """Render a basic view."""
    return render_template(
        "cds_migrator_kit/welcome.html",
    )


@blueprint.route("/results")
def results():
    """Render a basic view."""
    all_stats = JsonLogger.render_stats()

    return render_template(
        "cds_migrator_kit/index.html",
        results=all_stats)


@blueprint.route('/record/<recid>')
def send_json(recid):
    """Serves static json preview output files."""
    return send_from_directory('tmp/logs', '{0}.json'.format(recid))
