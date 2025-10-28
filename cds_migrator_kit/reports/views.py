# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS Migrator Records views."""

import io
import json
import logging
import os
import zipfile

from flask import (
    Blueprint,
    abort,
    current_app,
    jsonify,
    make_response,
    render_template,
    request,
    send_file,
)

from .log import MigrationProgressLogger, RecordStateLogger

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
def results(collection):
    """Render a basic view."""
    try:
        display_errors_only = request.args.get("errors", 0)
        reload = request.args.get("reload", 0)
        page = request.args.get("page", 1)
        pagination = request.args.get("pagination", 0)
        prev_page = False
        next_page = False
        logger = MigrationProgressLogger(collection=collection)
        record_logs = logger.read_log()
        template = "cds_migrator_kit_records/records.html"
        record_logs = list(record_logs)
        critical = 0
        warning = 0
        errored = 0
        migrated = 0
        total = len(record_logs)
        if pagination:
            page = int(page)
            paginated_record_logs = record_logs[page * 1000 : page * 1000 + 999]
            prev_page = page - 1 if page - 1 > 0 else None
            next_page = page + 1 if page * 1000 + 1 < total else None
        else:
            paginated_record_logs = record_logs
        for log in record_logs:
            if log.get("priority") == "critical":
                critical += 1
            if log.get("priority") == "warning":
                warning += 1
            if log.get("clean") == "False":
                errored += 1
            if log.get("clean") == "True":
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
            display_errors_only=display_errors_only,
            live_reload=reload,
            page=page,
            pagination=pagination,
            prev_page=prev_page,
            next_page=next_page,
            paginated_record_logs=paginated_record_logs,
        )
    except FileNotFoundError as e:
        template = "cds_migrator_kit_records/rectype_missing.html"
        return render_template(
            template,
        )


@blueprint.route("/record/<collection>/<recid>")
def send_json(collection, recid):
    """Serves static json preview output files."""
    print(collection, recid)
    logger = RecordStateLogger(collection=collection)
    records = logger.load_record_dumps()
    if recid not in records:
        abort(404)
    return jsonify(records[recid])


@blueprint.route("/results/<collection>/download")
def download_results(collection):
    """Download rendered HTML and CSV as a single ZIP."""
    html = results(collection)
    csv_path = os.path.abspath("cds_migrator_kit/tmp/logs/it/rdm_migration_errors.csv")

    mem_zip = io.BytesIO()
    with zipfile.ZipFile(mem_zip, "w") as zf:
        zf.writestr(f"{collection}_results.html", html)
        if os.path.exists(csv_path):
            zf.write(csv_path, arcname=f"{collection}_migration_errors.csv")
        else:
            zf.writestr("missing_file.txt", "CSV file not found.")

    mem_zip.seek(0)
    return send_file(
        mem_zip,
        mimetype="application/zip",
        as_attachment=True,
        download_name=f"{collection}_results_report.zip",
    )
