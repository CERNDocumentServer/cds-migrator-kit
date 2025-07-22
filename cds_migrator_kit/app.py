# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2020 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS Migrator Development instance entrypoint."""

from flask import Flask
from flask_babel import Babel

from cds_migrator_kit import CdsMigratorKit
from cds_migrator_kit.reports.views import blueprint

# Create Flask application
app = Flask(__name__)
Babel(app)
CdsMigratorKit(app)
app.register_blueprint(blueprint)


@app.after_request
def add_no_cache_headers(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response
