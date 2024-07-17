# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS Migrator app extension."""

from . import config
from .records.views import blueprint


class CdsMigratorKit(object):
    """cds-migrator-kit extension."""

    def __init__(self, app=None):
        """Extension initialization."""
        if app:
            self.init_app(app)

    def init_app(self, app):
        """Flask application initialization."""
        self.init_config(app)
        app.extensions["cds-migrator-kit"] = self
        app.register_blueprint(blueprint, name="cds_migrator_kit_records_bp")

    def init_config(self, app):
        """Initialize configuration."""
        # Use theme's base template if theme is installed
        if "BASE_TEMPLATE" in app.config:
            app.config.setdefault(
                "CDS_MIGRATOR_KIT_BASE_TEMPLATE",
                app.config["BASE_TEMPLATE"],
            )
        for k in dir(config):
            if k.startswith("CDS_MIGRATOR_KIT_"):
                app.config.setdefault(k, getattr(config, k))
