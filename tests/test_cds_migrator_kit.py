# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Module tests."""

from __future__ import absolute_import, print_function

from flask import Flask

from cds_migrator_kit import CdsMigratorKit


def test_version():
    """Test version import."""
    from cds_migrator_kit import __version__
    assert __version__


def test_init():
    """Test extension initialization."""
    app = Flask('testapp')
    ext = CdsMigratorKit(app)
    assert 'cds-migrator-kit' in app.extensions

    app = Flask('testapp')
    ext = CdsMigratorKit()
    assert 'cds-migrator-kit' not in app.extensions
    ext.init_app(app)
    assert 'cds-migrator-kit' in app.extensions


def test_view(base_app):
    """Test view."""
    CdsMigratorKit(base_app)
    with base_app.test_client() as client:
        res = client.get("/")
        assert res.status_code == 200
        assert 'migrator' in str(res.data)
