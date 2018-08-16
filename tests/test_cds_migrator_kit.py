# -*- coding: utf-8 -*-
#
# Copyright (C) 2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Module tests."""

from __future__ import absolute_import, print_function

from flask import Flask

from cds_migrator_kit import cdsmigratorkit


def test_version():
    """Test version import."""
    from cds_migrator_kit import __version__
    assert __version__


def test_init():
    """Test extension initialization."""
    app = Flask('testapp')
    ext = cdsmigratorkit(app)
    assert 'cds-migrator-kit' in app.extensions

    app = Flask('testapp')
    ext = cdsmigratorkit()
    assert 'cds-migrator-kit' not in app.extensions
    ext.init_app(app)
    assert 'cds-migrator-kit' in app.extensions


def test_view(app):
    """Test view."""
    cdsmigratorkit(app)
    with app.test_client() as client:
        res = client.get("/")
        assert res.status_code == 200
        assert 'Welcome to cds-migrator-kit' in str(res.data)
