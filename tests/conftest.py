# -*- coding: utf-8 -*-
#
# Copyright (C) 2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Pytest configuration."""

from __future__ import absolute_import, print_function

import os
import shutil
import tempfile
from os.path import join, dirname

import pytest
from flask import Flask
from flask_babelex import Babel


@pytest.yield_fixture()
def instance_path():
    """Temporary instance path."""
    path = tempfile.mkdtemp()
    yield path
    shutil.rmtree(path)


@pytest.fixture()
def base_app(instance_path):
    """Flask application fixture."""

    instance_path = tempfile.mkdtemp()

    os.environ.update(
        APP_INSTANCE_PATH=os.environ.get(
            'INSTANCE_PATH', instance_path),
    )

    app_ = Flask('testapp', instance_path=instance_path)
    app_.config.update(
        DEBUG_TB_ENABLED=False,
        SQLALCHEMY_DATABASE_URI=os.environ.get(
            'SQLALCHEMY_DATABASE_URI',
            'postgresql+psycopg2://localhost/cds_testing'),
        #  SQLALCHEMY_ECHO=True,
        TESTING=True,
        JSONSCHEMAS_HOST='cdslabs.cern.ch',
        PIDSTORE_DATACITE_DOI_PREFIX='10.0000',
        ACCOUNTS_JWT_ENABLE=False,
    )
    Babel(app_)

    return app_


@pytest.yield_fixture()
def app(base_app):
    """Flask application fixture."""
    with base_app.app_context():
        yield base_app


@pytest.fixture()
def datadir():
    """Get data directory."""
    return join(dirname(__file__), 'data')


# @pytest.yield_fixture()
# def db(app):
#     """Setup database."""
#     if not database_exists(str(db_.engine.url)):
#         create_database(str(db_.engine.url))
#     db_.create_all()
#     yield db_
#     db_.session.remove()
#     db_.drop_all()
