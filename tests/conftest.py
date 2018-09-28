# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Pytest configuration."""

from __future__ import absolute_import, print_function

import os
import shutil
import tempfile
from os.path import dirname, join

import pytest
from flask import Flask
from flask_babelex import Babel

from cds_migrator_kit import Cdsmigratorkit


@pytest.yield_fixture()
def instance_path():
    """Temporary instance path."""
    path = tempfile.mkdtemp()
    yield path
    shutil.rmtree(path)


@pytest.fixture()
def base_app(instance_path):
    """Flask application fixture."""
    dump = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        'dump/', 'stats.json')
    logs_dir = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), 'tmp/logs/')
    logs = os.path.join(logs_dir, 'stats.json')
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    os.environ.update(
        APP_INSTANCE_PATH=os.environ.get(
            'INSTANCE_PATH', instance_path),
    )
    app_ = Flask('testapp', instance_path=instance_path,
                 )
    app_.config.update(
        DEBUG_TB_ENABLED=False,
        TESTING=True,
        JSONSCHEMAS_HOST='cdslabs.cern.ch',
        PIDSTORE_DATACITE_DOI_PREFIX='10.0000',
        ACCOUNTS_JWT_ENABLE=False,
        CDS_MIGRATOR_KIT_DUMP_PATH=dump,
        CDS_MIGRATOR_KIT_LOGS_PATH=logs_dir,
        CDS_MIGRATOR_KIT_LOG=logs,
    )

    Babel(app_)
    Cdsmigratorkit(app_)
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
