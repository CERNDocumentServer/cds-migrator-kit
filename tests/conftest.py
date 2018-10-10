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
from os.path import dirname, join

import pytest
from invenio_app.factory import create_ui


@pytest.fixture(scope='module')
def app_config(app_config):
    """Application configuration fixture."""
    base_path = os.path.dirname(os.path.realpath(__file__))
    logs_dir = os.path.join(base_path, 'tmp/logs/')
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)

    app_config['CDS_MIGRATOR_KIT_LOGS_PATH'] = logs_dir
    return app_config


@pytest.fixture(scope='module')
def create_app():
    """Create test app."""
    return create_ui


@pytest.fixture()
def datadir():
    """Get data directory."""
    return join(dirname(__file__), 'data')
