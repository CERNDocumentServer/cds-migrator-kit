# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Pytest configuration."""

import os
from os.path import dirname, join

import pytest
from cds.modules.invenio_deposit.permissions import action_admin_access
from invenio_access.models import ActionRoles
from invenio_accounts.models import Role, User
from invenio_app.factory import create_ui


@pytest.fixture(scope="module")
def app_config(app_config):
    """Application configuration fixture."""
    base_path = os.path.dirname(os.path.realpath(__file__))
    logs_dir = os.path.join(base_path, "tmp/logs/")
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    app_config["MOUNTED_MEDIA_CEPH_PATH"] = os.path.join(
        base_path, "data/files//media_data"
    )
    app_config["CDS_MIGRATOR_KIT_LOGS_PATH"] = logs_dir
    app_config["CDS_MIGRATOR_KIT_STREAM_CONFIG"] = "tests/cds-videos/data/streams.yaml"
    return app_config


@pytest.fixture(scope="module")
def create_app(app_config):
    """Create test app."""
    return create_ui


@pytest.fixture()
def datadir():
    """Get data directory."""
    return join(dirname(__file__), "data")


@pytest.fixture()
def admin(app, db):
    """Admin user for requests."""
    with db.session.begin_nested():
        datastore = app.extensions["security"].datastore
        admin = datastore.create_user(
            email="admin@inveniosoftware.org", password="tester", active=True
        )
        # Give a admin role to admin
        admin_role = Role(name="admin")
        db.session.add(ActionRoles(action=action_admin_access.value, role=admin_role))
        datastore.add_role_to_user(admin, admin_role)
    db.session.commit()
    return admin.id
