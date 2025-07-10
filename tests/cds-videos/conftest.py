# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Pytest configuration."""

import os
import sys
import tempfile
from os.path import dirname, join

import pytest
from cds.modules.invenio_deposit.permissions import action_admin_access
from cds.modules.redirector.views import api_blueprint as cds_api_blueprint
from invenio_access.models import ActionRoles
from invenio_accounts.models import Role, User
from invenio_app.factory import create_app as invenio_create_app
from invenio_db import db as db_
from invenio_files_rest.views import blueprint as files_rest_blueprint
from sqlalchemy_utils.functions import create_database, database_exists


@pytest.fixture(scope="module")
def create_app():
    """Factory function for pytest-invenio."""

    def factory(**config):
        app = invenio_create_app(**config)
        # Register custom blueprints like in cds-videos
        app.register_blueprint(files_rest_blueprint)
        app.register_blueprint(cds_api_blueprint)
        return app

    return factory


@pytest.fixture(scope="module")
def app_config(app_config):
    """Application configuration fixture."""
    base_path = os.path.dirname(os.path.realpath(__file__))
    logs_dir = os.path.join(base_path, "tmp/logs/")
    os.makedirs(logs_dir, exist_ok=True)

    instance_path = tempfile.mkdtemp()
    os.environ["INVENIO_INSTANCE_PATH"] = instance_path
    os.environ["INVENIO_STATIC_FOLDER"] = os.path.join(
        sys.prefix, "var/instance/static"
    )

    app_config.update(
        {
            "DEBUG_TB_ENABLED": False,
            "TESTING": True,
            "CELERY_TASK_ALWAYS_EAGER": True,
            "CELERY_RESULT_BACKEND": "cache",
            "CELERY_CACHE_BACKEND": "memory",
            "CELERY_TASK_EAGER_PROPAGATES_EXCEPTIONS": True,
            "CELERY_TASK_TRACK_STARTED": True,
            "SQLALCHEMY_TRACK_MODIFICATIONS": False,
            "SQLALCHEMY_DATABASE_URI": "postgresql+psycopg2://invenio:invenio@localhost/invenio",
            "SITE_URL": "https://localhost:5000",
            "JSONSCHEMAS_HOST": "cds.cern.ch",
            "DEPOSIT_UI_ENDPOINT": "{scheme}://{host}/deposit/{pid_value}",
            "ACCOUNTS_JWT_ENABLE": False,
            "THEOPLAYER_LIBRARY_LOCATION": "https://localhost-theoplayer.com",
            "THEOPLAYER_LICENSE": "CHANGE_ME",
            "PRESERVE_CONTEXT_ON_EXCEPTION": False,
            "REST_CSRF_ENABLED": False,
            "MOUNTED_MEDIA_CEPH_PATH": os.path.join(base_path, "data/files/media_data"),
            "CDS_MIGRATOR_KIT_LOGS_PATH": logs_dir,
            "CDS_MIGRATOR_KIT_STREAM_CONFIG": "tests/cds-videos/data/streams.yaml",
            "WEBLECTURES_MIGRATION_SYSTEM_USER": "weblecture-service@cern.ch",
            "CAS_LECTURES_ACCESS": [],
        }
    )

    return app_config


@pytest.fixture(scope="module", autouse=True)
def db(app):
    """Setup database."""
    if not database_exists(str(db_.engine.url)):
        create_database(str(db_.engine.url))
    db_.create_all()
    yield db_
    db_.session.remove()
    db_.drop_all()


@pytest.fixture()
def datadir():
    """Get data directory."""
    return join(dirname(__file__), "data")


@pytest.fixture(scope="module", autouse=True)
def weblecture_system_user(base_app, db):
    """Create the weblecture system user by default."""
    email = base_app.config["WEBLECTURES_MIGRATION_SYSTEM_USER"]
    user = User.query.filter_by(email=email).first()
    if not user:
        with base_app.app_context():
            datastore = base_app.extensions["security"].datastore
            user = datastore.create_user(
                email=email,
                password="tester",
                active=True,
            )
            admin_role = Role.query.filter_by(name="admin").first()
            if not admin_role:
                admin_role = Role(name="admin")
                db.session.add(admin_role)

            db.session.add(
                ActionRoles(action=action_admin_access.value, role=admin_role)
            )
            datastore.add_role_to_user(user, admin_role)
            db.session.commit()

    return user
