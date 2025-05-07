# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos user api."""
import logging
from copy import deepcopy

from flask import current_app
from invenio_accounts.models import User, UserIdentity
from invenio_db import db
from psycopg.errors import UniqueViolation
from sqlalchemy.exc import IntegrityError

from cds_migrator_kit.users.api import MigrationUserAPI

logger_submitters = logging.getLogger("submitters")


class CDSVideosMigrationUserAPI(MigrationUserAPI):
    """CDS missing user load class."""

    def __init__(self, remote_account_client_id=None):
        """Constructor."""
        self.client_id = current_app.config["CERN_APP_OPENID_CREDENTIALS"][
            "consumer_key"
        ]

    def create_invenio_user(self, email, username):
        """Commit new user in db."""
        try:
            user = User(email=email, username=username, active=False)
            db.session.add(user)
            db.session.commit()
            return user
        except IntegrityError as e:
            logger_submitters.error(
                f"User creation failed: {email}, username: {username}\n {e}"
            )
            raise

    def create_invenio_user_identity(self, user_id, person_id):
        """Return new user identity entry."""
        cern_remote_app_name = current_app.config["REMOTE_APP_NAME"]
        try:
            return UserIdentity(
                id=person_id,
                method=cern_remote_app_name,
                id_user=user_id,
            )
        except (IntegrityError, UniqueViolation) as e:
            logger_submitters.error(
                f"User identity creation failed: {user_id}, username: {person_id}\n {e}"
            )
            raise

    def create_user(self, email, name, person_id, username, extra_data=None):
        """Create an invenio user."""
        user = self.create_invenio_user(email, username)
        user_id = user.id
        profile_data = {}
        if person_id:
            identity = self.create_invenio_user_identity(user_id, person_id)
            db.session.add(identity)
            profile_data = {
                "person_id": person_id,
            }
        if name:
            profile = deepcopy(user.user_profile)
            profile.update(profile_data)
            user.user_profile = profile
            db.session.add(user)

        remote_account = self.create_invenio_remote_account(user_id, extra_data)
        db.session.add(remote_account)

        return user
