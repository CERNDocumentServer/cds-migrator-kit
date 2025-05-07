# -*- coding: utf-8 -*-
#
# Copyright (C) 2024-2025 CERN.
#
# Ccds-migrator-kit is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""cds-migrator-kit user api."""

from abc import ABC, abstractmethod
from copy import deepcopy

from flask import current_app
from invenio_accounts.models import User, UserIdentity
from invenio_db import db
from invenio_oauthclient.models import RemoteAccount
from invenio_userprofiles import UserProfile
from sqlalchemy.exc import IntegrityError

from cds_migrator_kit.transform.dumper import CDSRecordDump


class MigrationUserAPI(ABC):
    """CDS missing user load class."""

    def __init__(self, remote_account_client_id=None):
        """Constructor."""
        self.client_id = current_app.config["CERN_APP_CREDENTIALS"]["consumer_key"]

    def check_person_id_exists(self, person_id):
        """Check if uer identity already exists."""
        return UserIdentity.query.filter_by(id=person_id).one_or_none()

    def create_invenio_user(self, email, username):
        """Commit new user in db."""
        try:
            user = User(email=email, username=username, active=False)
            db.session.add(user)
            db.session.commit()
            return user
        except IntegrityError as e:
            db.session.rollback()
            email_username = email.split("@")[0]
            user = User(
                email=email,
                username=f"duplicated_{username}_{email_username}",
                active=False,
            )
            db.session.add(user)
            db.session.commit()
            return user

    @abstractmethod
    def create_invenio_user_identity(self, user_id, person_id):
        """Return new user identity entry.

        Abstract method to be implemented in subclasses."""
        raise NotImplementedError

    def create_invenio_user_profile(self, user, name):
        """Return new user profile."""
        user_profile = UserProfile(user=user)
        user_profile.full_name = name
        return user_profile

    def create_invenio_remote_account(self, user_id, extra_data=None):
        """Return new user entry."""
        if extra_data is None:
            extra_data = {}
        return RemoteAccount.create(
            client_id=self.client_id, user_id=user_id, extra_data=extra_data
        )

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
            if "department" in extra_data:
                profile_data.update({"department": extra_data["department"]})
            profile = deepcopy(user.user_profile)
            profile.update(profile_data)
            user.user_profile = profile
            db.session.add(user)

        remote_account = self.create_invenio_remote_account(user_id, extra_data)
        db.session.add(remote_account)

        return user
