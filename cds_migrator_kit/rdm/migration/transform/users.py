# -*- coding: utf-8 -*-
#
# Copyright (C) 2024 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM transform user."""
import csv
import json

from flask import current_app
from invenio_oauthclient.models import RemoteAccount
from invenio_rdm_migrator.load import Load
from invenio_rdm_migrator.streams.users import UserEntry, UserTransform
from invenio_rdm_migrator.transform.base import Transform, Entry
from invenio_userprofiles import UserProfile
from sqlalchemy.exc import IntegrityError

from cds_migrator_kit.rdm.migration.extract import LegacyUserExtract
from invenio_accounts.models import User, UserIdentity
from invenio_db import db

from cds_migrator_kit.rdm.migration.transform.xml_processing.dumper import CDSRecordDump


class CDSUserEntry(Entry):
    """Transform CDS user record to RDM user record."""

    def _email(self, entry):
        """Returns the email."""
        return entry["email"]

    def _active(self, entry):
        """Returns if the user is active."""
        return False if entry["note"] == "0" else True

    def _preferences(self, entry):
        """Returns the preferences."""
        return {
            "visibility": "restricted",
            "email_visibility": "restricted",
        }

    def _login_information(self, entry):
        """Returns the login information."""
        return {
            "last_login_at": entry.get("last_login"),
            "current_login_at": None,
            "last_login_ip": None,
            "current_login_ip": None,
            "login_count": None,
        }

    def transform(self, entry):
        """Transform a user single entry."""
        record_dump = CDSRecordDump(
            entry,
        )

        record_dump.prepare_revisions()
        timestamp, json_data = record_dump.latest_revision
        return json_data


class CDSRDMUserTransform(Transform):
    """CDSUserTransform."""

    def _transform(self, entry):
        """Transform the user."""
        user = {}
        json_data = CDSUserEntry().transform(entry)
        return json_data


class CDSUserIntermediaryLoad(Load):
    """CDS user intermediate load class."""

    def __init__(self, filepath, **kwargs):
        """Constructor."""
        self.filepath = filepath
        self.dumpfile = open(self.filepath, "w", newline="")
        fieldnames = ["email", "person_id", "surname", "given_names", "department"]
        self.writer = csv.DictWriter(self.dumpfile, fieldnames=fieldnames)
        self.writer.writeheader()

    def _load(self, entry, *args, **kwargs):
        self.writer.writerow(
            {
                "email": entry["email"],
                "person_id": entry["person_id"],
                "surname": entry["surname"].upper(),
                "given_names": entry["given_names"],
                "department": entry["department"],
            }
        )

    def _cleanup(self):  # pragma: no cover
        """Cleanup data after loading."""
        pass


class CDSMissingUserLoad:
    """CDS missing user load class."""

    def __init__(self, remote_account_client_id=None):
        """Constructor."""
        self.client_id = current_app.config["CERN_APP_CREDENTIALS"]["consumer_key"]

    def create_invenio_user(self, email, username):
        """Commit new user in db."""
        try:
            user = User(email=email, username=username, active=False)
            db.session.add(user)
            db.session.commit()
            return user
        except IntegrityError as e:
            db.session.rollback()
            user = User(email=email, username=f"duplicate{username}", active=False)
            db.session.add(user)
            db.session.commit()
            return user

    def create_invenio_user_identity(self, user_id, person_id):
        """Return new user identity entry."""
        return UserIdentity(
            id=person_id,
            method=current_app.config["OAUTH_REMOTE_APP_NAME"],
            id_user=user_id,
        )

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

        if person_id:
            identity = self.create_invenio_user_identity(user_id, person_id)
            db.session.add(identity)

        if name:
            profile = self.create_invenio_user_profile(user, name)
            db.session.add(profile)

        remote_account = self.create_invenio_remote_account(user_id, extra_data)
        db.session.add(remote_account)

        return user
