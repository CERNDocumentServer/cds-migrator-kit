# -*- coding: utf-8 -*-
#
# Copyright (C) 2024 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM transform user."""
import csv
import json
from copy import deepcopy

from flask import current_app
from invenio_accounts.models import User, UserIdentity
from invenio_cern_sync.sso import cern_remote_app_name
from invenio_db import db
from invenio_oauthclient.models import RemoteAccount
from invenio_rdm_migrator.load import Load
from invenio_rdm_migrator.transform.base import Entry, Transform
from invenio_userprofiles import UserProfile
from psycopg.errors import UniqueViolation
from sqlalchemy.exc import IntegrityError

from cds_migrator_kit.transform.dumper import CDSRecordDump


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
    """CDS user intermediate load class.

        Writes a csv file containing translated people collection entries.
    """

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


