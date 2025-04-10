# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# cds-migration-kit is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""cds-migration-kit transform step module."""

import csv
import json
import logging
import os.path
import re

from invenio_accounts.models import User
from invenio_rdm_migrator.load.base import Load
from sqlalchemy.exc import NoResultFound

from cds_migrator_kit.videos.weblecture_migration.users.api import CDSVideosMigrationUserAPI

cli_logger = logging.getLogger("migrator")
logger_submitters = logging.getLogger("submitters")


class VideosSubmitterLoad(Load):
    """Submitter load class."""

    def __init__(
        self,
        missing_users_dir=None,
        dry_run=False,
    ):
        """Constructor."""
        self.dry_run = dry_run
        self.missing_users_dir = missing_users_dir
        self.dry_run = dry_run

    def _load(self, entry):
        """Load users."""
        self._owner(entry)

    def _validate(self, entry):
        """Validate data before loading."""
        if not entry:
            return False
        return True

    def _owner(self, json_entry):
        """Fetch or create owner."""
        email = json_entry.get("submitter")
        if not email:
            return
        try:
            user = User.query.filter_by(email=email).one()
            logger_submitters.info(f"User {email} exists.")

            return user.id
        except NoResultFound:
            if not self.dry_run:
                user_id = self._create_owner(email)
                return user_id

    def _create_owner(self, email_addr):
        """Create owner from legacy data.

        Every record needs an owner assigned in deposit.owner
        therefore we need to create dummy accounts
        """

        def get_person_old_db(email):
            missing_users_dump = os.path.join(
                self.missing_users_dir, "missing_users.json"
            )
            with open(missing_users_dump) as json_file:
                missing = json.load(json_file)
            person = next((item for item in missing if item["email"] == email), None)

            return person

        user_api = CDSVideosMigrationUserAPI()
        person_old_db = get_person_old_db(email_addr)

        person_id = None
        displayname = None
        username = None
        existing_identity = None
        extra_data = {"migration": {}}

        # check if email exists in legacy db
        # to obtain more info needed to create an account (full name, username)

        # if user email cannot be found anywhere, create a dummy account without
        # any UserIdentity attached - since we cannot provide identity info -
        # the consequence is that user will not be able to login to this account
        # if they come back to CERN

        if person_old_db:
            names = "".join(person_old_db["displayname"].split())
            username = names.lower().replace(".", "")
            if not username:
                username = f'MIGRATED{email_addr.split("@")[0].replace(".", "")}'
                username = re.sub(r"\W+", "", username)
            displayname = person_old_db["displayname"]
            extra_data["migration"]["source"] = "LEGACY DB, PERSON ID MISSING"
            logger_submitters.warning(f"User {email_addr} found in legacy DB")
        else:
            username = email_addr.split("@")[0].replace(".", "")
            username = re.sub(r"\W+", "", username).lower()
            username = f"MIGRATED{username}"
            extra_data["migration"]["source"] = "RECORD, EMAIL NOT FOUND IN ANY SOURCE"
            logger_submitters.warning(f"User {email_addr} not found.")
        extra_data["migration"]["note"] = "MIGRATED INACTIVE ACCOUNT"

        if person_id:
            existing_identity = user_api.check_person_id_exists(person_id)
            # check if person ID was already registered in the DB by prior sync
            # and return that user as source of truth ( we assume auth service is most
            # up to date)
            if existing_identity:
                return existing_identity.id_user

        try:
            user = user_api.create_user(
                email_addr,
                name=displayname,
                username=username,
                person_id=person_id,
                extra_data=extra_data,
            )
        except Exception as exc:
            logger_submitters.error(
                f"User failed to be migrated: {email_addr}, {displayname}, {username}, {person_id}, {json.dumps(extra_data)} \n {exc}"
            )
            return -1
        
        logger_submitters.info(f"User {email_addr} created.")
        return user.id

    def _cleanup(self):  # pragma: no cover
        """Cleanup data after loading."""
        pass
