# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM transform step module."""
import csv
import json
import logging
import os.path
import re

from flask import current_app
from invenio_accounts.models import User
from invenio_db import db
from invenio_rdm_migrator.load.base import Load
from sqlalchemy.exc import NoResultFound

cli_logger = logging.getLogger("migrator")


class CDSSubmitterLoad(Load):
    """Submitter load class."""

    def __init__(
        self,
        missing_users_dir=None,
        missing_users_filename="people.csv",
        dry_run=False,
        logger=logging.getLogger("users"),
        user_api_cls=None,
        missing_ldap_users_filename="missing_users.json",
    ):
        """Constructor."""
        self.dry_run = dry_run
        self.missing_users_dir = missing_users_dir
        self.missing_users_filename = missing_users_filename
        self.missing_ldap_users_filename = missing_ldap_users_filename
        self.dry_run = dry_run
        self.logger = logger
        self.user_api_cls = user_api_cls

    def _load(self, entry):
        """Load users."""
        self._owner(entry)
        self._reviewers(entry)

    def _validate(self, entry):
        """Validate data before loading."""
        if not entry:
            return False
        return True

    def _owner(self, json_entry):
        """Fetch or create owner."""
        email = json_entry.get("submitter")
        return self._find_or_create_by_email(email)

    def _reviewers(self, json_entry):
        """Fetch or create request reviewer accounts (906__m/906__p).

        906__m gives an email directly, handled the same way as the
        submitter (859__f). 906__p only gives a "Family name, Given name"
        string with no email: it is first matched against an existing
        account by profile name, then falls back to the same people.csv
        lookup used for owners (by name this time), and as a last resort
        a CERN-style email is made up so an account can still be created.
        """
        for reviewer in json_entry.get("reviewers", []):
            if "@" in reviewer:
                self._find_or_create_by_email(reviewer)
            else:
                self._find_or_create_reviewer_by_name(reviewer)

    def _find_or_create_by_email(self, email):
        """Fetch or create a user account by email."""
        if not email:
            return
        try:
            user = User.query.filter_by(email=email).one()
            return user.id
        except NoResultFound:
            if not self.dry_run:
                user_id = self._create_owner(email)
                return user_id

    def _parse_reviewer_name(self, name):
        """Split a "Family name, Given name" or "Given name Family name"
        string into (family_name, given_name)."""
        if "," in name:
            family_name, _, given_name = name.partition(",")
            return family_name.strip(), given_name.strip()
        parts = name.split()
        family_name = parts[-1] if parts else name.strip()
        given_name = " ".join(parts[:-1])
        return family_name, given_name

    def _find_reviewer_by_name(self, family_name, given_name):
        """Match a reviewer name to an existing account via their profile."""
        query = User.query.filter(
            db.func.lower(User._user_profile["family_name"].as_string())
            == family_name.lower()
        )
        if given_name:
            query = query.filter(
                db.func.lower(User._user_profile["given_name"].as_string())
                == given_name.lower()
            )
        return query.one_or_none()

    def _find_person_email_by_name(self, family_name, given_name):
        """Look up a reviewer's email in the people collection dump by name.

        Same source as _create_owner's `get_person`, but keyed by name
        since a name-only reviewer (906__p) has no email to search by.
        """
        missing_users_dump = os.path.join(
            self.missing_users_dir, self.missing_users_filename
        )
        with open(missing_users_dump) as csv_file:
            for row in csv.reader(csv_file):
                surname, given_names = row[2].strip().lower(), row[3].strip().lower()
                if surname != family_name.lower():
                    continue
                if given_name and given_names != given_name.lower():
                    continue
                return row[0]
        return None

    def _fabricate_email(self, family_name, given_name):
        """Make up a plausible CERN email when none can be found anywhere,
        following the firstname.lastname@cern.ch convention."""
        given = re.sub(r"[^a-z]", "", given_name.lower())
        family = re.sub(r"[^a-z]", "", family_name.lower())
        local_part = f"{given}.{family}" if given else family
        return f"{local_part}@cern.ch"

    def _find_or_create_reviewer_by_name(self, name):
        """Resolve a "Family name, Given name" reviewer with no email.

        Tries, in order: (1) an existing account matched by profile name,
        (2) an email looked up by name in the people collection dump, and
        (3) a fabricated CERN-style email as a last resort, so a reviewer
        with no email information can still get an account created - the
        same way an owner does, just starting from a name instead.
        """
        family_name, given_name = self._parse_reviewer_name(name)

        user = self._find_reviewer_by_name(family_name, given_name)
        if user is not None:
            return user.id

        if self.dry_run:
            return None

        email = self._find_person_email_by_name(family_name, given_name)
        if not email:
            email = self._fabricate_email(family_name, given_name)
            self.logger.warning(
                f"Reviewer '{name}' has no email and no matching person was "
                f"found in the people collection - using a made-up email: "
                f"{email}"
            )

        user_id = self._find_or_create_by_email(email)
        if user_id:
            self._ensure_reviewer_profile_name(user_id, family_name, given_name)
        return user_id

    def _ensure_reviewer_profile_name(self, user_id, family_name, given_name):
        """Make sure family_name/given_name are set on the profile.

        MigrationUserAPI.create_user() only ever sets `full_name`, but
        find_reviewer() (cds_migrator_kit/rdm/records/load/load.py) matches
        reviewers by `family_name`/`given_name` - without this, a
        just-created reviewer account would still be unmatchable by name
        later on. Only fills in missing values, never overwrites an
        existing (e.g. already CERN-synced) profile.
        """
        user = User.query.get(user_id)
        if user is None:
            return
        profile = dict(user.user_profile or {})
        changed = False
        if not profile.get("family_name"):
            profile["family_name"] = family_name
            changed = True
        if given_name and not profile.get("given_name"):
            profile["given_name"] = given_name
            changed = True
        if changed:
            user.user_profile = profile
            db.session.add(user)
            db.session.commit()

    def _create_owner(self, email_addr):
        """Create owner from legacy data.

        Every record needs an owner assigned in parent.access.owned_by
        therefore we need to create dummy accounts
        """
        logger_users = self.logger

        def get_person(email):
            missing_users_dump = os.path.join(
                self.missing_users_dir, self.missing_users_filename
            )
            with open(missing_users_dump) as csv_file:
                for row in csv.reader(csv_file):
                    if email == row[0].lower():
                        return row

        def get_person_old_db(email):
            missing_users_dump = os.path.join(
                self.missing_users_dir, self.missing_ldap_users_filename
            )
            with open(missing_users_dump) as json_file:
                missing = json.load(json_file)
            person = next((item for item in missing if item["email"] == email), None)

            return person

        user_api = self.user_api_cls()
        person = get_person(email_addr)
        person_old_db = get_person_old_db(email_addr)

        person_id = None
        displayname = None
        username = None
        existing_identity = None
        extra_data = {"migration": {}}

        # first check if submitter email is in people collection.
        # If yes, we take their person_id and check if account already synced
        # (by UserIdentity PK)

        # if person id cannot be obtained, check if email exists in legacy db
        # to obtain more info needed to create an account (full name, username)

        # if user email cannot be found anywhere, create a dummy account without
        # any UserIdentity attached - since we cannot provide identity info -
        # the consequence is that user will not be able to login to this account
        # if they come back to CERN

        if person:
            # person id might be missing from people collection
            person_id = person[1] if person[1] else None
            displayname = f"{person[2]} {person[3]}"
            username = f"{person[2][0]}{person[3]}".lower().replace(" ", "")
            username = re.sub(r"\W+", "", username)
            if len(person) == 5:
                extra_data["department"] = person[4]
            extra_data["migration"]["source"] = (
                f"PEOPLE COLLECTION, "
                f"{'PERSON_ID FOUND' if person_id else 'PERSON_ID NOT FOUND'}"
            )
            logger_users.warning(f"User {email_addr} found in people collection")
        elif person_old_db:
            names = "".join(person_old_db["displayname"].split())
            username = names.lower().replace(".", "")
            # Validate username, if not valid, generate a new prefixed username from email
            if not re.fullmatch(
                current_app.config["ACCOUNTS_USERNAME_REGEX"], username
            ):
                username = f'MIGRATED{email_addr.split("@")[0].replace(".", "")}'
                username = re.sub(r"\W+", "", username)
            displayname = person_old_db["displayname"]
            extra_data["migration"]["source"] = "LEGACY DB, PERSON ID MISSING"
            logger_users.warning(f"User {email_addr} found in legacy DB")
        else:
            username = email_addr.split("@")[0].replace(".", "")
            username = re.sub(r"\W+", "", username)
            username = f"MIGRATED{username}"
            extra_data["migration"]["source"] = "RECORD, EMAIL NOT FOUND IN ANY SOURCE"
            logger_users.warning(f"User {email_addr} not found.")
        extra_data["migration"]["note"] = "MIGRATED INACTIVE ACCOUNT"

        if person_id:
            existing_identity = user_api.check_person_id_exists(person_id)
            # check if person ID was already registered in the DB by prior sync
            # and return that user as source of truth ( we assume auth service is most
            # up to date)
            if existing_identity:
                logger_users.info(
                    f"User {email_addr} already exists with person ID {person_id}"
                )
                return existing_identity.id_user

        try:
            logger_users.info(
                f"Creating user {email_addr}, {displayname}, {username}, {person_id}, {json.dumps(extra_data)}"
            )
            user = user_api.create_user(
                email_addr,
                name=displayname,
                username=username,
                person_id=person_id,
                extra_data=extra_data,
            )
        except Exception as exc:
            logger_users.error(
                f"User failed to be migrated: {email_addr}, {displayname}, {username}, {person_id}, {json.dumps(extra_data)} \n {exc}"
            )
            return -1
        return user.id

    def _cleanup(self):  # pragma: no cover
        """Cleanup data after loading."""
        pass
