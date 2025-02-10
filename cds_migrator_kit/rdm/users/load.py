# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM transform step module."""
import csv
import datetime
import json
import logging
import os.path
import re
from abc import ABC
from collections import OrderedDict
from copy import deepcopy
from pathlib import Path

import arrow
from cds_rdm.legacy.models import CDSMigrationAffiliationMapping
from idutils import normalize_ror
from invenio_access.permissions import system_identity
from invenio_accounts.models import User
from invenio_db import db
from invenio_rdm_migrator.streams.records.transform import (
    RDMRecordEntry,
    RDMRecordTransform,
)
from invenio_records_resources.proxies import current_service_registry
from invenio_vocabularies.contrib.names.models import NamesMetadata
from opensearchpy import RequestError
from sqlalchemy.exc import NoResultFound

from cds_migrator_kit.rdm.migration_config import VOCABULARIES_NAMES_SCHEMES
from cds_migrator_kit.rdm.users.api import CDSMigrationUserAPI
from cds_migrator_kit.transform.dumper import CDSRecordDump
from cds_migrator_kit.transform.errors import (
    LossyConversion,
)
from cds_migrator_kit.errors import (
    ManualImportRequired,
    MissingRequiredField,
    RecordFlaggedCuration,
    RestrictedFileDetected,
    UnexpectedValue
)
from cds_migrator_kit.reports.log import RDMJsonLogger
from invenio_rdm_migrator.load.base import Load

cli_logger = logging.getLogger("migrator")


class CDSSubmitterLoad(Load):
    def __init__(
        self,
        missing_users_dir=None,
        missing_users_filename="people.csv",
        dry_run=False,
    ):
        """Constructor."""
        self.dry_run = dry_run
        self.missing_users_dir = missing_users_dir
        self.missing_users_filename = missing_users_filename
        self.dry_run = dry_run

    def _load(self, entry):
        self._owner(entry)

    def _validate(self, entry):
        """Validate data before loading."""
        if not entry:
            return False
        return True

    def _owner(self, json_entry):
        email = json_entry.get("submitter")
        if not email:
            return
        try:
            user = User.query.filter_by(email=email).one()
            return user.id
        except NoResultFound:
            if not self.dry_run:
                user_id = self._create_owner(email)
                return user_id

    def _create_owner(self, email_addr):
        """Create owner from legacy data.

        Every record needs an owner assigned in parent.access.owned_by
        therefore we need to create dummy accounts
        """
        logger_users = logging.getLogger("users")

        def get_person(email):
            missing_users_dump = os.path.join(
                self.missing_users_dir, self.missing_users_filename
            )
            with open(missing_users_dump) as csv_file:
                for row in csv.reader(csv_file):
                    if email == row[0]:
                        return row

        def get_person_old_db(email):
            missing_users_dump = os.path.join(
                self.missing_users_dir, "missing_users.json"
            )
            with open(missing_users_dump) as json_file:
                missing = json.load(json_file)
            person = next((item for item in missing if item["email"] == email), None)

            return person

        user_api = CDSMigrationUserAPI()
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
            if not username:
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
            logger_users.error(
                f"User failed to be migrated: {email_addr}, {displayname}, {username}, {person_id}, {json.dumps(extra_data)} \n {exc}"
            )
            return -1
        return user.id


    def _cleanup(self):  # pragma: no cover
        """Cleanup data after loading."""
        pass
