# -*- coding: utf-8 -*-
#
# Copyright (C) 2024-2025 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM transform user."""
import csv
import json
from copy import deepcopy

from cds_migrator_kit.users.api import MigrationUserAPI

from invenio_db import db
from invenio_accounts.models import User, UserIdentity
from invenio_cern_sync.sso import cern_remote_app_name
from psycopg.errors import UniqueViolation
from sqlalchemy.exc import IntegrityError

from cds_migrator_kit.transform.dumper import CDSRecordDump



class CDSMigrationUserAPI(MigrationUserAPI):
    """CDS missing user load class."""

    def __init__(self, remote_account_client_id=None):
        """Constructor."""
        super().__init__(remote_account_client_id)

    def create_invenio_user_identity(self, user_id, person_id):
        """Return new user identity entry."""
        try:
            return UserIdentity(
                id=person_id,
                method=cern_remote_app_name,
                id_user=user_id,
            )
        except (IntegrityError, UniqueViolation) as e:
            db.session.rollback()
            user_identity = UserIdentity(
                id=f"duplicate{person_id}",
                method=cern_remote_app_name,
                id_user=user_id,
            )
            db.session.add(user_identity)
            db.session.commit()
            return user_identity

