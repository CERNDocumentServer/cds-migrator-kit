# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS Migrator Circulation API."""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class UserMigrator():
    """Migrate legacy circulation users to Invenio ILS records.

    Expected input format for circulation users:
        [
          ...,
          {
            'id': ,
            'name': ,
            'email': ,
            'ccid': ,
            'borrower_since':
          },
          ...,
        ]
    """

    def __init__(self, circulation_users):
        """Constructor."""
        self.circulation_users = circulation_users

    def migrate_user_identity(self, user):
        """Return new user identity entry."""
        return {
            'id': user['uid'],
            'method': 'cern',
            'id_user': user['id'],
        }

    def migrate_user(self, user):
        """Return new user entry."""
        return {
            'id': user['id'],
            'email': user['email'],
            'active': True,
        }

    def migrate_remote_account(self, user):
        """Return new user entry."""
        return {
            'user_id': user['id'],
            'extra_data': {
                'person_id': user['ccid'],
                'department': user['department']
            }
        }

    def migrate_user_profile(self, user):
        """Return new user profile."""
        return {
            'user_id': user['id'],
            '_displayname': 'id_' + str(user['id']),
            'full_name': user['name'],
        }

    def migrate(self):
        """Return location and internal location records."""
        user_identities = []
        users = []
        users_profiles = []
        remote_accounts = []

        for borrower in self.circulation_users:
            user_identities.append(self.migrate_user_identity(borrower))
            users.append(self.migrate_user(borrower))
            users_profiles.append(self.migrate_user_profile(borrower))
            remote_accounts.append(self.migrate_remote_account(borrower))
        return users, users_profiles, user_identities, remote_accounts
