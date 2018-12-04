# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS Migrator Circulation Items CLI."""
import glob
import json
import logging
import os

import click
from flask import current_app

from cds_migrator_kit.circulation.users.api import UserMigrator

logger = logging.getLogger(__name__)


def users(users_json):
    """Load users from JSON files and import in db."""
    from invenio_accounts.models import User
    from invenio_db import db
    from invenio_oauthclient.models import RemoteAccount, UserIdentity
    from invenio_userprofiles.models import UserProfile

    def _import_users(users, users_identities, users_profiles,
                      remote_accounts):
        """Import users in db."""
        click.secho('Migrating {0} users'.format(len(users)), fg='green')
        for user in users:
            user = User(**user)
            db.session.add(user)

        click.secho('Migrating {0} user identities'.format(
            len(users_identities)), fg='green')

        for identity in users_identities:
            user_identity = UserIdentity(**identity)
            db.session.add(user_identity)

        click.secho('Migrating {0} user profiles'.format(
            len(users_profiles)), fg='green')
        for profile in users_profiles:
            user_profile = UserProfile(**profile)
            db.session.add(user_profile)

        click.secho('Migrating {0} remote accoutns'.format(
            len(remote_accounts)), fg='green')
        client_id = current_app.config['CERN_APP_CREDENTIALS']['consumer_key']
        for account in remote_accounts:
            remote_account = RemoteAccount(client_id=client_id, **account)
            db.session.add(remote_account)

        db.session.commit()

    click.secho(users_json, fg='green')
    with open(users_json, 'r') as fp:
        users = json.load(fp)
        total_import_records = len(users)

    migrator = UserMigrator(users)
    users, profiles, identities, remote_accounts = migrator.migrate()

    # Import users in db
    _import_users(users, identities, profiles, remote_accounts)
