# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS Migrator Circulation module."""

import click
from flask.cli import with_appcontext
from .items.cli import libraries, items
from .users.cli import users


@click.group()
def circulation():
    """CDS Migrator Circulation commands."""


@circulation.command()
@click.argument('users_json', type=click.Path(exists=True))
@with_appcontext
def borrowers(users_json):
    """Load users from JSON files and output ILS Records."""
    users(users_json)


@circulation.command()
@click.argument('libraries_json', type=click.Path(exists=True))
@with_appcontext
def libraries(libraries_json):
    """Load libraries from JSON files and output ILS Records."""
    libraries(libraries_json)


@circulation.command()
@click.argument('items_json_folder', type=click.Path(exists=True))
@click.argument('locations_json', type=click.Path(exists=True))
@with_appcontext
def items(items_json_folder, locations_json):
    """Load items from JSON files.

    :param str items_json_folder: The path to the JSON dump of the legacy items
    :param str locations_json: The path to the JSON records of the new ILS
                libraries (already migrated)
    """
    items(items_json_folder, locations_json)
