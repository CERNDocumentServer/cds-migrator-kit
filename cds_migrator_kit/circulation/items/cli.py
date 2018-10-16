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
from flask.cli import with_appcontext

from cds_migrator_kit.circulation.items.api import ItemsMigrator, \
    LibrariesMigrator

logger = logging.getLogger(__name__)


@click.group()
def circ_items():
    """CDS Migrator Circulation commands."""


@circ_items.command()
@click.argument('libraries_json', type=click.Path(exists=True))
@with_appcontext
def libraries(libraries_json):
    """Load libraries from JSON files and output ILS Records."""
    total_import_records = 0
    total_migrated_records = 0

    with open(libraries_json, 'r') as fp:
        libraries = json.load(fp)
        total_import_records = len(libraries)

    location, internal_locations = LibrariesMigrator(libraries).migrate()
    records = dict(location=location,
                   internal_locations=internal_locations)

    total_migrated_records = len(internal_locations) + 1  # 1 location

    filepath = os.path.join(
        current_app.config['CDS_MIGRATOR_KIT_LOGS_PATH'],
        'libraries.json'
    )
    with open(filepath, 'w') as fp:
        json.dump(records, fp, indent=2)

    _log = "Total number of migrated records: {0}/{1}".format(
        total_migrated_records, total_import_records)
    logger.info(_log)

    click.secho(_log, fg='green')


@circ_items.command()
@click.argument('items_json_folder', type=click.Path(exists=True))
@click.argument('locations_json', type=click.Path(exists=True))
@with_appcontext
def items(items_json_folder, locations_json):
    """Load items from JSON files.

    :param str items_json_folder: The path to the JSON dump of the legacy items
    :param str locations_json: The path to the JSON records of the new ILS
                libraries (already migrated)
    """
    output_filepath = os.path.join(
        current_app.config['CDS_MIGRATOR_KIT_LOGS_PATH'],
        'items_{0}.json'
    )

    with open(locations_json, 'r') as fp_locations:
        locations = json.load(fp_locations)
        internal_locations = locations['internal_locations']

    total_import_records = 0
    total_migrated_records = 0
    _files = glob.glob(os.path.join(items_json_folder, "*.json"))
    for i, items_json in enumerate(_files):
        _log = "Importing #{0} file".format(i)
        logger.info(_log)
        click.secho(_log, fg='yellow')

        with open(items_json, 'r') as fp_items:
            items = json.load(fp_items)
            total_import_records += len(items)

        records = ItemsMigrator(items, internal_locations).migrate()
        total_migrated_records += len(records)

        with open(output_filepath.format(i), 'w') as fp:
            json.dump(records, fp, indent=2)

    _log = "Total number of migrated records: {0}/{1}".format(
        total_migrated_records, total_import_records)
    logger.info(_log)

    click.secho(_log, fg='green')
