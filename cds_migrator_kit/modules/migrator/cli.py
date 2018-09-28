
# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS Books exceptions handlers."""
import json
import logging

import click

from cds_migrator_kit.config import MIGRATION_LOG_FILE
from cds_migrator_kit.modules.migrator.errors import LossyConversion
from cds_migrator_kit.modules.migrator.log import JsonLogger
from cds_migrator_kit.modules.migrator.records import CDSRecordDump

cli_logger = logging.getLogger('migrator')


def load_records(sources, source_type, eager):
    """Load records."""
    logger = JsonLogger()
    for idx, source in enumerate(sources, 1):
        click.secho('Loading dump {0} of {1} ({2})'.format(
            idx, len(sources), source), fg='yellow')
        data = json.load(source)
        source.close()
        with click.progressbar(data) as records:
            for item in records:
                dump = CDSRecordDump(data=item)
                logger.add_item(item)
                try:
                    dump.prepare_revisions()
                    logger.create_output_file(item['recid'],
                                              dump.revisions[-1][1])
                except LossyConversion as e:
                    cli_logger.error('[DATA ERROR]: {0}'.format(e.message))
                    JsonLogger().add_log(e, output=item)
                except Exception as e:
                    cli_logger.warning(e.message)
                    raise e
        click.secho('Check completed. See the report on: '
                    'books-migrator-dev.web.cern.ch/results', fg='green')


@click.group()
def report():
    """Create initial data and demo records."""


@report.command()
@click.argument('sources', type=click.File('r'), nargs=-1)
@click.option(
    '--source-type',
    '-t',
    type=click.Choice(['json', 'marcxml']),
    default='marcxml',
    help='Whether to use JSON or MARCXML.')
@click.option(
    '--recid',
    '-r',
    help='Record ID to load (NOTE: will load only one record!).',
    default=None)
# @with_appcontext
def dryrun(sources, source_type, recid):
    """Load records migration dump."""
    # if os.path.exists(MIGRATION_LOG_FILE):
    #     os.remove(MIGRATION_LOG_FILE)
    #     f = Path(MIGRATION_LOG_FILE)
    #     f.touch(exist_ok=True)
    with open(MIGRATION_LOG_FILE, "w+") as f:
        json.dump([], f)
    load_records(sources=sources, source_type=source_type, eager=True)
