# -*- coding: utf-8 -*-
#
# This file is part of CERN Document Server.
# Copyright (C) 2017 CERN.
#
# Invenio is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the
# License, or (at your option) any later version.
#
# Invenio is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Invenio; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA 02D111-1307, USA.
"""CDS Books exceptions handlers."""
import json
import logging
import os

import click
from flask.cli import with_appcontext
from pathlib import Path

from cds_migrator_kit.config import MIGRATION_LOG_FILE
from cds_migrator_kit.modules.migrator.errors import LossyConversion
from cds_migrator_kit.modules.migrator.log import JsonLogger
from cds_migrator_kit.modules.migrator.records import CDSRecordDump

cli_logger = logging.getLogger('migrator')


def load_records(sources, source_type, eager):
    """Load records."""
    for idx, source in enumerate(sources, 1):
        # source = '{0}{1}'.format(MIGRATION_DUMP_PATH, source)
        click.secho('Loading dump {0} of {1} ({2})'.format(
            idx, len(sources), source), fg='yellow')
        data = json.load(source)
        with click.progressbar(data) as records:
            for item in records:
                dump = CDSRecordDump(data=item)
                JsonLogger().add_item(item)
                try:
                    dump.prepare_revisions()
                    JsonLogger().create_output_file(item['recid'],
                                                    dump.revisions[-1][1])
                except LossyConversion as e:
                    cli_logger.error('[DATA ERROR]: {0}'.format(e.message))
                    JsonLogger().add_log(e, output=item)
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
@with_appcontext
def dryrun(sources, source_type, recid):
    """Load records migration dump."""
    # if os.path.exists(MIGRATION_LOG_FILE):
    #     os.remove(MIGRATION_LOG_FILE)
    #     f = Path(MIGRATION_LOG_FILE)
    #     f.touch(exist_ok=True)
    load_records(sources=sources, source_type=source_type, eager=True)
