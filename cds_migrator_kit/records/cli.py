# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS Migrator Records CLI."""

import json
import logging

import click
from cds_ils.importer.providers.cds.models.journal import \
    model as journal_model
from cds_ils.importer.providers.cds.models.multipart import \
    model as multipart_model
from cds_ils.importer.providers.cds.models.serial import model as serial_model
from flask import current_app
from flask.cli import with_appcontext

from cds_migrator_kit.records.validators import record_validator

from .errors import LossyConversion, RequiredFieldMissing
from .log import JsonLogger
from .records import CDSRecordDump

cli_logger = logging.getLogger(__name__)


def load_records(sources, source_type, eager, rectype=None, **params):
    """Load records."""
    logger = JsonLogger.get_json_logger(rectype)

    for idx, source in enumerate(sources, 1):
        click.secho('Loading dump {0} of {1} ({2})'.format(
            idx, len(sources), source), fg='yellow')
        content = None
        with open(source.name, 'r+') as file:
            content = file.read().encode('UTF-8')
        with open(source.name, 'wb') as file:
            file.write(content)
            file.close()
        data = json.load(source)
        source.close()
        with click.progressbar(data) as records:
            for item in records:
                dump = CDSRecordDump(data=item, logger=logger, **params)
                click.echo('Processing item {0}...'.format(item['recid']))
                logger.add_recid_to_stats(item['recid'])
                try:
                    dump.prepare_revisions()
                    record_validator(dump.revisions[-1][1], rectype=rectype)
                    logger.add_record(dump.revisions[-1][1])
                except LossyConversion as e:
                    cli_logger.error('[DATA ERROR]: {0}'.format(e.message))
                    logger.add_log(e, output=item)
                # except AttributeError as e:
                #     current_app.logger.error('Model missing')
                #     JsonLogger().add_log(e, output=item, rectype=rectype)
                #     raise e
                # except TypeError as e:
                #     current_app.logger.error(
                #         'Model missing recid:{}'.format(item['recid']))
                #     JsonLogger().add_log(e, output=item, rectype=rectype)
                #     raise e
                # except KeyError as e:
                #     current_app.logger.error(
                #         'Model missing recid:{}'.format(item['recid']))
                #     JsonLogger().add_log(e, output=item, rectype=rectype)
                except RequiredFieldMissing as e:
                    cli_logger.error(e)
                    current_app.logger.error(e)
                    logger.add_log(e, output=item)
                except Exception as e:
                    cli_logger.error(e)
                    current_app.logger.error(e)
                    logger.add_log(e, output=item)
                    raise e
        logger.save()
        click.secho('Check completed. See the report on: '
                    'books-migrator-dev.web.cern.ch/results', fg='green')


@click.group()
def report():
    """CDS Migrator report commands."""


@report.command()
@click.argument('sources', type=click.File('r', encoding='UTF-8',
                                           errors='replace'), nargs=-1)
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
@click.option(
    '--rectype',
    '-x',
    help='Type of record to load (f.e serial).',
    default='document')
@with_appcontext
def dryrun(sources, source_type, recid, rectype, model=None):
    """Load records migration dump."""
    params = {}
    if rectype == 'multipart':
        params['dojson_model'] = multipart_model
    elif rectype == 'serial':
        params['dojson_model'] = serial_model
    elif rectype == 'journal':
        params['dojson_model'] = journal_model
    elif rectype == 'document':
        # use default model
        pass
    else:
        raise ValueError('invalid rectype: {}'.format(rectype))
    load_records(sources=sources, source_type=source_type, eager=True,
                 rectype=rectype, **params)
