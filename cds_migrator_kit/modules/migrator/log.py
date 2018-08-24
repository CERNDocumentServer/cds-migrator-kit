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
# 59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.
"""Define Books loggers."""
import json
import logging

from cds_dojson.marc21.fields.books.errors import ManualMigrationRequired, \
    MissingRequiredField, UnexpectedValue

from cds_migrator_kit.config import MIGRATION_LOG_FILE, MIGRATION_LOGS_PATH
from cds_migrator_kit.modules.migrator.errors import LossyConversion


def set_logging():
    """Sets additional logging to file for debug."""
    logger = logging.getLogger('migrator')
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - '
                                  '%(message)s - \n '
                                  '[in %(pathname)s:%(lineno)d]')
    fh = logging.FileHandler('migrator.log')
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)
    return logger


class JsonLogger(object):
    """Log migration statistic to file controller."""

    @staticmethod
    def get_stat_by_recid(recid, stats_json):
        """Search for existing stats of given recid."""
        return next(
            (item for item in stats_json if item['recid'] == recid), None)

    @staticmethod
    def render_stats():
        """Load stats from file as json."""
        try:
            with open(MIGRATION_LOG_FILE, 'r') as f:
                try:
                    all_stats = json.load(f)
                except ValueError as e:
                    all_stats = []
        except IOError:
            file = open(MIGRATION_LOG_FILE, 'w')
            all_stats = []
        return all_stats

    def create_output_file(self, recid, output):
        """Create json preview output file."""
        filename = '{0}{1}.json'.format(MIGRATION_LOGS_PATH, recid)
        with open(filename, 'w') as f:
            json.dump(output, f, indent=2)

    def add_log(self, exc, key=None, value=None, output=None):
        """Add exception log."""
        all_stats = JsonLogger.render_stats()
        with open(MIGRATION_LOG_FILE, 'w') as f:
            record_stats = JsonLogger.get_stat_by_recid(output['recid'],
                                                        all_stats)
            if not record_stats:
                record_stats = {'recid': output['recid'],
                                'manual_migration': [],
                                'unexpected_value': [],
                                'missing_required_field': [],
                                'lost_data': [],
                                'clean': False,
                                }
                all_stats.append(record_stats)
            JsonLogger.resolve_error_type(exc, record_stats, key, value)
            json.dump(all_stats, f, indent=2)

    def add_item(self, output):
        """Add empty log item."""
        all_stats = JsonLogger.render_stats()
        with open(MIGRATION_LOG_FILE, 'w') as f:
            record_stats = JsonLogger.get_stat_by_recid(output['recid'],
                                                        all_stats)
            if not record_stats:
                record_stats = {'recid': output['recid'],
                                'manual_migration': [],
                                'unexpected_value': [],
                                'missing_required_field': [],
                                'lost_data': [],
                                'clean': True,
                                }
            all_stats.append(record_stats)
            json.dump(all_stats, f, indent=2)

    @staticmethod
    def resolve_error_type(exc, rec_stats, key, value):
        """Check the type of exception and log to dict."""
        rec_stats['clean'] = False
        if isinstance(exc, ManualMigrationRequired):
            rec_stats['manual_migration'].append(key)
        elif isinstance(exc, UnexpectedValue):
            rec_stats['unexpected_value'].append((key, value))
        elif isinstance(exc, MissingRequiredField):
            rec_stats['missing_required_field'].append(key)
        elif isinstance(exc, LossyConversion):
            rec_stats['lost_data'] = list(exc.missing)
        else:
            raise exc
