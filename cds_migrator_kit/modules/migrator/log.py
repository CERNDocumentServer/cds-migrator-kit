# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

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


logger = logging.getLogger('migrator')


class JsonLogger(object):
    """Log migration statistic to file controller."""

    @staticmethod
    def get_stat_by_recid(recid, stats_json):
        """Search for existing stats of given recid."""
        return next(
            (item for item in stats_json if item['recid'] == recid), None)

    def render_stats(self):
        """Load stats from file as json."""
        try:
            with open(MIGRATION_LOG_FILE, "r") as f:
                all_stats = json.load(f)
        except IOError as e:
            open(MIGRATION_LOG_FILE, "w+").close()
            all_stats = []
            logger.error(e.message)
        except ValueError as e:
            all_stats = []
            logger.error(e.message)
        return all_stats

    def create_output_file(self, recid, output):
        """Create json preview output file."""
        try:
            filename = "{0}{1}.json".format(MIGRATION_LOGS_PATH, recid)
            with open(filename, "w+") as f:
                json.dump(output, f, indent=2)
        except Exception as e:
            print e.message
            raise e

    def add_log(self, exc, key=None, value=None, output=None):
        """Add exception log."""
        all_stats = JsonLogger().render_stats()
        with open(MIGRATION_LOG_FILE, "w+") as f:
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
            self.resolve_error_type(exc, record_stats, key, value)
            json.dump(all_stats, f, indent=2)

    def add_item(self, output):
        """Add empty log item."""
        all_stats = JsonLogger().render_stats()
        with open(MIGRATION_LOG_FILE, "w+") as f:
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

    def resolve_error_type(self, exc, rec_stats, key, value):
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
