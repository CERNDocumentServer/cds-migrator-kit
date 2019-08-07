# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS Migrator Records loggers."""

import json
import logging
import os

from cds_dojson.marc21.fields.books.errors import ManualMigrationRequired, \
    MissingRequiredField, UnexpectedValue
from flask import current_app

from cds_migrator_kit.records.errors import LossyConversion


def set_logging():
    """Sets additional logging to file for debug."""
    logger_migrator = logging.getLogger('migrator')
    logger_migrator.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - '
                                  '%(message)s - \n '
                                  '[in %(pathname)s:%(lineno)d]')
    fh = logging.FileHandler('migrator.log')
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)
    logger_migrator.addHandler(fh)
    logger_matcher = logging.getLogger('cds_dojson.matcher.dojson_matcher')
    logger_matcher.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - '
                                  '%(message)s - \n '
                                  '[in %(pathname)s:%(lineno)d]')
    fh = logging.FileHandler('matcher.log')
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)
    logger_matcher.addHandler(fh)

    return logger_migrator


logger = logging.getLogger('migrator')


class JsonLogger(object):
    """Log migration statistic to file controller."""

    def __init__(self):
        """Constructor."""
        self._logs_path = current_app.config['CDS_MIGRATOR_KIT_LOGS_PATH']

        self.LOG_FILEPATH = os.path.join(self._logs_path, 'stats.json')
        self.LOG_SERIALS = os.path.join(self._logs_path, 'serials.json')
        if not os.path.exists(self._logs_path):
            os.makedirs(self._logs_path)
        if not os.path.exists(self.LOG_FILEPATH):
            with open(self.LOG_FILEPATH, "w+") as f:
                json.dump([], f, indent=2)
        if not os.path.exists(self.LOG_SERIALS):
            with open(self.LOG_SERIALS, "w") as f:
                json.dump([], f, indent=2)

    @staticmethod
    def clean_stats_file():
        """Removes contents of the statistics file."""
        filepath = os.path.join(
            current_app.config['CDS_MIGRATOR_KIT_LOGS_PATH'], 'stats.json')
        with open(filepath, 'w') as f:
            f.write('[]')
            f.close()

    @staticmethod
    def get_stat_by_recid(recid, stats_json):
        """Search for existing stats of given recid."""
        return next(
            (item for item in stats_json if item['recid'] == recid), None)

    def render_stats(self):
        """Load stats from file as json."""
        logger.warning(self.LOG_FILEPATH)
        with open(self.LOG_FILEPATH, "r") as f:
            all_stats = json.load(f)
            return all_stats

    def create_output_file(self, file, output):
        """Create json preview output file."""
        try:
            filename = os.path.join(
                current_app.config['CDS_MIGRATOR_KIT_LOGS_PATH'],
                "{0}/{1}.json".format(output['_migration']['record_type'],
                                      file))
            with open(filename, "w+") as f:
                json.dump(output, f, indent=2)
        except Exception as e:
            raise e

    def add_recid_to_serial(self,  current_entry, similar_series, ratio):
        """Add record id to existing serial stats."""
        all_stats = JsonLogger().render_stats()
        with open(self.LOG_FILEPATH, "w+") as f:
            record_stats = JsonLogger.get_stat_by_recid(
                similar_series['recid'], all_stats)
            if ratio < 100:
                record_stats['similar_series'].append(current_entry['recid'])
            else:
                record_stats['exact_series'].append(current_entry['recid'])
            json.dump(all_stats, f, indent=2)

    def add_extracted_records(self, recid, index):
        """Add additionally extracted records from many series."""
        all_stats = JsonLogger().render_stats()
        with open(self.LOG_FILEPATH, "w+") as f:
            record_stats = JsonLogger.get_stat_by_recid(
                recid, all_stats)
            record_stats['extracted_records'].append(index)
            json.dump(all_stats, f, indent=2)

    def add_log(self, exc, key=None, value=None, output=None, rectype=None):
        """Add exception log."""
        all_stats = JsonLogger().render_stats()
        with open(self.LOG_FILEPATH, "w+") as f:
            record_stats = JsonLogger.get_stat_by_recid(output['recid'],
                                                        all_stats)
            if not record_stats:
                record_stats = {'recid': output['recid'],
                                'record_type': rectype,
                                'manual_migration': [],
                                'unexpected_value': [],
                                'missing_required_field': [],
                                'lost_data': [],
                                'clean': False,
                                'similar_series': [],
                                'exact_series': [],
                                'extracted_records': []
                                }
                all_stats.append(record_stats)
            self.resolve_error_type(exc, record_stats, key, value)
            json.dump(all_stats, f, indent=2)

    def add_item(self, output, rectype=None):
        """Add empty log item."""
        all_stats = JsonLogger().render_stats()
        with open(self.LOG_FILEPATH, "w+") as f:
            record_stats = JsonLogger.get_stat_by_recid(output['recid'],
                                                        all_stats)
            if not record_stats:
                record_stats = {'recid': output['recid'],
                                'record_type': rectype,
                                'manual_migration': [],
                                'unexpected_value': [],
                                'missing_required_field': [],
                                'lost_data': [],
                                'clean': True,
                                'similar_series': [],
                                'exact_series': [],
                                'extracted_records': [],
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
        elif isinstance(exc, KeyError):
            rec_stats['unexpected_value'].append(str(exc))
        elif isinstance(exc, TypeError) or isinstance(exc, AttributeError):
            rec_stats['unexpected_value'].append(
                "Model definition missing for this record."
                " Contact CDS team to tune the query")
        else:
            raise exc

    def add_related_child(self, stored_parent, rectype, related_recid):
        """Dumps recids picked up during migration in the output file."""
        if '_index' in stored_parent:
            filename = '{0}/{1}_{2}_{3}.json'.format(
                rectype, rectype, stored_parent['recid'],
                stored_parent['_index'])
        else:
            filename = '{0}/{1}_{2}.json'.format(rectype, rectype,
                                                 stored_parent['recid'])
        filepath = os.path.join(self._logs_path, filename)
        with open(filepath, 'r+') as file:
            parent = json.load(file)
            key_name = '_migration_relation_{0}_recids'.format(rectype)
            if key_name not in parent:
                parent[key_name] = []
            parent[key_name].append(related_recid)
            file.seek(0)
            file.truncate(0)
            json.dump(parent, file, indent=2)
