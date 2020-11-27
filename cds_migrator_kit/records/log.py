# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS Migrator Records loggers."""

import copy
import json
import logging
import os

from cds_ils.importer.errors import ManualImportRequired, \
    MissingRequiredField, UnexpectedValue
from flask import current_app
from fuzzywuzzy import fuzz

from cds_migrator_kit.records.errors import LossyConversion, \
    RequiredFieldMissing
from cds_migrator_kit.records.utils import clean_exception_message, \
    compare_titles, same_issn


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

    LOG_FILEPATH = None

    @classmethod
    def get_json_logger(cls, rectype):
        """Get JsonLogger instance based on the rectype."""
        if rectype == 'serial':
            return SerialJsonLogger()
        elif rectype == 'journal':
            return JournalJsonLogger()
        elif rectype == 'document':
            return DocumentJsonLogger()
        elif rectype == 'multipart':
            return MultipartJsonLogger()
        else:
            raise Exception('Invalid rectype: {}'.format(rectype))

    def __init__(self, stats_filename, records_filename):
        """Constructor."""
        self._logs_path = current_app.config['CDS_MIGRATOR_KIT_LOGS_PATH']
        self.stats = {}
        self.records = {}
        self.STAT_FILEPATH = os.path.join(self._logs_path, stats_filename)
        self.RECORD_FILEPATH = os.path.join(self._logs_path, records_filename)

        if not os.path.exists(self._logs_path):
            os.makedirs(self._logs_path)

    def load(self):
        """Load stats from file as json."""
        logger.warning(self.STAT_FILEPATH)
        with open(self.STAT_FILEPATH, "r") as f:
            self.stats = json.load(f)
        with open(self.RECORD_FILEPATH, "r") as f:
            self.records = json.load(f)

    def save(self):
        """Save stats from file as json."""
        logger.warning(self.STAT_FILEPATH)
        with open(self.STAT_FILEPATH, "w") as f:
            json.dump(self.stats, f)
        with open(self.RECORD_FILEPATH, "w") as f:
            json.dump(self.records, f)

    def add_recid_to_stats(self, recid, **kwargs):
        """Add recid to stats."""
        pass

    def add_record(self, record, **kwargs):
        """Add record to list of collected records."""
        pass

    def add_log(self, exc, key=None, value=None, output=None):
        """Add exception log."""
        self.resolve_error_type(exc, output, key, value)

    def resolve_error_type(self, exc, output, key, value):
        """Check the type of exception and log to dict."""
        recid = output.get('recid', None) or output['legacy_recid']
        rec_stats = self.stats[recid]
        rec_stats['clean'] = False
        if isinstance(exc, ManualImportRequired):
            rec_stats['manual_migration'].append(dict(
                key=key,
                value=value,
                subfield=exc.subfield,
                message=clean_exception_message(exc.message)
            ))
        elif isinstance(exc, UnexpectedValue):
            rec_stats['unexpected_value'].append(dict(
                key=key,
                value=value,
                subfield=exc.subfield,
                message=clean_exception_message(exc.message)
            ))
        elif isinstance(exc, MissingRequiredField):
            rec_stats['missing_required_field'].append(dict(
                key=key,
                value=value,
                subfield=exc.subfield,
                message=clean_exception_message(exc.message)
            ))
        elif isinstance(exc, LossyConversion):
            rec_stats['lost_data'].append(dict(
                key=key,
                value=value,
                missing=list(exc.missing),
                message=exc.message
            ))
        elif isinstance(exc, RequiredFieldMissing):
            rec_stats['missing_required_field'].append(dict(
                value=exc.value,
                subfield=exc.subfield,
                missing=list(exc.missing),
                message=exc.message
            ))
        elif isinstance(exc, KeyError):
            rec_stats['unexpected_value'].append(str(exc))
        elif isinstance(exc, TypeError) or isinstance(exc, AttributeError):
            rec_stats['unexpected_value'].append(
                "Model definition missing for this record."
                " Contact CDS team to tune the query")
        else:
            raise exc


class DocumentJsonLogger(JsonLogger):
    """Log document migration statistic to file controller."""

    def __init__(self):
        """Constructor."""
        super().__init__('document_stats.json', 'document_records.json')

    def add_recid_to_stats(self, recid):
        """Add empty log item."""
        if recid not in self.stats:
            self.stats[recid] = {
                'recid': recid,
                'manual_migration': [],
                'unexpected_value': [],
                'missing_required_field': [],
                'lost_data': [],
                'clean': True,
            }

    def add_record(self, record):
        """Add record to collected records."""
        self.records[record['legacy_recid']] = record


class JournalJsonLogger(JsonLogger):
    """Log document migration statistic to file controller."""

    def __init__(self):
        """Constructor."""
        super().__init__('journal_stats.json', 'journal_records.json')

    def add_recid_to_stats(self, recid):
        """Add empty log item."""
        if recid not in self.stats:
            self.stats[recid] = {
                'recid': recid,
                'manual_migration': [],
                'unexpected_value': [],
                'missing_required_field': [],
                'lost_data': [],
                'clean': True,
            }

    def add_record(self, record):
        """Add record to collected records."""
        self.records[record['legacy_recid']] = record


class MultipartJsonLogger(JsonLogger):
    """Log multipart statistics to file."""

    def __init__(self):
        """Constructor."""
        super().__init__('multipart_stats.json', 'multipart_records.json')
        self.document_pid = 0

    def add_log(self, exc, key=None, value=None, output=None):
        """Add exception log."""
        self.resolve_error_type(exc, output, key, value)

    def next_doc_pid(self):
        """Get the next available fake doc pid."""
        self.document_pid += 1
        return self.document_pid

    def add_recid_to_stats(self, recid):
        """Add recid to stats."""
        if recid not in self.stats:
            self.stats[recid] = {
                'recid': recid,
                'manual_migration': [],
                'unexpected_value': [],
                'missing_required_field': [],
                'lost_data': [],
                'volumes': [],
                'volumes_found': [],
                'volumes_expected': 0,
                'clean': True,
            }

    def add_record(self, record):
        """Add log record."""
        recid = record['legacy_recid']
        self.records[recid] = record
        if 'volumes' in record['_migration']:
            for volume in record['_migration']['volumes']:
                if 'title' in volume:
                    self.stats[recid]['volumes'].append(volume)
                    found = self.stats[recid]['volumes_found']
                    if volume['volume'] not in found:
                        found.append(volume['volume'])

        self.stats[recid]['volumes_expected'] = \
            record.get('number_of_volumes', '-')


class SerialJsonLogger(JsonLogger):
    """Log migration statistic to file controller."""

    def __init__(self):
        """Constructor."""
        super().__init__('serial_stats.json', 'serial_records.json')

    def add_log(self, exc, key=None, value=None, output=None):
        """Add exception log."""
        pass

    def _add_to_stats(self, record):
        """Update serial stats."""
        title = record['title']
        if title in self.stats:
            self.stats[title]['documents'].append(record['legacy_recid'])
        else:
            self.stats[title] = {
                'title': title,
                'issn': record.get('issn', None),
                'documents': [record['legacy_recid']],
                'similars': {
                    'same_issn': [],
                    'similar_title': [],
                }
            }

    def _add_to_record(self, record):
        """Update serial record."""
        del record['legacy_recid']
        title = record['title']
        self.records[title] = record

    def add_record(self, record):
        """Add serial to collected records."""
        if 'title' not in record:
            record['title'] = ['FIXME - serial has invalid title - field 490']

        title = record['title']
        if len(title) > 1:
            for title in record['title']:
                new_record = copy.deepcopy(record)
                new_record['title'] = title
                self._add_to_stats(new_record)
                self._add_to_record(new_record)
        else:
            record['title'] = record['title'][0]
            self._add_to_stats(record)
            self._add_to_record(record)

    def _add_children(self):
        """Add children to collected record."""
        for record in self.records.values():
            record['_migration']['children'] = \
                self.stats[record['title']]['documents']

    def _match_similar(self):
        """Match similar serials."""
        items = self.stats.items()
        for title1, stat1 in items:
            for title2, stat2 in items:
                if title1 == title2:
                    continue
                if same_issn(stat1, stat2):
                    if title2 not in stat1['similars']['same_issn']:
                        stat1['similars']['same_issn'].append(title2)
                    if title1 not in stat2['similars']['same_issn']:
                        stat2['similars']['same_issn'].append(title1)
                else:
                    ratio = compare_titles(title1, title2)
                    if 95 <= ratio < 100:
                        if title2 not in stat1['similars']['similar_title']:
                            stat1['similars']['similar_title'].append(title2)
                        if title1 not in stat2['similars']['similar_title']:
                            stat2['similars']['similar_title'].append(title1)

    def save(self):
        """Save serials and update children and simliar matches."""
        self._add_children()
        self._match_similar()
        super().save()
