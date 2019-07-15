# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS Migrator Records utils."""
import copy
import json
import os

from flask import current_app
from fuzzywuzzy import fuzz


def process_fireroles(fireroles):
    """Extract firerole definitions."""
    rigths = set()
    for firerole in fireroles:
        for (allow, not_, field, expressions_list) in firerole[1]:
            if not allow:
                current_app.logger.warning(
                    'Not possible to migrate deny rules: {0}.'.format(
                        expressions_list))
                continue
            if not_:
                current_app.logger.warning(
                    'Not possible to migrate not rules: {0}.'.format(
                        expressions_list))
                continue
            if field in ('remote_ip', 'until', 'from'):
                current_app.logger.warning(
                    'Not possible to migrate {0} rule: {1}.'.format(
                        field, expressions_list))
                continue
            # We only deal with allow group rules
            for reg, expr in expressions_list:
                if reg:
                    current_app.logger.warning(
                        'Not possible to migrate groups based on regular'
                        ' expressions: {0}.'.format(expr))
                    continue
                clean_name = expr[
                    :-len(' [CERN]')].lower().strip().replace(' ', '-')
                rigths.add('{0}@cern.ch'.format(clean_name))
    return rigths


def update_access(data, *access):
    """Merge access rights information.

    :params data: current JSON structure with metadata and potentially an
        `_access` key.
    :param *access: List of dictionaries to merge to the original data, each of
        them in the form `action: []`.
    """
    current_rules = data.get('_access', {})
    for a in access:
        for k, v in a.items():
            current_x_rules = set(current_rules.get(k, []))
            current_x_rules.update(v)
            current_rules[k] = list(current_x_rules)

    data['_access'] = current_rules


def prepare_serials(serial, logger, rectype, item):
    """Prepare serials records to migrate."""
    return split_serials(serial, logger, rectype, item)


def check_for_duplicated_serials(serial):
    """Check if the serial already exists."""
    _logs_path = current_app.config['CDS_MIGRATOR_KIT_LOGS_PATH']
    filepath = os.path.join(_logs_path, 'serials.json')
    with open(filepath, 'r+') as file:
        all_serials = json.load(file)
        for stored_serial in all_serials:
            ratio = fuzz.ratio(serial['title']['title'],
                               stored_serial['title']['title'])
            if serial['title']['title'] == stored_serial['title']['title'] or \
                ('issn' in serial and serial['issn'] is not None and
                    serial['issn'] == stored_serial['issn']):
                return stored_serial, 100

            elif 95 <= ratio < 100:
                return stored_serial, ratio
        else:
            all_serials.append(
                        {'title': serial['title'],
                         'recid': serial['recid'],
                         'issn': serial['issn']
                         if 'issn' in serial else None,
                         }
                    )
            file.seek(0)
            file.truncate(0)
            if all_serials:
                json.dump(all_serials, file, indent=2)
        if not all_serials:
            all_serials.append(
                {'title': serial['title'],
                 'recid': serial['recid'],
                 'issn': serial['issn'] if 'issn' in serial else None,
                 }
            )
            file.seek(0)
            file.truncate(0)
            if all_serials:
                json.dump(all_serials, file, indent=2)


def split_serials(serial, logger, rectype, item):
    """Extract multiple serials if many in the record MARC."""
    if len(serial['title']) > 1:
        for i, title in enumerate(serial['title']):
            split_serial = copy.deepcopy(serial)
            split_serial['title'] = title
            out = check_for_duplicated_serials(split_serial)

            if out:
                logger.add_recid_to_serial(split_serial, *out)
            else:
                logger.add_extracted_records(split_serial['recid'], i)
                logger.create_output_file(
                    '{0}_{1}_{2}'.format(rectype, item['recid'], i),
                    split_serial)
        return i
    else:
        serial['title'] = serial['title'][0]
        out = check_for_duplicated_serials(serial)
        if out:
            logger.add_recid_to_serial(serial, *out)
        else:
            logger.create_output_file(
                '{0}_{1}'.format(rectype, item['recid']),
                serial)
        return 1
