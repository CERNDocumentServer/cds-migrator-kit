# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS Migrator Circulation API."""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class LibrariesMigrator():
    """Migrate legacy libraries to Invenio ILS records.

    Expected input format for legacy libraries:
        [
          ...,
          {
            'id': ,
            'name': ,
            'address': ,
            'email': ,
            'phone': ,
            'type': ,
            'notes': ,
          },
          ...,
        ]

    """

    IGNORE_LIBRARY_IDS = [
        43,  # name Other
    ]
    VALID_LIBRARY_TYPES = ['main', 'internal']

    def __init__(self, libraries):
        """Constructor."""
        self.libraries = [l for l in libraries
                          if l['type'] in self.VALID_LIBRARY_TYPES and
                          l['id'] not in self.IGNORE_LIBRARY_IDS]

    def _migrate_internal_locations(self, location_pid):
        """Return new internal location records."""
        internal_locations = []
        for i, library in enumerate(self.libraries):
            internal_library = {
                'internal_location_pid': "{}".format(i+1),
                'location_pid': "{}".format(location_pid),
                'legacy_id': "{}".format(library['id']),
                'name': library['name'],
                'physical_location': library.get('address', ''),
                'notes': library.get('notes', ''),
            }
            internal_locations.append(internal_library)
        return internal_locations

    def migrate(self):
        """Return location and internal location records."""
        location_pid = '1'
        internal_locations = self._migrate_internal_locations(location_pid)

        location = {
            'location_pid': '1',
            'name': 'CERN Central Library',
            'address': 'CH-1211 Geneva 23',
            'email': 'library.desk@cern.ch',
        }

        return location, internal_locations


class ItemsMigrator():
    """Migrate legacy items to Invenio ILS records.

    Expected input format for legacy items:
        [
          ...,
          {
            'barcode': ,
            'id_bibrec': ,
            'id_crcLIBRARY': ,
            'collection: ,
            'location': ,
            'description': ,
            'loan_period': ,
            'status': ,
            'expected_arrival_date': ,
            'creation_date': ,
            'modification_date': ,
            'number_of_requests': ,
          },
          ...,
        ]

    """

    ITEM_STATUSES = ['LOANABLE', 'MISSING', 'IN_BINDING', 'SCANNING']
    RESTRICTIONS = ['FOR_REFERENCE_ONLY']
    MEDIUMS = ['NOT_SPECIFIED', 'ONLINE', 'PAPER', 'CDROM', 'DVD', 'VHS']

    def __init__(self, items, internal_locations):
        """Constructor."""
        self.items = items
        # map of legacy library id with new PID
        self.internal_locations = dict()
        for il in internal_locations:
            pid = il['internal_location_pid']
            self.internal_locations[il['legacy_id']] = pid

    def _transform_status(self, item):
        """Return the new record status."""
        legacy_status = item['status'].lower()
        if legacy_status in ['on shelf', 'on loan']:
            new_status = self.ITEM_STATUSES[0]
        elif legacy_status in ['missing', 'untraceable']:
            new_status = self.ITEM_STATUSES[1]
        elif legacy_status == 'in binding':
            new_status = self.ITEM_STATUSES[2]
        else:
            raise ValueError

        return new_status

    def _transform_loan_period(self, item):
        """Return the new record circulation restriction."""
        legacy_restriction = item['loan_period'].lower()
        if legacy_restriction == 'reference':
            new_restriction = self.RESTRICTIONS[0]
        elif legacy_restriction in ['4 weeks', '1 week']:
            new_restriction = None
        else:
            raise ValueError
        return new_restriction

    def _clean_date(self, legacy_date, item):
        """Return the new record date format."""
        # old: 2016-01-29T17:28:17
        # new: "2018-05-16T12:34:28.233187+00:00"
        if not legacy_date:
            _log = "`creation_date` or `modification_date` is None for item " \
                   "with `barcode={barcode}`. Setting it to now().".format(
                    barcode=item['barcode'])
            logger.warning(_log)
            print(_log)
            d = datetime.now()
        else:
            d = datetime.strptime(legacy_date, '%Y-%m-%dT%H:%M:%S')
        return d.isoformat()

    def migrate(self):
        """Return items."""
        records = []
        for i, item in enumerate(self.items):
            # check barcode
            if not item['barcode']:
                _log = "Item with `id_bibrec = {id_bibrec}` not imported " \
                       "because `barcode is None`".format(
                        id_bibrec=item['id_bibrec'])
                logger.error(_log)
                print(_log)
                continue

            # library id
            if item['id_crcLIBRARY'] not in self.internal_locations:
                _log = "Item with `barcode = {barcode}` not imported because" \
                       "`library_id={libid}` not found in imported " \
                       "libraries".format(
                        barcode=item['barcode'], libid=item['id_crcLIBRARY'])
                logger.error(_log)
                print(_log)
                continue
            ilocid = self.internal_locations[item['id_crcLIBRARY']]

            # status
            try:
                status = self._transform_status(item)
            except ValueError:
                _log = "Item with `barcode = {barcode}` not imported because" \
                       "`status={status}` not recognized".format(
                        barcode=item['barcode'], status=item['status'])
                logger.error(_log)
                print(_log)
                continue

            # circulation_restriction
            try:
                circulation_restriction = self._transform_loan_period(item)
            except ValueError:
                _log = "Item with `barcode = {barcode}` not imported because" \
                       " `loan_period={loan_period}` not recognized".format(
                        barcode=item['barcode'],
                        loan_period=item['loan_period'])
                logger.error(_log)
                print(_log)
                continue

            try:
                created = self._clean_date(item['creation_date'], item)
            except ValueError:
                created = None

            try:
                updated = self._clean_date(item['modification_date'], item)
            except ValueError:
                updated = None

            record = {
                "item_pid": "{}".format(i+1),
                "document_pid": "to be set",
                "internal_location_pid": "{}".format(ilocid),
                "legacy_id": "{}".format(item['id_bibrec']),
                "legacy_library_id": "{}".format(item['id_crcLIBRARY']),
                "barcode": item['barcode'],
                "shelf": item['location'],
                "description": item['description'],
                "status": status,
                "circulation_restriction": circulation_restriction,
                "medium": "",
                "created": created,
                "updated": updated
            }
            records.append(record)

        return records
