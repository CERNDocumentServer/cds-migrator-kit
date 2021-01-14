# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS migration to CDSLabs tests."""

from cds_migrator_kit.records.records import CDSRecordDump
from tests.helpers import load_json


def test_migrate_record(datadir, base_app):
    """Test migrate date."""
    # [[ migrate the book ]]
    with base_app.app_context():
        data = load_json(datadir, 'book1.json')
        dump = CDSRecordDump(data=data[0])
        dump.prepare_revisions()
        res = dump.revisions[-1][1]
        assert res['legacy_recid'] == 262146
        assert res == {
            "agency_code": "SzGeCERN",
            '_created': '2001-03-19',
            'created_by': {'type': 'user'},
            "number_of_pages": "465",
            "languages": [
                "EN"
            ],
            "title": "Gauge fields, knots and gravity",
            "legacy_recid": 262146,
            "provider_recid": "262146",
            'publication_year': "1994",
            "identifiers": [
                {
                    "scheme": "ISBN",
                    "value": "9789810217297"
                },
                {
                    "scheme": "ISBN",
                    "value": "9789810220341"
                },
                {
                    "scheme": "ISBN",
                    "value": "9810217293"
                },
                {
                    "scheme": "ISBN",
                    "value": "9810220340"
                }
            ],
            "authors": [
                {
                    "full_name": "Baez, John C",
                    "roles": ["AUTHOR"]
                },
                {
                    "full_name": "Muniain, Javier P",
                    "roles": ["AUTHOR"]
                }
            ],
            "keywords": [
                {
                    "source": "CERN",
                    "value": "electromagnetism"
                },
                {
                    "source": "CERN",
                    "value": "gauge fields"
                },
                {
                    "source": "CERN",
                    "value": "general relativity"
                },
                {
                    "source": "CERN",
                    "value": "knot theory, applications"
                },
                {
                    "source": "CERN",
                    "value": "quantum gravity"
                }
            ],
            "internal_notes": [
                {
                    "value": "newqudc"
                }
            ],
            "document_type": 'BOOK',
            "imprint":
                {
                    "date": "1994-01-14",
                    "publisher": "World Scientific",
                    "place": "Singapore"
                },

            '_migration': {'has_tags': False,
                           'has_related': False,
                           'has_serial': False,
                           'has_journal': False,
                           'eitems_ebl': [],
                           'eitems_external': [],
                           'eitems_file_links': [],
                           'eitems_has_ebl': False,
                           'eitems_has_external': False,
                           'eitems_has_files': False,
                           'eitems_has_proxy': False,
                           'eitems_open_access': False,
                           'eitems_proxy': [],
                           'electronic_items': [],
                           'is_multipart': False,
                           'journal_record_legacy_id': '',
                           'record_type': 'document',
                           'volumes': [],
                           'serials': [],
                           'tags': [],
                           'related': [],
                           'relation_next': None,
                           'relation_previous': None,
                           'volumes_identifiers': [],
                           'volumes_urls': [],
                           'items': [],
                           },

        }
