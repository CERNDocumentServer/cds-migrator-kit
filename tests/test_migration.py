# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS migration to CDSLabs tests."""

from tests.helpers import load_json

from cds_migrator_kit.records.records import CDSRecordDump


def test_migrate_record(datadir, base_app):
    """Test migrate date."""
    # [[ migrate the book ]]
    with base_app.app_context():
        data = load_json(datadir, 'book1.json')
        dump = CDSRecordDump(data=data[0])
        dump.prepare_revisions()
        res = dump.revisions[-1][1]
        assert res['recid'] == 262146
        assert res == {
            "agency_code": "SzGeCERN",
            "acquisition_source": {
                "datetime": "2001-03-19"
            },
            "number_of_pages": 465,
            "languages": [
                "en"
            ],
            "_access": {
                "read": []
            },
            "titles": [
                {
                    "title": "Gauge fields, knots and gravity"
                }
            ],
            "recid": 262146,
            "isbns": [
                {
                    "value": "9789810217297"
                },
                {
                    "value": "9789810220341"
                },
                {
                    "value": "9810217293"
                },
                {
                    "value": "9810220340"
                }
            ],
            "authors": [
                {
                    "full_name": "Baez, John C"
                },
                {
                    "full_name": "Muniain, Javier P"
                }
            ],
            "keywords": [
                {
                    "source": "CERN",
                    "name": "electromagnetism"
                },
                {
                    "source": "CERN",
                    "name": "gauge fields"
                },
                {
                    "source": "CERN",
                    "name": "general relativity"
                },
                {
                    "source": "CERN",
                    "name": "knot theory, applications"
                },
                {
                    "source": "CERN",
                    "name": "quantum gravity"
                }
            ],
            "_private_notes": [
                {
                    "value": "newqudc"
                }
            ],
            "$schema": {
                "$ref": "records/books/book/book-v.0.0.1.json"
            },
            "document_type": [
                "BOOK"
            ],
            "imprints": [
                {
                    "date": "1994",
                    "publisher": "World Scientific",
                    "place": "Singapore"
                }
            ]
        }
