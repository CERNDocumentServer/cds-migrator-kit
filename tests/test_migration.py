# -*- coding: utf-8 -*-
#
# This file is part of CERN Document Server.
# Copyright (C) 2016, 2017 CERN.
#
# CERN Document Server is free software; you can redistribute it
# and/or modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the
# License, or (at your option) any later version.
#
# CERN Document Server is distributed in the hope that it will be
# useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with CERN Document Server; if not, write to the
# Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston,
# MA 02111-1307, USA.
#
# In applying this license, CERN does not
# waive the privileges and immunities granted to it by virtue of its status
# as an Intergovernmental Organization or submit itself to any jurisdiction.

"""CDS migration to CDSLabs tests."""
import pytest
from tests.helpers import load_json

from cds_migrator_kit.modules.migrator.errors import LossyConversion
from cds_migrator_kit.modules.migrator.records import CDSRecordDump


def test_migrate_record(datadir, app):
    """Test migrate date."""
    # [[ migrate the book ]]
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
