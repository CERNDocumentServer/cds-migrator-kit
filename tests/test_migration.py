# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS migration to CDSLabs tests."""

from cds_migrator_kit.rdm.migration.transform.xml_processing.dumper import CDSRecordDump
from tests.helpers import load_json


def test_migrate_record(datadir, base_app):
    """Test migrate date."""
    # [[ migrate the record]]
    with base_app.app_context():
        data = load_json(datadir, "summer_note.json")
        dump = CDSRecordDump(data=data[0])
        dump.prepare_revisions()
        res = dump.revisions[-1][1]

        assert res["legacy_recid"] == 2285569
        assert res == {
            "recid": "2285569",
            "legacy_recid": 2285569,
            "report_number": "CERN-STUDENTS-Note-2017-222",
            "languages": ["ENG"],
            "creators": [
                {
                    "person_or_org": {
                        "type": "personal",
                        "family_name": "Arzi, Ofir",
                        "identifiers": [],
                    },
                    "role": {"id": "other"},
                }
            ],
            "title": "Deep Learning Methods for Particle Reconstruction in the HGCal",
            "description": "The High Granularity end-cap Calorimeter is part of the phase-2 CMS upgrade (see Figure \\ref{fig:cms})\\cite{Contardo:2020886}. It's goal it to provide measurements of high resolution in time, space and energy. Given such measurements, the purpose of this work is to discuss the use of Deep Neural Networks for the task of particle and trajectory reconstruction, identification and energy estimation, during my participation in the CERN Summer Students Program.",
        }
