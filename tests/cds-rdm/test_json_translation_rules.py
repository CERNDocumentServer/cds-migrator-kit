# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS migration to CDSLabs tests."""

from cds_migrator_kit.transform.dumper import CDSRecordDump
from tests.helpers import load_json


def test_migrate_sspn_record(datadir, base_app):
    """Test migrate date."""
    # [[ migrate the record]]
    with base_app.app_context():
        data = load_json(datadir, "summer_note.json")
        dump = CDSRecordDump(data=data[0])
        dump.prepare_revisions()
        created_date, res = dump.latest_revision

        assert res["legacy_recid"] == 2285569
        assert res == {
            "resource_type": {"id": "publication-technicalnote"},
            "recid": "2285569",
            "legacy_recid": 2285569,
            "identifiers": [
                {"identifier": "2285569", "scheme": "lcds"},
                {"scheme": "cds_ref", "identifier": "CERN-STUDENTS-Note-2017-222"},
            ],
            "languages": [{"id": "eng"}],
            "creators": [
                {
                    "person_or_org": {
                        "type": "personal",
                        "family_name": "A",
                        "given_name": "O",
                        "identifiers": [
                            {"identifier": "2261577", "scheme": "lcds"},
                            {"identifier": "1111", "scheme": "cern"},
                        ],
                    }
                }
            ],
            "title": "Deep Learning Methods for Particle Reconstruction in the HGCal",
            "publisher": "CERN",
            "publication_date": "2017-06-24",
            "description": "The High Granularity end-cap Calorimeter is part of the phase-2 CMS upgrade (see Figure \\ref{fig:cms})\\cite{Contardo:2020886}. It's goal it to provide measurements of high resolution in time, space and energy. Given such measurements, the purpose of this work is to discuss the use of Deep Neural Networks for the task of particle and trajectory reconstruction, identification and energy estimation, during my participation in the CERN Summer Students Program.",
            "subjects": [
                {
                    "id": "Particle Physics - Experiment",
                    "subject": "Particle Physics - Experiment",
                },
                {"subject": "Deep Learning"},
                {"subject": "HGCAL"},
                {"subject": "Particle Reconsturcion"},
            ],
            "custom_fields": {
                "cern:experiments": ["CMS"],
                "cern:accelerators": [],
                "cern:projects": [],
                "cern:facilities": [],
                "cern:studies": [],
                "cern:beams": [],
                "cern:departments": ["EP"],
            },
            "contributors": [
                {
                    "person_or_org": {
                        "type": "organizational",
                        "name": "CERN. Geneva. EP Department",
                        "family_name": "CERN. Geneva. EP Department",
                    },
                    "role": {"id": "hostinginstitution"},
                },
                {
                    "person_or_org": {
                        "type": "personal",
                        "name": "c l",
                        "family_name": "c l",
                    },
                    "role": {"id": "supervisor"},
                },
                {
                    "person_or_org": {
                        "type": "personal",
                        "name": "p m",
                        "family_name": "p m",
                    },
                    "role": {"id": "supervisor"},
                },
                {
                    "person_or_org": {
                        "type": "personal",
                        "name": "j k",
                        "family_name": "j k",
                    },
                    "role": {"id": "supervisor"},
                },
            ],
            "submitter": "oa@cern.ch",
            "_created": "2017-09-18",
            "record_restriction": "public",
        }


def test_migrate_record_all_fields(datadir, base_app):
    """Test migrate date."""
    # [[ migrate the record]]
    with base_app.app_context():
        data = load_json(datadir, "all_fields.json")
        dump = CDSRecordDump(data=data[0])
        dump.prepare_revisions()
        created_date, res = dump.latest_revision
        assert res["legacy_recid"] == 2684743
        assert res == {
            "resource_type": {"id": "publication-technicalnote"},
            "recid": "2684743",
            "legacy_recid": 2684743,
            "identifiers": [
                {"scheme": "lcds", "identifier": "2684743"},
                {"scheme": "cds_ref", "identifier": "CERN-STUDENTS-Note-2019-028"},
                {"scheme": "cds_ref", "identifier": "CERN-PBC-Notes-2021-006"},
            ],
            "languages": [{"id": "eng"}],
            "creators": [
                {
                    "person_or_org": {
                        "type": "personal",
                        "family_name": "Juste",
                        "given_name": "Vincent",
                        "identifiers": [
                            {"identifier": "2675934", "scheme": "lcds"},
                            {"identifier": "81111", "scheme": "cern"},
                        ],
                    }
                }
            ],
            "title": "FLUKA and ActiWiz benchmark on BDF materials",
            "additional_descriptions": [
                {
                    "description": "Abbreviations: BDF stands for Beam Dump Facility",
                    "type": {"id": "other"},
                }
            ],
            "publisher": "CERN",
            "publication_date": "2018-08-02",
            "description": "This note describes the FLUKA and Actiwiz benchmark with gamma spectroscopy results of various material samples, which were irradiated during the Beam Dump Facility (BDF) prototype target test in the North Area of the Super Proton Synchrotron (SPS) at CERN. The samples represent most of the materials that will be used in the construction of the BDF facility.",
            "internal_notes": [{"note": "Comments submitted after 31-08-2021 10:41"}],
            "subjects": [
                {
                    "id": "Nuclear Physics - Experiment",
                    "subject": "Nuclear Physics - Experiment",
                },
                {"subject": "FLUKA benchmark"},
                {"subject": "ActiWiz benchmark"},
                {"subject": "BDF"},
            ],
            "custom_fields": {
                "cern:experiments": [],
                "cern:accelerators": [],
                "cern:projects": [],
                "cern:facilities": [],
                "cern:studies": ["Physics Beyond Colliders"],
                "cern:beams": [],
                "cern:departments": ["HSE"],
            },
            "contributors": [
                {
                    "affiliations": [
                        "The Barcelona Institute of Science and " "Technology BIST ES"
                    ],
                    "person_or_org": {
                        "family_name": "Casolino",
                        "given_name": "Mirkoantonio",
                        "identifiers": [
                            {
                                "identifier": "INSPIRE-00366594",
                                "scheme": "inspire_author",
                            },
                            {"identifier": "1111", "scheme": "cern"},
                            {"identifier": "2083412", "scheme": "lcds"},
                        ],
                        "type": "personal",
                    },
                    "role": {"id": "other"},
                },
                {
                    "affiliations": ["CERN"],
                    "person_or_org": {
                        "family_name": "A",
                        "given_name": "CC",
                        "identifiers": [
                            {"identifier": "2087282", "scheme": "lcds"},
                            {"identifier": "1111", "scheme": "cern"},
                        ],
                        "type": "personal",
                    },
                    "role": {"id": "other"},
                },
                {
                    "person_or_org": {
                        "type": "personal",
                        "family_name": "V",
                        "given_name": "H",
                        "identifiers": [
                            {"identifier": "2067721", "scheme": "lcds"},
                            {"identifier": "1111", "scheme": "cern"},
                        ],
                    },
                    "role": {"id": "other"},
                    "affiliations": ["CERN"],
                },
                {
                    "person_or_org": {
                        "type": "organizational",
                        "name": "RP collaboration",
                        "family_name": "RP collaboration",
                    },
                    "role": {"id": "hostinginstitution"},
                },
                {
                    "person_or_org": {
                        "type": "personal",
                        "name": "C M.",
                        "family_name": "C M.",
                    },
                    "role": {"id": "supervisor"},
                },
                {
                    "person_or_org": {
                        "type": "personal",
                        "name": "A C.",
                        "family_name": "A C.",
                    },
                    "role": {"id": "supervisor"},
                },
            ],
            "submitter": "vj@cern.ch",
            "_created": "2019-07-29",
            "record_restriction": "public",
        }
