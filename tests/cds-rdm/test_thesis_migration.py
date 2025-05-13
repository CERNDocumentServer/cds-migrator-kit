# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Tests suites."""
import json
from pathlib import Path

from cds_rdm.clc_sync.models import CDSToCLCSyncModel
from cds_rdm.legacy.models import CDSMigrationLegacyRecord
from helpers import config
from invenio_access.permissions import system_identity
from invenio_rdm_records.proxies import current_rdm_records_service
from invenio_rdm_records.records.api import RDMDraft, RDMParent, RDMRecord
from invenio_search.engine import dsl

from cds_migrator_kit.rdm.records.streams import RecordStreamDefinition
from cds_migrator_kit.runner.runner import Runner


def test_full_thesis_stream(
    test_app,
    minimal_restricted_record,
    uploader,
    client,
    search,
    search_clear,
    superuser_identity,
    orcid_name_data,
    community,
    mocker,
):
    # Creates a new name for the orcid_user

    stream_config = config(mocker, community, orcid_name_data)

    runner = Runner(
        stream_definitions=[RecordStreamDefinition],
        config_filepath=Path(stream_config).absolute(),
        dry_run=False,
        collection="thesis",
    )
    runner.run()

    # 3 test records
    assert CDSMigrationLegacyRecord.query.count() == 3
    # only one to be synced to ILS
    assert CDSToCLCSyncModel.query.count() == 1

    legacy_recid = "2742366"

    RDMRecord.index.refresh()
    results = current_rdm_records_service.search(
        system_identity, q=f"metadata.identifiers.identifier:{legacy_recid}"
    )
    results = results.to_dict()
    assert results["hits"]["total"] == 1
    new_record = current_rdm_records_service.read(
        system_identity, results["hits"]["hits"][0]["id"]
    )

    new_record = new_record.to_dict()

    # check if sync entry created
    assert CDSToCLCSyncModel.query.filter_by(
        parent_record_pid=new_record["parent"]["id"]
    )

    assert new_record["custom_fields"]["thesis:thesis"]["date_submitted"] == "2020"
    assert new_record["custom_fields"]["thesis:thesis"]["date_defended"] == "2020-04-30"
    assert new_record["custom_fields"]["thesis:thesis"]["type"] == "PhD"
    assert new_record["custom_fields"]["cern:experiments"] == [
        {"id": "CMS", "title": {"en": "CMS"}}
    ]
    assert new_record["metadata"]["subjects"] == [
        {
            "id": "Detectors and Experimental Techniques",
            "subject": "Detectors and Experimental Techniques",
            "scheme": "CERN",
        },
        {"subject": "TOP"},
    ]

    assert new_record["created"] == "2020-10-19T00:00:00+00:00"

    # original DOI coming from DESY
    assert new_record["pids"]["doi"] == {
        "identifier": "10.3204/PUBDB-2020-02655",
        "provider": "external",
    }

    assert new_record["metadata"]["related_identifiers"] == [
        {
            "identifier": "978-3-030-90375-6",
            "scheme": "isbn",
            "relation_type": {"id": "isversionof", "title": {"en": "Is version of"}},
        },
        {
            "identifier": "978-3-030-90376-3",
            "scheme": "isbn",
            "relation_type": {"id": "isversionof", "title": {"en": "Is version of"}},
        },
        ## secondary DOI is a version of the publication in springer
        {
            "identifier": "10.1007/978-3-030-90376-3",
            "scheme": "doi",
            "relation_type": {"id": "isversionof", "title": {"en": "Is version of"}},
            "resource_type": {
                "id": "publication",
                "title": {"en": "Publication", "de": "Publikation"},
            },
        },
    ]

    another_legacy_recid = "2741624"

    results = current_rdm_records_service.search(
        system_identity, q=f"metadata.identifiers.identifier:{another_legacy_recid}"
    )

    results = results.to_dict()

    assert results["hits"]["total"] == 1
    new_record = current_rdm_records_service.read(
        system_identity, results["hits"]["hits"][0]["id"]
    )

    new_record = new_record.to_dict()
    assert new_record["pids"] == {
        "doi": {"identifier": "10.18154/RWTH-2020-05782", "provider": "external"},
        "oai": {
            "identifier": f'oai:oai:invenio-app-rdm.org::{new_record["id"]}',
            "provider": "oai",
        },
    }
    assert new_record["custom_fields"]["thesis:thesis"] == {
        "university": "RWTH Aachen University",
        "type": "PhD",
        "date_submitted": "2020",
        "date_defended": "2020-05-14",
    }

    legacy_recid_aida = "2316709"

    results = current_rdm_records_service.search(
        system_identity, q=f"metadata.identifiers.identifier:{legacy_recid_aida}"
    )

    results = results.to_dict()

    assert results["hits"]["total"] == 1
    new_record = current_rdm_records_service.read(
        system_identity, results["hits"]["hits"][0]["id"]
    )

    new_record = new_record.to_dict()
    assert new_record["metadata"]["subjects"] == [
        {
            "id": "Detectors and Experimental Techniques",
            "subject": "Detectors and Experimental Techniques",
            "scheme": "CERN",
        },
        {"subject": "collection:AIDA-2020"},
    ]

    assert new_record["metadata"]["additional_descriptions"] == [
        {
            "description": "2: Innovation and outreach (WP)",
            "type": {"id": "technical-info", "title": {"en": "Technical info"}},
        }
    ]

    assert new_record["metadata"]["languages"] == [
        {"id": "eng", "title": {"en": "English", "da": "Engelsk"}}
    ]

    assert new_record["metadata"]["funding"] == [
        {
            "funder": {"id": "00k4n6c32", "name": "European Commission"},
            "award": {
                "id": "00k4n6c32::654168",
                "number": "654168",
                "title": {
                    "en": "Advanced European Infrastructures for Detectors at Accelerators (2020)"
                },
                "identifiers": [
                    {
                        "identifier": "https://cordis.europa.eu/project/id/755021",
                        "scheme": "url",
                    }
                ],
                "acronym": "AIDA-2020",
                "program": "H2020-EU.1.4.",
            },
        }
    ]
