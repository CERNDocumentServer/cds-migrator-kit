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


def test_full_hr_stream(
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
    groups,
):

    stream_config = config(mocker, community, orcid_name_data)

    runner = Runner(
        stream_definitions=[RecordStreamDefinition],
        config_filepath=Path(stream_config).absolute(),
        dry_run=False,
        collection="hr",
        keep_logs=False,
    )
    runner.run()

    # 2 Test records
    assert CDSMigrationLegacyRecord.query.count() == 2

    legacy_recid = "2647384"
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

    # Check if sync entry created
    assert CDSToCLCSyncModel.query.filter_by(
        parent_record_pid=new_record["parent"]["id"]
    )

    # Administrative Unit
    assert new_record["custom_fields"]["cern:administrative_unit"] == "DI"

    # Title extracted from field 037__ (CERN-STAFF-RULES-ED01) when missing in 245__
    assert new_record["metadata"]["title"] == "Staff Rules and Regulations No.ED01"

    # Restricted access - field 591 --> CERN INTERNAL
    assert new_record["access"] == {
        "record": "restricted",
        "files": "restricted",
        "embargo": {"active": False, "reason": None},
        "status": "restricted",
    }

    # Dates --> 9999 not in 925__b
    new_record["metadata"]["dates"] = [
        {"date": "2021-05-01", "type": {"id": "valid", "title": {"en": "Valid"}}},
        {
            "date": "2021-05-01",
            "type": {"id": "withdrawn", "title": {"en": "Withdrawn"}},
        },
    ]

    another_legacy_recid = "2364643"

    results = current_rdm_records_service.search(
        system_identity, q=f"metadata.identifiers.identifier:{another_legacy_recid}"
    )

    results = results.to_dict()

    assert results["hits"]["total"] == 1
    new_record = current_rdm_records_service.read(
        system_identity, results["hits"]["hits"][0]["id"]
    )

    new_record = new_record.to_dict()

    # File status restricted
    assert new_record["access"]["files"] == "restricted"
