# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

import json
from pathlib import Path

from cds_rdm.clc_sync.models import CDSToCLCSyncModel
from cds_rdm.legacy.models import CDSMigrationLegacyRecord
from helpers import config
from invenio_access.permissions import system_identity
from invenio_rdm_records.proxies import current_rdm_records_service
from invenio_rdm_records.records.api import RDMDraft, RDMParent, RDMRecord

from cds_migrator_kit.rdm.records.streams import RecordStreamDefinition
from cds_migrator_kit.runner.runner import Runner


def grant_access_permissions(record):
    """2872558."""
    dict_rec = record.to_dict()

    assert dict_rec["parent"]["access"]["grants"] == [
        {
            "permission": "view",
            "subject": {"id": "hr-dep", "type": "role"},
            "origin": "migrated",
        },
        {
            "permission": "view",
            "subject": {"id": "1", "type": "user"},
            "origin": "migrated",
        },
    ]


def file_restricted(record):
    """2872569."""
    dict_rec = record.to_dict()
    assert "access" in dict_rec
    assert dict_rec["access"]["record"] == "public"
    assert dict_rec["access"]["files"] == "restricted"


def check_log_for_error(record_id, target_error):
    """Checks if the log contains the specified error for the specified record ID."""
    with open("tests/cds-rdm/tmp/logs/it/rdm_migration_errors.csv", "r") as file:
        lines = file.readlines()

        for line in lines:
            if record_id in line and target_error in line:
                return True

        return False


def test_access_permissions(
    test_app,
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
        collection="it",
        keep_logs=False,
    )
    runner.run()

    assert CDSMigrationLegacyRecord.query.count() == 3

    assert check_log_for_error("2872550", "GrantCreationError")  # User doesn't exist
    assert check_log_for_error(
        "126154", "ManualImportRequired"
    )  # Grant validation fails

    with open("tests/cds-rdm/tmp/logs/it/rdm_records_state.json", "r") as state_logs:
        records = json.load(state_logs)

    for record in records:
        loaded_rec = current_rdm_records_service.read(
            system_identity, record["latest_version"]
        )
        if record["legacy_recid"] == "2872558":
            grant_access_permissions(loaded_rec)
        if record["legacy_recid"] == "2872569":
            file_restricted(loaded_rec)


# Tests for access_grants_view configuration (collection-wide access grants)


def test_collection_with_access_grants_view_configuration(
    test_app,
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
    """Test that collections configured with access_grants_view properly assign access grants."""
    # Configure the stream with access_grants_view for HR collection

    stream_config = config(mocker, community, orcid_name_data)

    runner = Runner(
        stream_definitions=[RecordStreamDefinition],
        config_filepath=Path(stream_config).absolute(),
        dry_run=False,
        collection="hr_restricted",
        keep_logs=False,
    )
    runner.run()

    # Check that records were migrated with access grants from configuration
    RDMRecord.index.refresh()

    # Test record (555555) to ensure access_grants_view is applied consistently
    legacy_recid = "555555"
    results = current_rdm_records_service.search(
        system_identity, q=f"metadata.identifiers.identifier:{legacy_recid}"
    )
    assert results.total == 1
    new_record = current_rdm_records_service.read(
        system_identity, list(results.hits)[0]["id"]
    )
    new_record = new_record.to_dict()

    # Verify this record also has parent with access grants
    parent_id = new_record["parent"]["id"]
    assert parent_id is not None

    # restricted file status, restricted  = cern-personnel
    # see https://cds.cern.ch/admin/webaccess/webaccessadmin.py/showactiondetails?id_action=39&reverse=1
    assert new_record["access"]["record"] == "restricted"
    assert new_record["access"]["files"] == "restricted"
    grants = new_record["parent"]["access"]["grants"]
    sorted_grants = sorted(grants, key=lambda d: d["subject"]["id"])
    expected = [
        {
            "permission": "view",
            "subject": {"id": "cern-personnel", "type": "role"},
            "origin": "migrated",
        },
    ]

    sorted_expected = sorted(expected, key=lambda d: d["subject"]["id"])
    assert sorted_grants == sorted_expected

    # assigned status
    legacy_recid = "23646466"
    results = current_rdm_records_service.search(
        system_identity, q=f"metadata.identifiers.identifier:{legacy_recid}"
    )
    assert results.total == 1

    new_record = current_rdm_records_service.read(
        system_identity, list(results.hits)[0]["id"]
    )
    new_record = new_record.to_dict()

    # Verify this record also has parent with access grants
    parent_id = new_record["parent"]["id"]
    assert parent_id is not None

    assert new_record["access"]["record"] == "restricted"
    assert new_record["access"]["files"] == "restricted"
    grants = new_record["parent"]["access"]["grants"]
    sorted_grants = sorted(grants, key=lambda d: d["subject"]["id"])
    expected = [
        {
            "permission": "view",
            "subject": {"id": "hr-web-gacepa", "type": "role"},
            "origin": "migrated",
        },
        {
            "permission": "view",
            "subject": {"id": "eligibility-retr-actual", "type": "role"},
            "origin": "migrated",
        },
    ]

    sorted_expected = sorted(expected, key=lambda d: d["subject"]["id"])
    # check if restricted file status takes over the collection settings
    # see https://cds.cern.ch/admin/webaccess/webaccessadmin.py/showactiondetails?id_action=39&reverse=1
    assert sorted_grants == sorted_expected
