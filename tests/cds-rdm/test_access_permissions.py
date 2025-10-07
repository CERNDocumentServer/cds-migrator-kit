# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

import json
from pathlib import Path

from cds_rdm.legacy.models import CDSMigrationLegacyRecord
from helpers import config
from invenio_access.permissions import system_identity
from invenio_accounts.models import User
from invenio_rdm_records.proxies import current_rdm_records_service

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
        {
            "permission": "manage",
            "subject": {"id": "2", "type": "user"},
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
