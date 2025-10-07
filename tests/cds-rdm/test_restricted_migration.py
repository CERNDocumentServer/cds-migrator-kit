# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Tests suites."""

# 2684743 -> access fields
# 2783104 -> file restricted

import json
from pathlib import Path

from cds_rdm.legacy.models import CDSMigrationLegacyRecord
from helpers import config
from invenio_access.permissions import system_identity
from invenio_rdm_records.proxies import current_rdm_records_service

from cds_migrator_kit.rdm.records.streams import RecordStreamDefinition
from cds_migrator_kit.runner.runner import Runner


def access_restricted(record):
    """2684743."""
    dict_rec = record.to_dict()
    assert dict_rec["status"] == "published"
    assert dict_rec["access"] == {
        "record": "restricted",
        "files": "restricted",
        "embargo": {"active": False, "reason": None},
        "status": "restricted",
    }


def file_restricted(record):
    """2783104."""
    dict_rec = record.to_dict()
    assert "access" in dict_rec
    assert dict_rec["access"]["record"] == "restricted"
    assert dict_rec["access"]["files"] == "restricted"
    assert "files" in dict_rec
    assert dict_rec["files"]["enabled"] == True
    assert dict_rec["files"]["count"] == 1


def test_restricted_migration(
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
        collection="sspn_restr",
        keep_logs=False,
    )
    runner.run()

    with open(
        "tests/cds-rdm/tmp/logs/sspn_restr/rdm_records_state.json", "r"
    ) as state_logs:
        records = json.load(state_logs)

    for record in records:

        loaded_rec = current_rdm_records_service.read(
            system_identity, record["latest_version"]
        )
        if record["legacy_recid"] == "2684743":
            access_restricted(loaded_rec)
        if record["legacy_recid"] == "2783104":
            file_restricted(loaded_rec)
