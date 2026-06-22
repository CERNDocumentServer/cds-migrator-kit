# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Tests suites."""

import json
from pathlib import Path

from cds_rdm.legacy.models import CDSMigrationLegacyRecord
from helpers import config
from invenio_access.permissions import system_identity
from invenio_rdm_records.proxies import current_rdm_records_service

from cds_migrator_kit.rdm.records.streams import RecordStreamDefinition
from cds_migrator_kit.runner.runner import Runner


def publication_date_consistency_across_versions(record, record_state):
    """2889522."""

    dict_rec = record.to_dict()

    all_dates = []

    for record_version in record_state["versions"]:
        rec = current_rdm_records_service.read(
            system_identity, record_version["new_recid"]
        )
        dict_version = rec.to_dict()

        all_dates.append(dict_version["metadata"]["publication_date"])
    assert len(all_dates) > 0

    # Check all versions have the same publication date
    assert len(set(all_dates)) == 1
    assert dict_rec["metadata"]["publication_date"] == all_dates[0]


def test_new_version_publication_date(
    test_app,
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
        collection="sspn_publication_date_consistency",
        keep_logs=False,
    )
    runner.run()

    with open(
        "tests/cds-rdm/tmp/logs/sspn_publication_date_consistency/rdm_records_state.json",
        "r",
    ) as state_logs:
        records = json.load(state_logs)

    for record in records:

        loaded_rec = current_rdm_records_service.read(
            system_identity, record["latest_version"]
        )
        if record["legacy_recid"] == "2889522":
            publication_date_consistency_across_versions(loaded_rec, record)
