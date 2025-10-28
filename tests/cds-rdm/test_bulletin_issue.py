# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
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
from invenio_rdm_records.records.api import RDMDraft, RDMParent, RDMRecord

from cds_migrator_kit.rdm.records.streams import RecordStreamDefinition
from cds_migrator_kit.runner.runner import Runner


def parent_related_identifier(record):
    """2234683."""
    dict_rec = record.to_dict()
    assert dict_rec["metadata"]["related_identifiers"] == [
        {
            "identifier": "1713049",
            "scheme": "cds",
            "relation_type": {
                "id": "ispublishedin",
                "title": {"de": "Ist veröffentlicht in", "en": "Is published in"},
            },
        }
    ]


def test_bulletin_issue(
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
    # Creates a new name for the orcid_user

    stream_config = config(mocker, community, orcid_name_data)

    runner = Runner(
        stream_definitions=[RecordStreamDefinition],
        config_filepath=Path(stream_config).absolute(),
        dry_run=False,
        collection="bulletin_issue",
        keep_logs=False,
    )

    runner.run()
    assert CDSMigrationLegacyRecord.query.count() == 1

    with open(
        "tests/cds-rdm/tmp/logs/bulletin_issue/rdm_records_state.json", "r"
    ) as state_logs:
        records = json.load(state_logs)

    for record in records:
        loaded_rec = current_rdm_records_service.read(
            system_identity, record["latest_version"]
        )
        if record["legacy_recid"] == "2234683":
            parent_related_identifier(loaded_rec)
