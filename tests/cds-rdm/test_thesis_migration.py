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
from cds_rdm.clc_sync.models import CDSToCLCSyncModel
from helpers import config
from invenio_rdm_records.proxies import current_rdm_records_service

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

    assert CDSMigrationLegacyRecord.query.count() == 1
    assert CDSToCLCSyncModel.query.count() == 1
