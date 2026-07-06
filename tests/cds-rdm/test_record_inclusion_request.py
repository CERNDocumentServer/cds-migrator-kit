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
from invenio_rdm_records.records.api import RDMRecord
from invenio_rdm_records.requests import CommunityInclusion
from invenio_requests.proxies import current_requests_service
from invenio_requests.records.api import Request
from invenio_search.api import dsl

from cds_migrator_kit.rdm.records.streams import RecordStreamDefinition
from cds_migrator_kit.runner.runner import Runner


def test_accepted_record_inclusion_request(
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
        collection="lcd_restr",
        keep_logs=False,
    )
    runner.run()

    assert CDSMigrationLegacyRecord.query.count() == 1
    legacy_recid = "2294138"

    RDMRecord.index.refresh()
    Request.index.refresh()
    results = current_rdm_records_service.search(
        system_identity, q=f"metadata.identifiers.identifier:{legacy_recid}"
    )

    for record in results:
        request_item = current_requests_service.search(
            system_identity,
            extra_filter=dsl.Q(
                "bool",
                must=[
                    dsl.Q("term", **{"topic.record": record["id"]}),
                    dsl.Q("term", **{"type": CommunityInclusion.type_id}),
                ],
            ),
        )
    assert request_item.total == 1
    status = next(iter(request_item), {}).get("status")
    assert status == "accepted"
