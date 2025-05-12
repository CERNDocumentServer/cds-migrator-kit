# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Tests suites."""

from cds_rdm.legacy.models import CDSMigrationLegacyRecord
from flask import current_app
from invenio_access.permissions import system_identity
from invenio_rdm_records.proxies import current_rdm_records_service
from invenio_records_resources.proxies import current_service_registry
from invenio_vocabularies.contrib.names.api import Name

from cds_migrator_kit.rdm.users.runner import SubmitterRunner
from cds_migrator_kit.rdm.users.streams import SubmitterStreamDefinition


def config(mocker, community, orcid_name_data):
    """Configure migration streams."""
    service = current_service_registry.get("names")
    service.create(system_identity, orcid_name_data)
    Name.index.refresh()
    mocker.patch(
        "cds_migrator_kit.runner.runner.Runner._read_config",
        return_value={
            "db_uri": "postgresql://cds-rdm-migration:cds-rdm-migration@localhost:5432/cds-rdm-migration",
            "records": {
                "sspn": {
                    "data_dir": "tests/cds-rdm/data/sspn",
                    "tmp_dir": "tests/cds-rdm/data/sspn",
                    "log_dir": "tests/cds-rdm/data/log/sspn",
                    "extract": {"dirpath": "tests/cds-rdm/data/sspn/dumps/"},
                    "transform": {
                        "files_dump_dir": "tests/cds-rdm/data/sspn/files/",
                        "missing_users": "tests/cds-rdm/data/users",
                        "community_id": f"{str(community.id)}",
                    },
                    "load": {
                        "legacy_pids_to_redirect": "cds_migrator_kit/rdm/data/summer_student_reports/duplicated_pids.json"
                    },
                },
                "thesis": {
                    "data_dir": "tests/cds-rdm/data/thesis",
                    "tmp_dir": "tests/cds-rdm/data/thesis",
                    "log_dir": "tests/cds-rdm/data/log/thesis",
                    "extract": {"dirpath": "tests/cds-rdm/data/thesis/dump/"},
                    "transform": {
                        "files_dump_dir": "tests/cds-rdm/data/thesis/files/",
                        "missing_users": "tests/cds-rdm/data/users",
                        "community_id": f"{str(community.id)}",
                    },
                    "load": {
                        "legacy_pids_to_redirect": "tests/cds-rdm/data/thesis/duplicated_pids.json"
                    },
                },
            },
        },
    )

    stream_config = current_app.config["CDS_MIGRATOR_KIT_STREAM_CONFIG"]
    user_runner = SubmitterRunner(
        stream_definition=SubmitterStreamDefinition,
        missing_users_dir="tests/cds-rdm/data/users",
        dirpath="tests/cds-rdm/data/sspn/dumps/",
        log_dir="tests/cds-rdm/data/log/users",
        dry_run=False,
    )
    user_runner.run()

    return stream_config
