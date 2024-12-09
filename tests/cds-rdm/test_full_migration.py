# 2788738 -> user with orcid
# 2684743 -> many fields
# 2889522 -> multiple file versions
# 2051872 -> multiple version with custom fields
# 2207587 -> multiple custom fields
# 2783103 -> file missing # TODO
# 2783104 -> file restricted # TODO
# 2046076 -> irregular experiment field value # TODO
# 1597985 -> contains ALEPH identifier # TODO
# 2041388 -> custom affiliation # TODO
# 2779426 -> clump of keywords # TODO
# 2294138 -> author with inspire id # TODO
import json
from pathlib import Path

import pytest_mock
from cds_rdm.legacy.models import CDSMigrationLegacyRecord
from flask import current_app
from invenio_access.permissions import system_identity
from invenio_rdm_records.proxies import current_rdm_records_service

from cds_migrator_kit.rdm.migration.runner import Runner
from cds_migrator_kit.rdm.migration.streams import RecordStreamDefinition


def suite_multi_field(record):
    dict_rec = record.to_dict()
    assert dict_rec["created"] == "2019-07-29T00:00:00+00:00"
    assert dict_rec["versions"]["index"] == 1
    assert dict_rec["status"] == "published"
    assert dict_rec["access"] == {
        "record": "public",
        "files": "public",
        "embargo": {"active": False, "reason": None},
        "status": "open",
    }
    assert (
        dict_rec["internal_notes"][0]["note"]
        == "Comments submitted after 31-08-2021 10:41"
    )
    assert dict_rec["files"]["count"] == 1
    assert dict_rec["files"]["total_bytes"] == 13264
    assert "TN_T6SamplesGammaSpecBenchmark.pdf" in dict_rec["files"]["entries"]
    assert dict_rec["custom_fields"] == {
        "cern:departments": [{"id": "HSE", "title": {"en": "HSE"}}],
        "cern:studies": ["Physics Beyond Colliders"],
    }

    assert dict_rec["metadata"]["creators"] == [
        {
            "person_or_org": {
                "type": "personal",
                "name": "Juste, Vincent",
                "given_name": "Vincent",
                "family_name": "Juste",
                "identifiers": [{"identifier": "2675934", "scheme": "lcds"}],
            }
        }
    ]
    assert dict_rec["metadata"]["publication_date"] == "2018-08-02"
    assert (
        dict_rec["metadata"]["title"] == "FLUKA and ActiWiz benchmark on BDF materials"
    )
    assert dict_rec["metadata"]["publisher"] == "CERN"
    assert dict_rec["metadata"]["subjects"] == [
        {
            "id": "Nuclear Physics - Experiment",
            "subject": "Nuclear Physics - Experiment",
            "scheme": "CERN",
        },
        {"subject": "FLUKA benchmark"},
        {"subject": "ActiWiz benchmark"},
        {"subject": "BDF"},
    ]
    assert dict_rec["metadata"]["contributors"] == [
        {
            "person_or_org": {
                "type": "personal",
                "name": "Vincke, Helmut",
                "given_name": "Helmut",
                "family_name": "Vincke",
                "identifiers": [{"identifier": "2067721", "scheme": "lcds"}],
            },
            "role": {"id": "other", "title": {"en": "Other"}},
            "affiliations": [{"name": "CERN"}],
        },
        {
            "person_or_org": {"type": "organizational", "name": "RP collaboration"},
            "role": {
                "id": "hostinginstitution",
                "title": {"en": "Hosting institution"},
            },
        },
        {
            "person_or_org": {
                "type": "personal",
                "name": "Casolino M.",
                "family_name": "Casolino M.",
            },
            "role": {"id": "supervisor", "title": {"en": "Supervisor"}},
        },
        {
            "person_or_org": {
                "type": "personal",
                "name": "Ahdida C.",
                "family_name": "Ahdida C.",
            },
            "role": {"id": "supervisor", "title": {"en": "Supervisor"}},
        },
    ]
    assert dict_rec["metadata"]["identifiers"] == [
        {"identifier": "CERN-STUDENTS-Note-2019-028", "scheme": "cds_ref"},
        {"identifier": "CERN-PBC-Notes-2021-006", "scheme": "cds_ref"},
    ]

    assert dict_rec["metadata"]["additional_descriptions"] == [
        {
            "description": "Abbreviations: BDF stands for Beam Dump Facility",
            "type": {"id": "other", "title": {"en": "Other"}},
        }
    ]


def orcid_id(record):
    dict_rec = record.to_dict()
    # TODO pre-create user with orcid
    assert dict_rec["metadata"]["creators"] == [
        {
            "person_or_org": {
                "type": "personal",
                "name": "Mendoza, Diego",
                "given_name": "Diego",
                "family_name": "Mendoza",
                "identifiers": [{"identifier": "2773374", "scheme": "lcds"}],
            }
        }
    ]


def multiple_versions(record, record_state):
    dict_rec = record.to_dict()
    assert dict_rec["versions"]["index"] == 2


def multiple_versions_with_cs(record):
    dict_rec = record.to_dict()
    assert dict_rec["versions"]["index"] == 2
    assert dict_rec["custom_fields"] == {
        "cern:experiments": [{"id": "CMS", "title": {"en": "CMS"}}],
        "cern:departments": [{"id": "PH", "title": {"en": "PH"}}],
    }


def test_full_migration_stream(
    test_app,
    minimal_restricted_record,
    uploader,
    client,
    search,
    search_clear,
    superuser_identity,
    community,
    mocker,
):
    mocker.patch(
        "cds_migrator_kit.rdm.migration.runner.Runner._read_config",
        return_value={
            "data_dir": "tests/cds-rdm/data/",
            "tmp_dir": "tests/cds-rdm/data/",
            "state_dir": "tests/cds-rdm/data/cache",
            "log_dir": "tests/cds-rdm/data/log",
            "db_uri": "postgresql://cds-rdm-migration:cds-rdm-migration@localhost:5432/cds-rdm-migration",
            "old_secret_key": "CHANGE_ME",
            "new_secret_key": "CHANGE_ME",
            "records": {
                "extract": {"dirpath": "tests/cds-rdm/data/sspn/dumps/"},
                "transform": {
                    "files_dump_dir": "tests/cds-rdm/data/sspn/files/",
                    "missing_users": "tests/cds-rdm/data/users",
                    "community_id": f"{str(community.id)}",
                },
                "load": {
                    "legacy_pids_to_redirect": "cds_migrator_kit/rdm/migration/data/summer_student_reports/duplicated_pids.json"
                },
            },
        },
    )
    stream_config = current_app.config["CDS_MIGRATOR_KIT_STREAM_CONFIG"]
    runner = Runner(
        stream_definitions=[RecordStreamDefinition],
        config_filepath=Path(stream_config).absolute(),
        dry_run=False,
    )
    runner.run()

    assert CDSMigrationLegacyRecord.query.count() == 12

    with open("tests/cds-rdm/tmp/logs/rdm_records_state.json") as state_logs:
        records = json.load(state_logs)

    for record in records:

        loaded_rec = current_rdm_records_service.read(
            system_identity, record["latest_version"]
        )
        if record["legacy_recid"] == "2684743":
            suite_multi_field(loaded_rec)
        if record["legacy_recid"] == "2788738":
            orcid_id(loaded_rec)
        if record["legacy_recid"] == "2889522":
            multiple_versions(loaded_rec, record)
        if record["legacy_recid"] == "2051872":
            multiple_versions_with_cs(loaded_rec)
