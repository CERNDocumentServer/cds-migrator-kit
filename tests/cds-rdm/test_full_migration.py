# 2788738 -> user with orcid # TODO
# 2684743 -> many fields
# 2889522 -> multiple file versions
# 2051872 -> multiple version with custom fields
# 2207587 -> multiple custom fields
# 2783103 -> file missing
# 2783104 -> file restricted
# 2046076 -> irregular experiment field value
# 1597985 -> contains ALEPH identifier
# 2041388 -> custom affiliation
# 2779426 -> clump of keywords
# 2294138 -> author with inspire id
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
    """2684743."""
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
    """2788738."""
    # TODO pre-create user with orcid
    dict_rec = record.to_dict()
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
    """2889522."""
    dict_rec = record.to_dict()
    assert dict_rec["versions"]["index"] == 2


def multiple_versions_with_cs(record):
    """2051872."""
    dict_rec = record.to_dict()
    assert dict_rec["versions"]["index"] == 2
    assert dict_rec["custom_fields"] == {
        "cern:experiments": [{"id": "CMS", "title": {"en": "CMS"}}],
        "cern:departments": [{"id": "PH", "title": {"en": "PH"}}],
    }


def multiple_custom_fields(record):
    """2207587."""
    dict_rec = record.to_dict()
    assert "custom_fields" in dict_rec
    assert dict_rec["custom_fields"] == {
        "cern:accelerators": [
            {"id": "CERN AD", "title": {"en": "CERN AD"}},
            {"id": "CERN AD", "title": {"en": "CERN AD"}},
        ],
    }


def file_missing(record):
    """2783103."""
    dict_rec = record.to_dict()
    assert "files" in dict_rec
    assert dict_rec["files"]["enabled"] == False
    assert dict_rec["files"]["count"] == 0
    assert "media_files" in dict_rec
    assert dict_rec["media_files"]["enabled"] == False
    assert dict_rec["media_files"]["count"] == 0


def file_restricted(record):
    """2783104."""
    dict_rec = record.to_dict()
    assert "access" in dict_rec
    assert dict_rec["access"]["record"] == "public"
    assert dict_rec["access"]["files"] == "restricted"
    assert "files" in dict_rec
    assert dict_rec["files"]["enabled"] == True
    assert dict_rec["files"]["count"] == 1


def irregular_exp_field(record):
    """2046076."""
    dict_rec = record.to_dict()
    assert "custom_fields" in dict_rec
    assert dict_rec["custom_fields"] == {
        "cern:accelerators": [
            {"id": "CERN AD", "title": {"en": "CERN AD"}},
        ],
    }
    assert "subjects" in dict_rec["metadata"]
    assert dict_rec["metadata"]["subjects"] == [
        {
            "id": "Computing and Computers",
            "subject": "Computing and Computers",
            "scheme": "CERN",
        },
        {"subject": "COMPASS"},
        {"subject": "DAQ"},
        {"subject": "FPGA"},
        {"subject": "GUI"},
        {"subject": "COMPASS NA58"},
    ]


def custom_affiliation(record):
    """2041388."""
    dict_rec = record.to_dict()
    for creator in dict_rec["metadata"]["creators"]:
        assert "affiliations" in creator
        for affiliation in creator["affiliations"]:
            assert "ror" != affiliation.get("scheme", None)


def contains_aleph(record):
    """1597985."""
    dict_rec = record.to_dict()
    assert "identifiers" in dict_rec["metadata"]
    assert dict_rec["metadata"]["identifiers"] == [
        {"identifier": "CERN-STUDENTS-Note-2013-181", "scheme": "cds_ref"},
        {"identifier": "000733613CER", "scheme": "aleph"},
    ]


def contains_keywords(record):
    """2779426."""
    dict_rec = record.to_dict()
    assert "subjects" in dict_rec["metadata"]
    assert dict_rec["metadata"]["subjects"] == [
        {
            "id": "Detectors and Experimental Techniques",
            "subject": "Detectors and Experimental Techniques",
            "scheme": "CERN",
        },
        {
            "subject": "Gaseous detectors, Drift tubes, CSC, MSGC, Microdot chambers, Micromegas, Micropattern, GEM, TPC, RPC, TGC, RICH, electron avalanche, Garfield++, Magboltz"
        },
    ]


def author_with_inspire(record):
    """2294138."""
    dict_rec = record.to_dict()
    assert "contributors" in dict_rec["metadata"]
    assert dict_rec["metadata"]["contributors"] == [
        {
            "person_or_org": {
                "type": "personal",
                "name": "Glatzer, Julian",
                "given_name": "Julian",
                "family_name": "Glatzer",
                "identifiers": [
                    {"identifier": "INSPIRE-00013837", "scheme": "inspire"},
                    {"identifier": "2073275", "scheme": "lcds"},
                ],
            },
            "role": {
                "id": "other",
                "title": {"en": "Other"},
            },
            "affiliations": [
                {"name": "Universitat Autonoma de Barcelona ES"},
            ],
        },
    ]


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
        if record["legacy_recid"] == "2207587":
            multiple_custom_fields(loaded_rec)
        if record["legacy_recid"] == "2783103":
            file_missing(loaded_rec)
        if record["legacy_recid"] == "2783104":
            file_restricted(loaded_rec)
        if record["legacy_recid"] == "2046076":
            irregular_exp_field(loaded_rec)
        if record["legacy_recid"] == "2041388":
            custom_affiliation(loaded_rec)
        if record["legacy_recid"] == "1597985":
            contains_aleph(loaded_rec)
        if record["legacy_recid"] == "2779426":
            contains_keywords(loaded_rec)
        if record["legacy_recid"] == "2294138":
            author_with_inspire(loaded_rec)
