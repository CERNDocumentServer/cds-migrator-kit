# 2788738 -> user with orcid
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
from datetime import datetime, timezone
from pathlib import Path

import pytest_mock
from cds_rdm.legacy.models import CDSMigrationLegacyRecord
from flask import current_app
from invenio_access.permissions import system_identity
from invenio_accounts.models import User, UserIdentity
from invenio_oauthclient.models import RemoteAccount
from invenio_rdm_records.proxies import current_rdm_records_service
from invenio_records_resources.proxies import current_service_registry
from invenio_vocabularies.contrib.names.api import Name
from invenio_vocabularies.contrib.names.models import NamesMetadata

from cds_migrator_kit.rdm.records.streams import RecordStreamDefinition
from cds_migrator_kit.rdm.users.runner import SubmitterRunner
from cds_migrator_kit.rdm.users.streams import SubmitterStreamDefinition
from cds_migrator_kit.runner.runner import Runner


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
            "affiliations": [
                {"name": "The Barcelona Institute of Science and Technology " "BIST ES"}
            ],
            "person_or_org": {
                "family_name": "Casolino",
                "given_name": "Mirkoantonio",
                "identifiers": [
                    {"identifier": "INSPIRE-00366594", "scheme": "inspire"},
                    {"identifier": "2083412", "scheme": "lcds"},
                ],
                "name": "Casolino, Mirkoantonio",
                "type": "personal",
            },
            "role": {"id": "other", "title": {"en": "Other"}},
        },
        {
            "affiliations": [{"name": "CERN"}],
            "person_or_org": {
                "family_name": "Ahdida",
                "given_name": "Claudia Christina",
                "identifiers": [{"identifier": "2087282", "scheme": "lcds"}],
                "name": "Ahdida, Claudia Christina",
                "type": "personal",
            },
            "role": {"id": "other", "title": {"en": "Other"}},
        },
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


def orcid_id(record, orcid_name_data):
    """2788738."""
    dict_rec = record.to_dict()
    assert dict_rec["metadata"]["creators"] == [
        {
            "person_or_org": {
                "type": "personal",
                "name": "Mendoza, Diego",
                "given_name": "Diego",
                "family_name": "Mendoza",
                "identifiers": [
                    {"identifier": "2773374", "scheme": "lcds"},
                    {"identifier": "0009-0007-7638-4652", "scheme": "orcid"},
                ],
            }
        }
    ]

    name_from_db = NamesMetadata.query.filter_by(pid=orcid_name_data["id"]).one()
    assert "identifiers" in name_from_db.json
    orcid_identifier = orcid_name_data["identifiers"][0]
    assert orcid_identifier in name_from_db.json["identifiers"]

    user_identity = UserIdentity.query.filter_by(
        id=name_from_db.internal_id, method="cern"
    ).one()
    assert user_identity is not None

    remote_account = RemoteAccount.query.filter_by(user_id=user_identity.id_user).one()
    assert hasattr(remote_account, "extra_data")
    assert remote_account.extra_data == {
        "migration": {
            "source": "PEOPLE COLLECTION, PERSON_ID FOUND",
            "note": "MIGRATED INACTIVE ACCOUNT",
        }
    }

    for identifier in dict_rec["metadata"]["creators"][0]["person_or_org"][
        "identifiers"
    ]:
        identifier_scheme = identifier.get("scheme")
        assert "cern" != identifier_scheme
        if identifier_scheme in current_app.config["VOCABULARIES_NAMES_SCHEMES"]:
            assert identifier in name_from_db.json["identifiers"]


def multiple_versions(record, record_state):
    """2889522."""
    dict_rec = record.to_dict()
    for record_version in record_state["versions"]:
        if record_version["version"] == 1:
            first_version = current_rdm_records_service.read(
                system_identity, record_version["new_recid"]
            )
            dict_first_version = first_version.to_dict()
            # It matches record created date instead of the file creation date
            assert dict_first_version["created"] == "2024-02-19T00:00:00+00:00"

    assert dict_rec["versions"]["index"] == 2
    # Check that the record creation date matches the files creation date
    assert dict_rec["created"] == "2024-02-19T12:47:01+00:00"


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
    # Assert that the creation date matches the file creation date
    # as the record's one is missing
    assert dict_rec["created"] == "2017-11-23T16:06:45+00:00"
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


def check_log_for_error(record_id, target_error):
    """Checks if the log contains the specified error for the specified record ID."""
    with open("tests/cds-rdm/tmp/logs/sspn/rdm_migration_errors.csv", "r") as file:
        lines = file.readlines()

        for line in lines:
            if record_id in line and target_error in line:
                return True

        return False


def test_full_migration_stream(
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
                }
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
    runner = Runner(
        stream_definitions=[RecordStreamDefinition],
        config_filepath=Path(stream_config).absolute(),
        dry_run=False,
        collection="sspn",
    )
    runner.run()

    assert CDSMigrationLegacyRecord.query.count() == 12

    with open("tests/cds-rdm/tmp/logs/sspn/rdm_records_state.json") as state_logs:
        records = json.load(state_logs)

    assert check_log_for_error("2783112", "ManualImportRequired")

    for record in records:

        loaded_rec = current_rdm_records_service.read(
            system_identity, record["latest_version"]
        )
        if record["legacy_recid"] == "2684743":
            suite_multi_field(loaded_rec)
        if record["legacy_recid"] == "2788738":
            orcid_id(loaded_rec, orcid_name_data)
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

    # Check if remote account has the correct metadata
    remote_account_metadata()


def remote_account_metadata():
    """Checks for remote account extra_data."""
    user = User.query.filter_by(email="submitter13@cern.ch").one()
    assert user is not None

    remote_account = RemoteAccount.query.filter_by(user_id=user.id).one()
    assert hasattr(remote_account, "extra_data")
    assert remote_account.extra_data == {
        "migration": {
            "source": "LEGACY DB, PERSON ID MISSING",
            "note": "MIGRATED INACTIVE ACCOUNT",
        }
    }

    user = User.query.filter_by(email="submitter16@cern.ch").one()
    assert user is not None

    remote_account = RemoteAccount.query.filter_by(user_id=user.id).one()
    assert hasattr(remote_account, "extra_data")
    assert remote_account.extra_data == {
        "migration": {
            "source": "RECORD, EMAIL NOT FOUND IN ANY SOURCE",
            "note": "MIGRATED INACTIVE ACCOUNT",
        }
    }

    user = User.query.filter_by(email="submitter11@cern.ch").one()
    assert user is not None

    remote_account = RemoteAccount.query.filter_by(user_id=user.id).one()
    assert hasattr(remote_account, "extra_data")
    assert remote_account.extra_data == {
        "migration": {
            "source": "PEOPLE COLLECTION, PERSON_ID NOT FOUND",
            "note": "MIGRATED INACTIVE ACCOUNT",
        }
    }

    user = User.query.filter_by(email="submitter15@cern.ch").one()
    assert user is not None

    remote_account = RemoteAccount.query.filter_by(user_id=user.id).one()
    assert hasattr(remote_account, "extra_data")
    assert remote_account.extra_data == {
        "migration": {
            "source": "PEOPLE COLLECTION, PERSON_ID FOUND",
            "note": "MIGRATED INACTIVE ACCOUNT",
        }
    }
