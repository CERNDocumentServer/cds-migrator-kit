# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Who can see what after migrating legacy records with mixed restrictions.

Each test:

1. describes a legacy record with ``legacy_record()``: only the parts that
   matter for access (files and their statuses, owners, viewers, ...), on top
   of a fixed minimal MARC record;
2. migrates it with ``migrate()``: the real transform and load steps, as in a
   migration run, into the test database;
3. compares ``summarize()`` of each resulting RDM record with the expected
   access, exactly - so that any extra grant, file or community fails the test.

A record with both public and restricted files is split into a public record
and a restricted record, which link to each other with ``isvariantformof``.
"""

from unittest.mock import MagicMock

import pytest
from invenio_access.permissions import system_identity
from invenio_accounts.models import User
from invenio_rdm_records.proxies import current_rdm_records_service

from cds_migrator_kit.errors import ManualImportRequired
from cds_migrator_kit.rdm.records.load.entities.split_migration_entry_load import (
    SplitMigrationEntryLoad,
)
from cds_migrator_kit.rdm.records.transform.entities import (
    parent as transform_parent_module,
)
from cds_migrator_kit.rdm.records.transform.entities.community_members import (
    community_subjects,
)
from cds_migrator_kit.rdm.records.transform.transform import CDSToRDMRecordTransform

RECID = 1000001
LEGACY_FILES_ROOT = "/opt/cdsweb/var/data/files"
# The content of every file, and its checksum, which the load step verifies.
FILE_CONTENT = b"test"
FILE_CHECKSUM = "098f6bcd4621d373cade4e832627b4f6"

# A minimal legacy summer student note, which only the summer student report
# model matches.
MARC_TEMPLATE = """<record>
  <controlfield tag="001">{recid}</controlfield>
  <datafield tag="037" ind1=" " ind2=" ">
    <subfield code="a">CERN-STUDENTS-Note-2021-001</subfield>
  </datafield>
  <datafield tag="100" ind1=" " ind2=" ">
    <subfield code="a">Doe, Jane</subfield>
  </datafield>
  <datafield tag="245" ind1=" " ind2=" ">
    <subfield code="a">A record with mixed restrictions</subfield>
  </datafield>
  <datafield tag="269" ind1=" " ind2=" ">
    <subfield code="c">04 Oct 2021</subfield>
  </datafield>
  <datafield tag="916" ind1=" " ind2=" ">
    <subfield code="s">n</subfield>
    <subfield code="w">202140</subfield>
  </datafield>
  <datafield tag="980" ind1=" " ind2=" ">
    <subfield code="a">NOTE</subfield>
  </datafield>
</record>"""


def legacy_file(name, status="", bibdocid=None):
    """A file of a legacy record; ``status`` is empty for public files."""
    return {"full_name": name, "status": status, "bibdocid": bibdocid}


def legacy_record(files, owners=(), viewers=(), restricted_collections=None):
    """A legacy record dump, as exported by invenio-migrator.

    :param files: ``legacy_file()`` entries.
    :param owners: emails of the record owners (``859__f``).
    :param viewers: emails or groups allowed to view the record (``506__m``).
    :param restricted_collections: restricted collections of the record, with
        the legacy roles allowed to view each of them.
    """
    file_dumps = []
    for index, file in enumerate(files):
        bibdocid = file["bibdocid"] or index + 1
        file_dumps.append(
            {
                "recid": RECID,
                "bibdocid": bibdocid,
                "full_name": file["full_name"],
                "name": file["full_name"].removesuffix(".pdf"),
                "status": file["status"],
                "version": 1,
                "type": "Main",
                "hidden": False,
                "subformat": "",
                "format": ".pdf",
                "superformat": ".pdf",
                "mime": "application/pdf",
                "checksum": FILE_CHECKSUM,
                "size": 4,
                "creation_date": "2021-10-04T12:00:00+00:00",
                "modification_date": "2021-10-04T12:00:00+00:00",
                "full_path": f"{LEGACY_FILES_ROOT}/g0/{bibdocid}/content.pdf;1",
                "path": f"{LEGACY_FILES_ROOT}/g0/{bibdocid}/content.pdf;1",
                "url": f"http://cds.cern.ch/record/{RECID}/files/{file['full_name']}",
                "recids_doctype": [[RECID, "Main", file["full_name"]]],
                "comment": None,
                "description": None,
                "encoding": None,
                "flags": [],
                "license": {},
                "copyright": {},
            }
        )

    return {
        "recid": RECID,
        "creation_date": "2021-10-04 12:00:00",
        "collections": None,
        "record": [
            {
                "json": None,
                "marcxml": MARC_TEMPLATE.format(recid=RECID),
                "modification_datetime": "2021-10-04T12:00:00+00:00",
            }
        ],
        "files": file_dumps,
        "restrictions": {
            "collections": restricted_collections or {},
            "files": [
                {"bibdocid": f["bibdocid"], "status": f["status"], "roles": []}
                for f in file_dumps
                if f["status"]
            ],
            "record": {
                "owners": {"859__f": list(owners), "270__m": []},
                "viewers": {"506__m": list(viewers), "5061_d": []},
            },
            "edit": {"collections": {}, "any_collection": []},
            "comments": {
                "primary_collection": "",
                "view": {"roles": [], "any_collection": []},
                "send": {"roles": [], "any_collection": []},
                "record_restriction": "",
                "restricted_comments": [],
            },
        },
    }


@pytest.fixture()
def migrate(test_app, community, groups, search_clear, tmp_path):
    """Return a function migrating a legacy record, see the module docstring.

    It returns the migrated RDM records: ``[record]``, or ``[public,
    restricted]`` when the record was split. If the migration of the record
    fails, the error is raised instead of only being logged.
    """
    # Module-level caches, which would otherwise leak users and members
    # between tests.
    transform_parent_module._submitter_id_by_email.clear()
    community_subjects.cache_clear()

    def raise_error(error, record=None):
        raise error

    migration_logger = MagicMock()
    migration_logger.add_log.side_effect = raise_error
    record_state_logger = MagicMock()

    def _migrate(legacy, target_community=community):
        for file in legacy["files"]:
            path = tmp_path / file["full_path"].removeprefix(LEGACY_FILES_ROOT + "/")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(FILE_CONTENT)

        transform = CDSToRDMRecordTransform(
            files_dump_dir=tmp_path,
            communities_ids=[str(target_community.id)],
            migration_logger=migration_logger,
            record_state_logger=record_state_logger,
        )
        load = SplitMigrationEntryLoad(
            migration_logger=migration_logger,
            record_state_logger=record_state_logger,
        )
        load._load(transform._transform(legacy))

        state = record_state_logger.add_record_state.call_args.args[0]
        record_ids = [state["latest_version"]]
        if "internal_version" in state:
            record_ids.append(state["internal_version"])
        return [
            current_rdm_records_service.read(system_identity, record_id)
            for record_id in record_ids
        ]

    return _migrate


def summarize(record):
    """The access-related parts of a migrated RDM record, as plain values."""
    record = current_rdm_records_service.read(system_identity, record.id)
    parent = record._record.parent

    def email(user_id):
        return User.query.get(int(user_id)).email

    owner = parent.access.owned_by.owner_id
    grants = []
    for grant in parent.access.grants:
        subject = grant.subject_id
        if grant.subject_type == "user":
            subject = email(subject)
        grants.append((grant.subject_type, subject, grant.permission))

    return {
        "access": {
            "record": record.data["access"]["record"],
            "files": record.data["access"]["files"],
        },
        "files": sorted(record.data["files"]["entries"]),
        "owner": owner if owner == "system" else email(owner),
        "grants": set(grants),
        "communities": set(parent.communities.ids),
        "related": [
            (related["relation_type"]["id"], related["identifier"])
            for related in record.data["metadata"].get("related_identifiers", [])
        ],
    }


def test_public_and_sso_files(migrate, uploader, community, cern_scientific_community):
    """The SSO file goes to a restricted record, viewable by CERN personnel."""
    legacy = legacy_record(
        files=[
            legacy_file("public.pdf"),
            legacy_file("secret.pdf", status="SSO"),
        ],
        owners=[uploader.email],
    )

    public, restricted = migrate(legacy)

    assert summarize(public) == {
        "access": {"record": "public", "files": "public"},
        "files": ["public.pdf"],
        "owner": uploader.email,
        "grants": set(),
        "communities": {str(community.id), str(cern_scientific_community.id)},
        "related": [("isvariantformof", restricted.id)],
    }
    assert summarize(restricted) == {
        "access": {"record": "restricted", "files": "restricted"},
        "files": ["secret.pdf"],
        "owner": uploader.email,
        "grants": {("role", "cern-personnel", "view")},
        "communities": {str(community.id)},
        "related": [("isvariantformof", public.id)],
    }


def test_only_public_files(migrate, uploader, community, cern_scientific_community):
    """The record should not get split, only the public one should get created."""
    legacy = legacy_record(
        files=[
            legacy_file("public.pdf"),
        ],
        owners=[uploader.email],
    )

    migrated_records = migrate(legacy)
    assert len(migrated_records) == 1
    public = migrated_records[0]

    assert summarize(public) == {
        "access": {"record": "public", "files": "public"},
        "files": ["public.pdf"],
        "owner": uploader.email,
        "grants": set(),
        "communities": {str(community.id), str(cern_scientific_community.id)},
        "related": [],
    }


def test_only_restricted_files(migrate, uploader, community):
    """The record should not get split, only the restricted one should get created."""
    legacy = legacy_record(
        files=[
            legacy_file("secret.pdf", status="SSO"),
        ],
        owners=[uploader.email],
    )

    migrated_records = migrate(legacy)
    assert len(migrated_records) == 1
    restricted = migrated_records[0]

    assert summarize(restricted) == {
        "access": {"record": "public", "files": "restricted"},
        "files": ["secret.pdf"],
        "owner": uploader.email,
        "grants": {("role", "cern-personnel", "view")},
        "communities": {str(community.id)},
        "related": [],
    }


def test_fails_public_in_restricted_community(migrate, uploader, restricted_community):
    legacy = legacy_record(
        files=[
            legacy_file("public.pdf"),
            legacy_file("secret.pdf", status="SSO"),
        ],
        owners=[uploader.email],
    )

    with pytest.raises(ManualImportRequired) as exc:
        migrate(legacy, target_community=restricted_community)

    assert (
        exc.value.message is not None
        and "added to at least one restricted community" in exc.value.message
    )


def test_grants_in_restricted_community(
    migrate, uploader, restricted_community, normal_user
):
    legacy = legacy_record(
        files=[
            legacy_file("no_status.pdf"),
        ],
        owners=[uploader.email],
        viewers=[normal_user.email],
        restricted_collections={
            "Example Collection": [
                {
                    "id": "example-role",
                    "name": "Example role",
                    "description": "Example description",
                    "firerole_def_src": 'allow group "example-group"',
                    "users": [],
                }
            ]
        },
    )

    migrated_records = migrate(legacy, target_community=restricted_community)

    assert len(migrated_records) == 1
    restricted = migrated_records[0]

    assert summarize(restricted) == {
        "access": {"record": "restricted", "files": "restricted"},
        "files": ["no_status.pdf"],
        "owner": uploader.email,
        "grants": {
            ("user", normal_user.email, "view"),
        },
        "communities": {str(restricted_community.id)},
        "related": [],
    }
