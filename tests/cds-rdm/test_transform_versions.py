# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Tests for record version file snapshot logic in transform._versions()."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from cds_migrator_kit.rdm.records.load.entities.record import RecordLoad
from cds_migrator_kit.rdm.records.transform.transform import CDSToRDMRecordTransform


def _file_dump(
    *,
    full_name="draft.pdf",
    file_version=1,
    file_type="Main",
    creation_date="2020-01-01T00:00:00+00:00",
    checksum=None,
    hidden=False,
    subformat="",
    recid=123,
    bibdocid=1,
    status="",
):
    """Build a minimal legacy file dump entry."""
    checksum = checksum or f"checksum-v{file_version}"
    return {
        "comment": None,
        "status": status,
        "version": file_version,
        "encoding": None,
        "creation_date": creation_date,
        "bibdocid": bibdocid,
        "mime": "application/pdf",
        "full_name": full_name,
        "superformat": ".pdf",
        "recids_doctype": [[recid, file_type, full_name]],
        "path": (
            f"/opt/cdsweb/var/data/files/g{bibdocid}/{bibdocid}/"
            f"content.pdf;{file_version}"
        ),
        "size": 1000,
        "license": {},
        "modification_date": creation_date,
        "copyright": {},
        "url": f"http://cds.cern.ch/record/{recid}/files/{full_name}",
        "checksum": checksum,
        "description": None,
        "format": ".pdf",
        "name": full_name.rsplit(".", 1)[0],
        "subformat": subformat,
        "etag": f'"{bibdocid}.pdf{file_version}"',
        "recid": recid,
        "flags": [],
        "hidden": hidden,
        "type": file_type,
        "full_path": (
            f"/opt/cdsweb/var/data/files/g{bibdocid}/{bibdocid}/"
            f"content.pdf;{file_version}"
        ),
    }


def _record(pids=None):
    """Build a minimal RecordEntry-shaped test double."""
    body = {"metadata": {"publication_date": "2020-01-01"}}
    if pids:
        body["pids"] = pids
    return SimpleNamespace(access_status="public", body=body)


CDS_DOI = {
    "identifier": "10.17181/cds.12345",
    "provider": "datacite",
}
EXTERNAL_DOI = {
    "identifier": "10.1234/external",
    "provider": "external",
}


def _external_doi_record():
    """A record double whose DOI provider is "external" (not our prefix)."""
    return _record(pids={"doi": EXTERNAL_DOI})


@pytest.fixture
def transform(tmp_path):
    """Transform instance."""
    return CDSToRDMRecordTransform(
        files_dump_dir=tmp_path,
        missing_users=tmp_path,
        migration_logger=MagicMock(),
    )


def test_versions_preserve_file_revision_per_record_version(transform):
    """Each record version keeps the file revision."""
    raw_dump_entry = {
        "recid": 123,
        "files": [
            _file_dump(file_version=1, checksum="checksum-v1"),
            _file_dump(file_version=2, checksum="checksum-v2"),
            _file_dump(full_name="test.pdf", file_version=1, checksum="checksum-v3"),
        ],
    }

    versions = transform._versions(raw_dump_entry, _record())

    assert list(versions.keys()) == [1, 2]
    assert versions[1]["files"]["draft.pdf"]["version"] == 1
    assert versions[1]["files"]["draft.pdf"]["checksum"] == "checksum-v1"
    assert versions[1]["files"]["test.pdf"]["version"] == 1
    assert versions[2]["files"]["draft.pdf"]["version"] == 2
    assert versions[2]["files"]["test.pdf"]["version"] == 1
    assert versions[2]["files"]["draft.pdf"]["checksum"] == "checksum-v2"
    assert versions[1]["files"] is not versions[2]["files"]


def test_versions_with_skipped_files(transform):
    """Versions with skipped files should not create extra record versions."""
    raw_dump_entry = {
        "recid": 123,
        "files": [
            _file_dump(
                full_name="main.pdf",
                file_version=1,
                bibdocid=10,
                creation_date="2020-01-01T00:00:00+00:00",
            ),
            _file_dump(
                full_name="main.pdf",
                file_version=2,
                bibdocid=10,
                creation_date="2020-01-02T00:00:00+00:00",
            ),
            _file_dump(
                full_name="plot.png",
                file_version=1,
                file_type="Plot",
                bibdocid=20,
                creation_date="2020-01-03T00:00:00+00:00",
            ),
            _file_dump(
                full_name="plot.png",
                file_version=2,
                file_type="Plot",
                bibdocid=20,
                creation_date="2020-01-04T00:00:00+00:00",
            ),
            _file_dump(
                full_name="plot.png",
                file_version=3,
                file_type="Plot",
                bibdocid=20,
                creation_date="2020-01-05T00:00:00+00:00",
            ),
            _file_dump(
                full_name="plot.png",
                file_version=4,
                file_type="Plot",
                bibdocid=20,
                creation_date="2020-01-06T00:00:00+00:00",
            ),
        ],
    }

    versions = transform._versions(raw_dump_entry, _record())

    assert list(versions.keys()) == [1, 2]
    assert set(versions[1]["files"]) == {"main.pdf"}
    assert set(versions[2]["files"]) == {"main.pdf"}
    assert versions[1]["files"]["main.pdf"]["version"] == 1
    assert versions[2]["files"]["main.pdf"]["version"] == 2
    assert "plot.png" not in versions[1]["files"]
    assert "plot.png" not in versions[2]["files"]


def test_versions_no_files_falls_back_to_metadata_only_version(transform):
    """A record with no files gets a single, empty, public version."""
    raw_dump_entry = {"recid": 123, "files": []}

    versions = transform._versions(raw_dump_entry, _record())

    assert list(versions.keys()) == [1]
    assert versions[1]["files"] == {}
    assert versions[1]["publication_date"] == "2020-01-01"
    assert versions[1]["access"] == {
        "access_obj": {"record": "public", "files": "public"}
    }


def test_versions_external_doi_collapses_into_single_version(transform):
    """An external DOI record gets one version with every file, not one per revision."""
    raw_dump_entry = {
        "recid": 123,
        "files": [
            _file_dump(file_version=1, checksum="checksum-v1"),
            _file_dump(file_version=2, checksum="checksum-v2"),
            _file_dump(full_name="test.pdf", file_version=1, checksum="checksum-v3"),
        ],
    }

    versions = transform._versions(raw_dump_entry, _external_doi_record())

    assert list(versions.keys()) == [2]
    assert set(versions[2]["files"]) == {"draft.pdf", "test.pdf"}
    # the latest revision of a same-named file wins, same as the regular
    # cross-version carry-forward would produce
    assert versions[2]["files"]["draft.pdf"]["version"] == 2
    assert versions[2]["files"]["draft.pdf"]["checksum"] == "checksum-v2"
    assert versions[2]["files"]["test.pdf"]["version"] == 1


def test_versions_external_doi_with_no_files_falls_back_to_metadata_only(transform):
    """An external DOI record with no files still gets the metadata-only fallback."""
    raw_dump_entry = {"recid": 123, "files": []}

    versions = transform._versions(raw_dump_entry, _external_doi_record())

    assert list(versions.keys()) == [1]
    assert versions[1]["files"] == {}


def test_versions_individual_file_restriction_sets_access_meta(transform):
    """A file with its own restriction status flags that version as restricted."""
    status = (
        'firerole: allow group "some-group [CERN]"\ndeny until "1996-02-01"\nallow all'
    )
    raw_dump_entry = {
        "recid": 123,
        "files": [_file_dump(status=status)],
    }

    versions = transform._versions(raw_dump_entry, _record())

    assert versions[1]["access"] == {
        "access_obj": {"record": "public", "files": "restricted"},
        "meta": status,
    }
    transform.migration_logger.add_information.assert_called_once()
    recid, info = transform.migration_logger.add_information.call_args[0]
    assert recid == "123"
    assert info["message"] == "Record has individual file restrictions"
    assert info["value"] == status


def _record_load(pids):
    """Build a RecordLoad with only the record body needed for DOI helpers."""
    return RecordLoad(
        record_entry=SimpleNamespace(
            body={"pids": pids, "metadata": {"publication_date": "2020-01-01"}},
            recid="123",
        ),
        record_parent=MagicMock(),
        migration_logger=MagicMock(),
    )


def test_create_payload_keeps_cds_doi_for_single_version():
    """Single-version CDS DOI records keep the DOI on create (#628)."""
    payload = _record_load({"doi": CDS_DOI})._create_payload(versions={1: {}})
    assert payload["pids"]["doi"] == CDS_DOI


def test_create_payload_strips_cds_doi_when_multiple_versions():
    """Multi-version CDS DOI records strip the DOI from the first create (#628)."""
    payload = _record_load({"doi": CDS_DOI})._create_payload(versions={1: {}, 2: {}})
    assert "doi" not in payload.get("pids", {})


def test_put_cds_doi_only_on_latest_of_multiple_versions(monkeypatch):
    """CDS DOI is attached only when publishing the latest version (#628)."""
    load = _record_load({"doi": CDS_DOI})
    draft = MagicMock()
    draft.to_dict.return_value = {"id": "draft-id", "pids": {}, "metadata": {}}
    draft.__getitem__.side_effect = lambda key: "draft-id"

    identity = MagicMock()
    uow = MagicMock()
    service = MagicMock()
    monkeypatch.setattr(
        "cds_migrator_kit.rdm.records.load.entities.record.current_rdm_records_service",
        service,
    )

    out = load._put_cds_doi_on_latest_draft(
        identity, draft, versions={1: {}, 2: {}}, version=1, uow=uow
    )
    assert out is draft
    service.update_draft.assert_not_called()

    load._put_cds_doi_on_latest_draft(
        identity, draft, versions={1: {}, 2: {}}, version=2, uow=uow
    )
    service.update_draft.assert_called_once()
    assert service.update_draft.call_args.kwargs["data"]["pids"]["doi"] == CDS_DOI
