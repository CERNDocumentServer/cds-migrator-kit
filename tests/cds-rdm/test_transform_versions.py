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

from cds_migrator_kit.errors import ManualImportRequired
from cds_migrator_kit.rdm.records.load.entities.record import (
    file_is_empty,
    pdf_has_zero_pages,
)
from cds_migrator_kit.rdm.records.transform.config import EMPTY_PDF_CHECKSUM
from cds_migrator_kit.rdm.records.transform.transform import CDSToRDMRecordTransform
from cds_migrator_kit.rdm.records.transform.transform_versions import (
    RecordVersionsTransform,
)


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


def _raw_dump_entry(files):
    """Build a raw dump entry, with the ``restrictions`` the legacy dumper adds.

    Like a legacy BibDoc status, a file's restriction is the status of its
    latest revision; only restricted files are listed.
    """
    statuses = {}
    for file_dump in sorted(files, key=lambda f: f["version"]):
        statuses[file_dump["bibdocid"]] = file_dump["status"]
    return {
        "recid": 123,
        "files": files,
        "restrictions": {
            "files": [
                {"bibdocid": bibdocid, "status": status, "roles": []}
                for bibdocid, status in statuses.items()
                if status
            ]
        },
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
def transform(tmp_path, monkeypatch):
    """Transform instance."""
    monkeypatch.setattr(
        "cds_migrator_kit.rdm.records.transform.transform_versions.current_app",
        SimpleNamespace(config={"DATACITE_PREFIX": "10.17181"}),
    )
    return CDSToRDMRecordTransform(
        files_dump_dir=tmp_path,
        missing_users=tmp_path,
        migration_logger=MagicMock(),
    )


def test_versions_preserve_file_revision_per_record_version(transform):
    """Each record version keeps the file revision."""
    raw_dump_entry = _raw_dump_entry(
        [
            _file_dump(file_version=1, checksum="checksum-v1"),
            _file_dump(file_version=2, checksum="checksum-v2"),
            _file_dump(full_name="test.pdf", file_version=1, checksum="checksum-v3"),
        ]
    )

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
    raw_dump_entry = _raw_dump_entry(
        [
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
        ]
    )

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
    raw_dump_entry = _raw_dump_entry([])

    versions = transform._versions(raw_dump_entry, _record())

    assert list(versions.keys()) == [1]
    assert versions[1]["files"] == {}
    assert versions[1]["publication_date"] == "2020-01-01"
    assert versions[1]["access"] == {"record": "public", "files": "public"}


def test_versions_external_doi_collapses_into_single_version(transform):
    """An external DOI record gets one version with every file, not one per revision."""
    raw_dump_entry = _raw_dump_entry(
        [
            _file_dump(file_version=1, checksum="checksum-v1"),
            _file_dump(file_version=2, checksum="checksum-v2"),
            _file_dump(full_name="test.pdf", file_version=1, checksum="checksum-v3"),
        ]
    )

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
    raw_dump_entry = _raw_dump_entry([])

    versions = transform._versions(raw_dump_entry, _external_doi_record())

    assert list(versions.keys()) == [1]
    assert versions[1]["files"] == {}


def test_versions_individual_file_restriction_sets_files_restricted(transform):
    """A file with its own restriction status flags that version as restricted."""
    status = (
        'firerole: allow group "some-group [CERN]"\ndeny until "1996-02-01"\nallow all'
    )
    raw_dump_entry = _raw_dump_entry([_file_dump(status=status)])

    versions = transform._versions(raw_dump_entry, _record())

    assert versions[1]["access"] == {"record": "public", "files": "restricted"}
    transform.migration_logger.add_information.assert_called_once()
    recid, info = transform.migration_logger.add_information.call_args[0]
    assert recid == "123"
    assert info["message"] == "Record has individual file restrictions"
    assert info["value"] == status


def test_versions_keyword_file_restriction_is_status_form_agnostic(transform):
    """A plain keyword status flows through as restricted.

    compute_access doesn't validate the status against
    CDS_ACCESS_GROUP_MAPPINGS (hence no app context needed here); recognising
    the keyword is resolve_grants' job.
    """
    raw_dump_entry = _raw_dump_entry([_file_dump(status="SSO")])

    versions = transform._versions(raw_dump_entry, _record())

    assert versions[1]["access"] == {"record": "public", "files": "restricted"}


def test_versions_all_public_files_stay_public(transform):
    """A version whose files all have empty status stays fully public."""
    raw_dump_entry = _raw_dump_entry(
        [
            _file_dump(full_name="a.pdf", bibdocid=1, status=""),
            _file_dump(full_name="b.pdf", bibdocid=2, status=""),
        ]
    )

    versions = transform._versions(raw_dump_entry, _record())

    assert set(versions[1]["files"]) == {"a.pdf", "b.pdf"}
    assert versions[1]["access"] == {"record": "public", "files": "public"}
    transform.migration_logger.add_information.assert_not_called()


@pytest.mark.parametrize(
    "files",
    [
        # dump order must not matter
        [("restricted.pdf", "SSO"), ("public.pdf", "")],
        [("public.pdf", ""), ("restricted.pdf", "SSO")],
        # several files sharing the same restriction
        [("public.pdf", ""), ("a.pdf", "SSO"), ("b.pdf", "SSO")],
    ],
)
def test_versions_public_and_single_restriction_mix_is_restricted(transform, files):
    """Public files mixed with files of a single restriction status are allowed.

    The record is split into a public and a restricted record at load time.
    """
    raw_dump_entry = _raw_dump_entry(
        [
            _file_dump(full_name=name, bibdocid=i + 1, status=status)
            for i, (name, status) in enumerate(files)
        ]
    )

    versions = transform._versions(raw_dump_entry, _record())

    assert versions[1]["access"] == {"record": "public", "files": "restricted"}


@pytest.mark.parametrize(
    "files",
    [
        [("a.pdf", "SSO"), ("b.pdf", "ITDepRestrFile")],
        [("public.pdf", ""), ("a.pdf", "SSO"), ("b.pdf", "ITDepRestrFile")],
    ],
)
def test_versions_several_distinct_restrictions_raise(transform, files):
    """Files with more than one distinct restriction status hard-stop.

    At most one restricted and one public record can be created per legacy
    record, so several restrictions can't be represented.
    """
    raw_dump_entry = _raw_dump_entry(
        [
            _file_dump(full_name=name, bibdocid=i + 1, status=status)
            for i, (name, status) in enumerate(files)
        ]
    )

    with pytest.raises(ManualImportRequired):
        transform._versions(raw_dump_entry, _record())


def test_versions_restriction_comes_from_current_file_statuses(transform):
    """Access is evaluated from the files' current (dumped) restrictions.

    Legacy: A v1 restricted + B v1 restricted, then A v2 public (B stays v1).
    Only B is still restricted, so every version has restricted files (the
    public/restricted mix is split at load time).
    """
    raw_dump_entry = _raw_dump_entry(
        [
            _file_dump(
                full_name="A.pdf",
                file_version=1,
                bibdocid=1,
                status="SSO",
                creation_date="2020-01-01T00:00:00+00:00",
            ),
            _file_dump(
                full_name="B.pdf",
                file_version=1,
                bibdocid=2,
                status="SSO",
                creation_date="2020-01-01T00:00:00+00:00",
            ),
            _file_dump(
                full_name="A.pdf",
                file_version=2,
                bibdocid=1,
                status="",
                creation_date="2020-02-01T00:00:00+00:00",
            ),
        ]
    )

    versions = transform._versions(raw_dump_entry, _record())

    assert list(versions.keys()) == [1, 2]
    for version in versions.values():
        assert version["access"] == {"record": "public", "files": "restricted"}


def test_versions_restriction_lifted_on_only_file_is_public(transform):
    """A lone file whose restriction is lifted in a new revision is public.

    The dump only has the file's current restriction (none), so every version,
    including v1, is public.
    """
    raw_dump_entry = _raw_dump_entry(
        [
            _file_dump(
                full_name="A.pdf",
                file_version=1,
                bibdocid=1,
                status="SSO",
                creation_date="2020-01-01T00:00:00+00:00",
            ),
            _file_dump(
                full_name="A.pdf",
                file_version=2,
                bibdocid=1,
                status="",
                creation_date="2020-02-01T00:00:00+00:00",
            ),
        ]
    )

    versions = transform._versions(raw_dump_entry, _record())

    assert versions[1]["access"] == {"record": "public", "files": "public"}
    assert versions[2]["access"] == {"record": "public", "files": "public"}
    assert versions[2]["files"]["A.pdf"]["version"] == 2


def _patch_datacite_prefix(monkeypatch, prefix="10.17181"):
    """Point version DOI helpers at a fixed DATACITE_PREFIX without a full app."""
    monkeypatch.setattr(
        "cds_migrator_kit.rdm.records.transform.transform_versions.current_app",
        SimpleNamespace(config={"DATACITE_PREFIX": prefix}),
    )


def _versions_transform(record, raw_dump_entry, tmp_path):
    """Build a RecordVersionsTransform for DOI-placement unit tests."""
    return RecordVersionsTransform(
        raw_dump_entry=raw_dump_entry,
        record=record,
        files_dump_dir=tmp_path,
        plots=False,
        migration_logger=MagicMock(),
    )


def test_cds_doi_uses_datacite_prefix_not_provider(monkeypatch, tmp_path):
    """CDS DOI detection follows DATACITE_PREFIX, not the provider label."""
    _patch_datacite_prefix(monkeypatch)
    mistyped_provider = {
        "identifier": "10.17181/cds.12345",
        "provider": "external",
    }
    wrong_prefix = {
        "identifier": "10.1234/external",
        "provider": "datacite",
    }
    assert (
        _versions_transform(
            _record(pids={"doi": mistyped_provider}), {"files": []}, tmp_path
        )._cds_doi()
        == mistyped_provider
    )
    assert (
        _versions_transform(
            _record(pids={"doi": wrong_prefix}), {"files": []}, tmp_path
        )._cds_doi()
        is None
    )
    assert (
        _versions_transform(
            _record(pids={"doi": CDS_DOI}), {"files": []}, tmp_path
        )._cds_doi()
        == CDS_DOI
    )


def test_cds_doi_stays_on_body_for_single_version(monkeypatch, transform):
    """Single-version CDS DOI records keep the DOI on the shared body."""
    _patch_datacite_prefix(monkeypatch)
    record = _record(pids={"doi": CDS_DOI})
    raw_dump_entry = _raw_dump_entry([_file_dump(full_name="main.pdf", file_version=1)])

    versions = transform._versions(raw_dump_entry, record)

    assert list(versions) == [1]
    assert record.body["pids"]["doi"] == CDS_DOI
    assert "pids" not in versions[1]


def test_cds_doi_moves_to_latest_version_when_multiple(monkeypatch, transform):
    """Multi-version CDS DOI is stripped from the body and set on latest only."""
    _patch_datacite_prefix(monkeypatch)
    record = _record(pids={"doi": CDS_DOI})
    raw_dump_entry = _raw_dump_entry(
        [
            _file_dump(full_name="main.pdf", file_version=1),
            _file_dump(
                full_name="main.pdf",
                file_version=2,
                creation_date="2020-02-01T00:00:00+00:00",
            ),
        ]
    )

    versions = transform._versions(raw_dump_entry, record)

    assert list(versions) == [1, 2]
    assert "doi" not in record.body.get("pids", {})
    assert "pids" not in versions[1]
    assert versions[2]["pids"]["doi"] == CDS_DOI


def test_external_doi_stays_on_body_when_versions_collapsed(monkeypatch, transform):
    """External DOI collapse keeps a single version and leaves the DOI on body."""
    _patch_datacite_prefix(monkeypatch)
    record = _external_doi_record()
    raw_dump_entry = _raw_dump_entry(
        [
            _file_dump(full_name="main.pdf", file_version=1),
            _file_dump(
                full_name="main.pdf",
                file_version=2,
                creation_date="2020-02-01T00:00:00+00:00",
            ),
        ]
    )

    versions = transform._versions(raw_dump_entry, record)

    assert list(versions) == [2]
    assert record.body["pids"]["doi"] == EXTERNAL_DOI
    assert "pids" not in versions[2]


def test_known_corrupt_checksum_raises_manual_import(transform):
    """Known empty/corrupt stub checksum fails for curator review."""
    raw_dump_entry = _raw_dump_entry(
        [
            _file_dump(
                full_name="cer-002523122.pdf",
                checksum=EMPTY_PDF_CHECKSUM,
            ),
            # pdfa sibling is still dropped; we fail instead of falling back to it
            _file_dump(
                full_name="cer-002523122.pdf",
                file_version=2,
                subformat="pdfa",
                checksum="valid-pdfa",
            ),
        ]
    )

    with pytest.raises(ManualImportRequired) as exc_info:
        transform._versions(raw_dump_entry, _record())

    assert exc_info.value.recid == "123"
    assert exc_info.value.value == "cer-002523122.pdf"
    assert "curator review required" in exc_info.value.message


def test_load_detects_empty_file_and_pdf_from_disk(tmp_path):
    """Load checks use the file on disk, not legacy mimetype metadata."""
    empty = tmp_path / "empty.txt"
    empty.write_bytes(b"")
    pdf = tmp_path / "stub.bin"
    pdf.write_bytes(b"%PDF-1.4\n1 0 obj\n<< /Type /Pages /Count 0 >>\nendobj\n")
    not_pdf = tmp_path / "notes.pdf"
    not_pdf.write_bytes(b"plain text")

    assert file_is_empty(empty) is True
    assert file_is_empty(pdf) is False
    assert pdf_has_zero_pages(pdf) is True
    assert pdf_has_zero_pages(not_pdf) is False
