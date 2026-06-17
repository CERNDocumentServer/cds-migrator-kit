# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Tests for record version file snapshot logic in transform._versions()."""

from unittest.mock import MagicMock

import pytest

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
):
    """Build a minimal legacy file dump entry."""
    checksum = checksum or f"checksum-v{file_version}"
    return {
        "comment": None,
        "status": "",
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


def _record():
    """Build a minimal record."""
    return {
        "access": "public",
        "json": {"metadata": {"publication_date": "2020-01-01"}},
    }


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
    entry = {
        "recid": 123,
        "files": [
            _file_dump(file_version=1, checksum="checksum-v1"),
            _file_dump(file_version=2, checksum="checksum-v2"),
            _file_dump(full_name="test.pdf", file_version=1, checksum="checksum-v3"),
        ],
    }

    versions = transform._versions(entry, _record())

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
    entry = {
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

    versions = transform._versions(entry, _record())

    assert list(versions.keys()) == [1, 2]
    assert set(versions[1]["files"]) == {"main.pdf"}
    assert set(versions[2]["files"]) == {"main.pdf"}
    assert versions[1]["files"]["main.pdf"]["version"] == 1
    assert versions[2]["files"]["main.pdf"]["version"] == 2
    assert "plot.png" not in versions[1]["files"]
    assert "plot.png" not in versions[2]["files"]
