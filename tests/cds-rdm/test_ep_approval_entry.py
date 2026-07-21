# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Tests for EP approval entry splitting (PublicEntry / RestrictedEntry)."""

from collections import OrderedDict
from copy import deepcopy
from unittest.mock import MagicMock

import pytest

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.rdm.migration_config import CDS_CERN_SCIENTIFIC_COMMUNITY_ID
from cds_migrator_kit.rdm.records.load.ep_approval_entry import (
    EPPHAPP_FILE_TYPE,
    PublicEntry,
    RestrictedEntry,
)

RECID = "12345"
APPROVED_REPORT_NUMBER = "CERN-EP-2020-001"
DRAFT_REPORT_NUMBER = "CERN-EP-DRAFT-TEST-2020-001"
PUBLIC_FILE_KEY = "main.pdf"
DRAFT_FILE_KEY = "draft.pdf"


def _make_approval_request(report_number=APPROVED_REPORT_NUMBER):
    ar = MagicMock()
    ar.report_number = report_number
    ar.resource_type = {"id": "publication-article"}
    return ar


def _make_migration_logger():
    return MagicMock()


def _public_file(key=PUBLIC_FILE_KEY, checksum="aaa", version=1, id_bibdoc=100):
    return {
        "key": key,
        "checksum": checksum,
        "version": version,
        "id_bibdoc": id_bibdoc,
        "access": "",
        "type": "Main",
        "creation_date": "2020-01-15",
    }


def _epphapp_file(key=DRAFT_FILE_KEY, checksum="bbb", version=1, id_bibdoc=200):
    return {
        "key": key,
        "checksum": checksum,
        "version": version,
        "id_bibdoc": id_bibdoc,
        "access": "EP Restricted Draft",
        "type": EPPHAPP_FILE_TYPE,
        "creation_date": "2020-01-10",
    }


def _make_entry(
    versions,
    recid=RECID,
    identifiers=None,
    has_doi=False,
    report_number=APPROVED_REPORT_NUMBER,
):
    """Build a minimal entry dict for testing."""
    if identifiers is None:
        identifiers = [
            {"identifier": recid, "scheme": "cds"},
            {"scheme": "cdsrn", "identifier": report_number},
            {"scheme": "cdsrn", "identifier": DRAFT_REPORT_NUMBER},
        ]

    record_json = {
        "metadata": {
            "creators": [
                {
                    "person_or_org": {
                        "type": "personal",
                        "family_name": "Smith",
                        "given_name": "Alice",
                    },
                    "affiliations": [{"name": "Example University"}],
                }
            ],
            "title": "Example publication title",
            "resource_type": {"id": "publication-article"},
            "description": "Example description.",
            "publication_date": "2020-01-15",
            "identifiers": identifiers,
        },
    }
    if has_doi:
        record_json["pids"] = {
            "doi": {
                "identifier": "10.1234/example-doi",
                "provider": "external",
            }
        }

    return {
        "record": {
            "recid": recid,
            "json": record_json,
            "ep_approval": [
                {
                    "status": "waiting",
                    "ep_report_number": report_number,
                },
                {
                    "status": "approved",
                    "ep_report_number": report_number,
                },
            ],
            "owned_by": "uploader",
            "_request_data": {"placeholder": True},
        },
        "parent": {
            "json": {
                "access": {"owned_by": {"user": "uploader"}},
                "communities": {"ids": ["example-community"]},
            }
        },
        "versions": versions,
    }


def _versions_with_epphapp():
    """Four legacy versions: draft file changes, public file stays the same."""
    return OrderedDict(
        [
            (
                1,
                {
                    "files": {
                        DRAFT_FILE_KEY: _epphapp_file(checksum="draft-v1", version=1),
                        PUBLIC_FILE_KEY: _public_file(checksum="main-v1"),
                    },
                    "publication_date": "2020-01-10",
                    "access": {
                        "access_obj": {"record": None, "files": "restricted"},
                        "meta": "EP Restricted Draft",
                    },
                },
            ),
            (
                2,
                {
                    "files": {
                        DRAFT_FILE_KEY: _epphapp_file(checksum="draft-v2", version=2),
                        PUBLIC_FILE_KEY: _public_file(checksum="main-v1"),
                    },
                    "publication_date": "2020-01-12",
                    "access": {
                        "access_obj": {"record": None, "files": "restricted"},
                        "meta": "EP Restricted Draft",
                    },
                },
            ),
            (
                3,
                {
                    "files": {
                        DRAFT_FILE_KEY: _epphapp_file(checksum="draft-v3", version=3),
                        PUBLIC_FILE_KEY: _public_file(checksum="main-v1"),
                    },
                    "publication_date": "2020-01-14",
                    "access": {
                        "access_obj": {"record": None, "files": "restricted"},
                        "meta": "EP Restricted Draft",
                    },
                },
            ),
            (
                4,
                {
                    "files": {
                        DRAFT_FILE_KEY: _epphapp_file(checksum="draft-v4", version=4),
                        PUBLIC_FILE_KEY: _public_file(checksum="main-v1"),
                    },
                    "publication_date": "2020-01-15",
                    "access": {
                        "access_obj": {"record": None, "files": "restricted"},
                        "meta": "EP Restricted Draft",
                    },
                },
            ),
        ]
    )


def _versions_public_only():
    """Single version with only public files."""
    return OrderedDict(
        [
            (
                1,
                {
                    "files": {
                        "document.pdf": _public_file(
                            key="document.pdf", checksum="doc-v1", id_bibdoc=300
                        ),
                    },
                    "publication_date": "2020-02-01",
                    "access": {"access_obj": {"record": None, "files": None}},
                },
            ),
        ]
    )


class TestPublicEntryVersions:
    """Test that PublicEntry filters out EPPHAPP files and deduplicates versions."""

    def test_public_excludes_epphapp_files(self):
        entry = _make_entry(_versions_with_epphapp())
        result = PublicEntry(
            entry, _make_approval_request(), _make_migration_logger()
        ).build()

        for _, vdata in result["versions"].items():
            for key, fdata in vdata["files"].items():
                assert (
                    fdata["type"] != EPPHAPP_FILE_TYPE
                ), f"EPPHAPP file {key} should not appear in public split"

    def test_public_deduplicates_identical_versions(self):
        entry = _make_entry(_versions_with_epphapp())
        result = PublicEntry(
            entry, _make_approval_request(), _make_migration_logger()
        ).build()

        assert len(result["versions"]) == 1

    def test_public_access_is_public(self):
        entry = _make_entry(_versions_with_epphapp())
        result = PublicEntry(
            entry, _make_approval_request(), _make_migration_logger()
        ).build()

        for _, vdata in result["versions"].items():
            assert vdata["access"]["access_obj"]["record"] == "public"
            assert vdata["access"]["access_obj"]["files"] == "public"

    def test_public_raises_when_no_public_files(self):
        versions = OrderedDict(
            [
                (
                    1,
                    {
                        "files": {
                            DRAFT_FILE_KEY: _epphapp_file(checksum="draft-only"),
                        },
                        "publication_date": "2020-01-10",
                        "access": {
                            "access_obj": {"record": None, "files": "restricted"},
                        },
                    },
                ),
            ]
        )
        entry = _make_entry(versions)
        with pytest.raises(UnexpectedValue, match="No public files found"):
            PublicEntry(
                entry, _make_approval_request(), _make_migration_logger()
            ).build()

    def test_public_raises_on_restricted_files(self):
        versions = OrderedDict(
            [
                (
                    1,
                    {
                        "files": {
                            "restricted.pdf": {
                                "key": "restricted.pdf",
                                "checksum": "restricted-v1",
                                "version": 1,
                                "id_bibdoc": 999,
                                "access": "restricted",
                                "type": "Main",
                                "creation_date": "2020-01-01",
                            },
                        },
                        "publication_date": "2020-01-01",
                        "access": {"access_obj": {"record": None, "files": None}},
                    },
                ),
            ]
        )
        entry = _make_entry(versions)
        with pytest.raises(UnexpectedValue, match="restricted files"):
            PublicEntry(
                entry, _make_approval_request(), _make_migration_logger()
            ).build()

    def test_public_multiple_distinct_versions(self):
        versions = OrderedDict(
            [
                (
                    1,
                    {
                        "files": {
                            "paper.pdf": _public_file(
                                key="paper.pdf", checksum="paper-v1", id_bibdoc=300
                            ),
                        },
                        "publication_date": "2020-01-01",
                        "access": {"access_obj": {"record": None, "files": None}},
                    },
                ),
                (
                    2,
                    {
                        "files": {
                            "paper.pdf": _public_file(
                                key="paper.pdf",
                                checksum="paper-v2",
                                version=2,
                                id_bibdoc=300,
                            ),
                        },
                        "publication_date": "2020-02-01",
                        "access": {"access_obj": {"record": None, "files": None}},
                    },
                ),
            ]
        )
        entry = _make_entry(versions)
        result = PublicEntry(
            entry, _make_approval_request(), _make_migration_logger()
        ).build()

        assert len(result["versions"]) == 2


class TestRestrictedEntryVersions:
    """Test that RestrictedEntry keeps EPPHAPP files when present."""

    def test_restricted_keeps_only_epphapp_when_present(self):
        entry = _make_entry(_versions_with_epphapp())
        result = RestrictedEntry(
            entry, _make_approval_request(), _make_migration_logger()
        ).build()

        for _, vdata in result["versions"].items():
            for key, fdata in vdata["files"].items():
                assert (
                    fdata["type"] == EPPHAPP_FILE_TYPE
                ), f"Non-EPPHAPP file {key} should not appear in restricted split"

    def test_restricted_keeps_all_changing_epphapp_versions(self):
        entry = _make_entry(_versions_with_epphapp())
        result = RestrictedEntry(
            entry, _make_approval_request(), _make_migration_logger()
        ).build()

        assert len(result["versions"]) == 4

    def test_restricted_access_is_restricted(self):
        entry = _make_entry(_versions_with_epphapp())
        result = RestrictedEntry(
            entry, _make_approval_request(), _make_migration_logger()
        ).build()

        for _, vdata in result["versions"].items():
            assert vdata["access"]["access_obj"]["record"] == "restricted"
            assert vdata["access"]["access_obj"]["files"] == "restricted"

    def test_restricted_uses_public_files_when_no_epphapp(self):
        entry = _make_entry(_versions_public_only())
        logger = _make_migration_logger()
        result = RestrictedEntry(entry, _make_approval_request(), logger).build()

        assert len(result["versions"]) == 1
        assert "document.pdf" in result["versions"][1]["files"]
        logger.add_information.assert_called()

    def test_restricted_raises_when_no_files_at_all(self):
        versions = OrderedDict(
            [
                (
                    1,
                    {
                        "files": {},
                        "publication_date": "2020-01-01",
                        "access": {"access_obj": {"record": None, "files": None}},
                    },
                ),
            ]
        )
        entry = _make_entry(versions)
        with pytest.raises(UnexpectedValue, match="No files found"):
            RestrictedEntry(
                entry, _make_approval_request(), _make_migration_logger()
            ).build()


class TestPublicEntryIdentifiers:
    """Test identifier handling in the public split."""

    def test_public_removes_cern_ep_report_numbers(self):
        entry = _make_entry(_versions_with_epphapp())
        result = PublicEntry(
            entry, _make_approval_request(), _make_migration_logger()
        ).build()

        identifiers = result["record"]["json"]["metadata"]["identifiers"]
        cdsrn_values = {i["identifier"] for i in identifiers if i["scheme"] == "cdsrn"}

        assert APPROVED_REPORT_NUMBER not in cdsrn_values
        assert any(
            i["scheme"] == "apprn" and i["identifier"] == APPROVED_REPORT_NUMBER
            for i in identifiers
        )

    def test_public_keeps_non_ep_cdsrn(self):
        identifiers = [
            {"identifier": RECID, "scheme": "cds"},
            {"scheme": "cdsrn", "identifier": APPROVED_REPORT_NUMBER},
            {"scheme": "cdsrn", "identifier": "OTHER-RN-001"},
        ]
        entry = _make_entry(_versions_with_epphapp(), identifiers=identifiers)
        result = PublicEntry(
            entry, _make_approval_request(), _make_migration_logger()
        ).build()

        cdsrn_ids = [
            i
            for i in result["record"]["json"]["metadata"]["identifiers"]
            if i["scheme"] == "cdsrn"
        ]
        assert len(cdsrn_ids) == 1
        assert cdsrn_ids[0]["identifier"] == "OTHER-RN-001"


class TestRestrictedEntryIdentifiers:
    """Test identifier handling in the restricted split."""

    def test_restricted_removes_matching_cern_ep_rn(self):
        entry = _make_entry(_versions_with_epphapp())
        result = RestrictedEntry(
            entry, _make_approval_request(), _make_migration_logger()
        ).build()

        cdsrn_values = {
            i["identifier"]
            for i in result["record"]["json"]["metadata"]["identifiers"]
            if i["scheme"] == "cdsrn"
        }

        assert APPROVED_REPORT_NUMBER not in cdsrn_values

    def test_restricted_keeps_draft_report_number(self):
        entry = _make_entry(_versions_with_epphapp())
        result = RestrictedEntry(
            entry, _make_approval_request(), _make_migration_logger()
        ).build()

        cdsrn_values = {
            i["identifier"]
            for i in result["record"]["json"]["metadata"]["identifiers"]
            if i["scheme"] == "cdsrn"
        }

        assert DRAFT_REPORT_NUMBER in cdsrn_values

    def test_restricted_raises_on_mismatched_report_number(self):
        identifiers = [
            {"identifier": RECID, "scheme": "cds"},
            {"scheme": "cdsrn", "identifier": "CERN-EP-2020-999"},
        ]
        entry = _make_entry(_versions_with_epphapp(), identifiers=identifiers)
        with pytest.raises(UnexpectedValue, match="not the same"):
            RestrictedEntry(
                entry, _make_approval_request(), _make_migration_logger()
            ).build()

    def test_restricted_removes_doi_pid(self):
        entry = _make_entry(_versions_with_epphapp(), has_doi=True)
        result = RestrictedEntry(
            entry, _make_approval_request(), _make_migration_logger()
        ).build()

        assert "doi" not in result["record"]["json"].get("pids", {})


class TestPublicEntryModifications:
    """Test record/parent level modifications on the public split."""

    def test_public_removes_request_data(self):
        entry = _make_entry(_versions_with_epphapp())
        result = PublicEntry(
            entry, _make_approval_request(), _make_migration_logger()
        ).build()

        assert "_request_data" not in result["record"]

    def test_public_sets_owned_by_system(self):
        entry = _make_entry(_versions_with_epphapp())
        result = PublicEntry(
            entry, _make_approval_request(), _make_migration_logger()
        ).build()

        assert result["record"]["owned_by"] == "system"
        assert result["parent"]["json"]["access"]["owned_by"] == {"user": "system"}

    def test_public_adds_cern_scientific_community(self):
        entry = _make_entry(_versions_with_epphapp())
        result = PublicEntry(
            entry, _make_approval_request(), _make_migration_logger()
        ).build()

        assert CDS_CERN_SCIENTIFIC_COMMUNITY_ID in (
            result["parent"]["json"]["communities"]["ids"]
        )

    def test_public_does_not_duplicate_community(self):
        entry = _make_entry(_versions_with_epphapp())
        entry["parent"]["json"]["communities"]["ids"] = [
            "example-community",
            CDS_CERN_SCIENTIFIC_COMMUNITY_ID,
        ]
        result = PublicEntry(
            entry, _make_approval_request(), _make_migration_logger()
        ).build()

        community_ids = result["parent"]["json"]["communities"]["ids"]
        assert community_ids.count(CDS_CERN_SCIENTIFIC_COMMUNITY_ID) == 1


class TestEntryImmutability:
    """Ensure build() deep-copies and does not mutate the original entry."""

    def test_public_build_does_not_mutate_original(self):
        entry = _make_entry(_versions_with_epphapp())
        original = deepcopy(entry)
        PublicEntry(entry, _make_approval_request(), _make_migration_logger()).build()

        assert (
            entry["record"]["json"]["metadata"]["identifiers"]
            == original["record"]["json"]["metadata"]["identifiers"]
        )

    def test_restricted_build_does_not_mutate_original(self):
        entry = _make_entry(_versions_with_epphapp())
        original = deepcopy(entry)
        RestrictedEntry(
            entry, _make_approval_request(), _make_migration_logger()
        ).build()

        assert (
            entry["record"]["json"]["metadata"]["identifiers"]
            == original["record"]["json"]["metadata"]["identifiers"]
        )
