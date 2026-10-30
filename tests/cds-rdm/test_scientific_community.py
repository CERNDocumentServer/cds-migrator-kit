# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Tests for auto-inclusion in the CERN Research community."""

from types import SimpleNamespace

import pytest

from cds_migrator_kit.errors import MissingConfiguration
from cds_migrator_kit.rdm.records.transform.config import (
    CERN_SCIENTIFIC_RESOURCE_TYPES,
)
from cds_migrator_kit.rdm.records.transform.entities.parent import RecordParent
from cds_migrator_kit.rdm.records.transform.entities.record import RecordEntry

RECID = "123456"


def _file_dump(version=1, status="", full_name="draft.pdf"):
    """Build a minimal legacy file dump, as found in ``raw_dump_entry["files"]``."""
    return {
        "recid": int(RECID),
        "full_name": full_name,
        "version": version,
        "status": status,
        "hidden": False,
        "subformat": "",
        "type": "Main",
    }


def _restrictions(files):
    """Build the dump's ``restrictions``, as the legacy dumper would.

    Like a legacy BibDoc status, a file's restriction is the status of its
    latest revision; only restricted files are listed.
    """
    statuses = {}
    for file_dump in sorted(files, key=lambda f: f["version"]):
        statuses[file_dump["full_name"]] = file_dump["status"]
    return {
        "collections": {},
        "files": [
            {"bibdocid": i, "status": status, "roles": []}
            for i, status in enumerate(statuses.values())
            if status
        ],
        "record": {"owners": {}, "viewers": {}},
        "edit": {"collections": {}, "any_collection": []},
    }


def _dojson_entry(communities=None):
    """Build a minimal DOJSON-processed entry (it carries no ``files`` key)."""
    return {
        "communities": communities or [],
    }


def _record(
    dojson_entry,
    files=None,
    resource_type="publication-preprint",
    restricted=False,
):
    """Build a real RecordEntry, skipping only the heavy ``build()`` step.

    ``access_status`` and the restriction helpers come from the actual class;
    ``body`` (normally produced by ``build()``) is set to the one field
    RecordParent reads from it.
    """
    record = RecordEntry(
        dump=SimpleNamespace(
            data={
                "recid": RECID,
                "files": files or [],
                "restrictions": _restrictions(files or []),
            },
            first_created=None,
        ),
        dojson_entry=dojson_entry,
        restricted=restricted,
    )
    metadata = {}
    if resource_type is not None:
        metadata["resource_type"] = {"id": resource_type}
    record.body = {"metadata": metadata}
    return record


def _build_communities(
    communities_ids,
    dojson_entry,
    files=None,
    resource_type="publication-preprint",
    restricted=False,
):
    """Run RecordParent's community resolution."""
    record = _record(dojson_entry, files, resource_type, restricted)
    parent = RecordParent(
        record=record,
        raw_dump_entry=record.raw_dump_entry,
        dojson_entry=dojson_entry,
        communities_ids=communities_ids,
        access_grants_view=None,
    )
    return parent._build_communities()


class TestRecordEntryRestrictions:
    """Test RecordEntry.has_any_restricted_file()/has_any_restrictions()."""

    def test_public_files_in_all_versions(self):
        """Public files in every version: no restrictions."""
        files = [_file_dump(version=1), _file_dump(version=2)]
        record = _record(_dojson_entry(), files=files)

        assert not record.has_any_restricted_file()
        assert not record.has_any_restrictions()

    def test_restricted_file_in_latest_version(self):
        """A restricted file in the latest version is detected."""
        files = [_file_dump(version=1), _file_dump(version=2, status="restricted")]
        record = _record(_dojson_entry(), files=files)

        assert record.has_any_restricted_file()
        assert record.has_any_restrictions()

    def test_restriction_lifted_in_latest_version(self):
        """A file whose latest revision is public is not restricted.

        A legacy file's status is set per BibDoc: the dump only has the
        current one.
        """
        files = [_file_dump(version=1, status="restricted"), _file_dump(version=2)]
        record = _record(_dojson_entry(), files=files)

        assert not record.has_any_restricted_file()
        assert not record.has_any_restrictions()


@pytest.fixture
def communities_ids(community):
    """Collection community configured for the migration run."""
    return [str(community.id)]


class TestBuildCommunities:
    """Test RecordParent._build_communities()."""

    def test_adds_scientific_community_for_public_research_record(
        self, communities_ids, community, cern_scientific_community
    ):
        """Public research records are included in the CERN Scientific community."""
        result = _build_communities(communities_ids, _dojson_entry())

        assert result == {
            "ids": [str(community.id), str(cern_scientific_community.id)],
            "default": str(community.id),
        }

    def test_keep_collection_community_as_default(
        self, communities_ids, community, cern_scientific_community
    ):
        """Collection community remains the default when CERN Scientific community is added."""
        result = _build_communities(
            communities_ids,
            _dojson_entry(communities=["test-community"]),
        )

        assert result["default"] == str(community.id)
        assert result["ids"] == [
            str(community.id),
            "test-community",
            str(cern_scientific_community.id),
        ]

    @pytest.mark.parametrize("resource_type", CERN_SCIENTIFIC_RESOURCE_TYPES)
    def test_research_resource_types(
        self, communities_ids, cern_scientific_community, resource_type
    ):
        """All configured public research resource types trigger inclusion."""
        result = _build_communities(
            communities_ids, _dojson_entry(), resource_type=resource_type
        )

        assert str(cern_scientific_community.id) in result["ids"]
        assert str(cern_scientific_community.id) != result["default"]

    def test_skip_restricted_files(
        self, communities_ids, community, cern_scientific_community
    ):
        """Records with restricted files are not included in the CERN Scientific community."""
        result = _build_communities(
            communities_ids,
            _dojson_entry(),
            files=[_file_dump(status="restricted")],
        )

        assert result == {
            "ids": [str(community.id)],
            "default": str(community.id),
        }

    def test_skip_restricted_file_with_several_versions(
        self, communities_ids, community, cern_scientific_community
    ):
        """A restricted file with several revisions excludes the whole parent."""
        result = _build_communities(
            communities_ids,
            _dojson_entry(),
            files=[
                _file_dump(version=1, status="restricted"),
                _file_dump(version=2, status="restricted"),
            ],
        )

        assert result == {
            "ids": [str(community.id)],
            "default": str(community.id),
        }

    def test_skip_non_research_resource_type(
        self, communities_ids, community, cern_scientific_community
    ):
        """Non-research resource types are not included in the CERN Scientific community."""
        result = _build_communities(
            communities_ids, _dojson_entry(), resource_type="other"
        )

        assert result == {
            "ids": [str(community.id)],
            "default": str(community.id),
        }

    def test_skip_when_stream_is_restricted(
        self, communities_ids, community, cern_scientific_community
    ):
        """Records on restricted migration streams are not included in the CERN Scientific community."""
        result = _build_communities(communities_ids, _dojson_entry(), restricted=True)

        assert result == {
            "ids": [str(community.id)],
            "default": str(community.id),
        }

    def test_raise_when_cern_scientific_community_not_configured(
        self, test_app, communities_ids, monkeypatch
    ):
        """Missing CERN Scientific community config raises."""
        monkeypatch.setitem(test_app.config, "CDS_CERN_SCIENTIFIC_COMMUNITY_ID", None)

        with pytest.raises(MissingConfiguration):
            _build_communities(communities_ids, _dojson_entry())
