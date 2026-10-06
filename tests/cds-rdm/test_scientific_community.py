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


def _record(
    access="public",
    resource_type="publication-preprint",
    restricted=False,
    recid="123456",
):
    """Build a minimal RecordEntry stand-in (only what RecordParent reads)."""
    metadata = {
        "title": "Test record",
        "publication_date": "2020-01-01",
    }
    if resource_type is not None:
        metadata["resource_type"] = {"id": resource_type}

    return SimpleNamespace(
        recid=recid,
        access_status=access,
        restricted=restricted,
        body={"metadata": metadata},
    )


def _dojson_entry(files_restricted=False, communities=None):
    """Build a minimal DOJSON-processed entry for community tests."""
    status = "restricted" if files_restricted else ""
    return {"files": [{"status": status}], "communities": communities or []}


def _build_communities(communities_ids, record, dojson_entry):
    """Run RecordParent's community resolution."""
    parent = RecordParent(
        record=record,
        raw_dump_entry={"recid": record.recid},
        dojson_entry=dojson_entry,
        communities_ids=communities_ids,
        access_grants_view=None,
    )
    return parent._build_communities()


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
        result = _build_communities(communities_ids, _record(), _dojson_entry())

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
            _record(),
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
            communities_ids,
            _record(resource_type=resource_type),
            _dojson_entry(),
        )

        assert str(cern_scientific_community.id) in result["ids"]
        assert str(cern_scientific_community.id) != result["default"]

    def test_skip_restricted_record(
        self, communities_ids, community, cern_scientific_community
    ):
        """Restricted records are not included in the CERN Research community."""
        result = _build_communities(
            communities_ids, _record(access="restricted"), _dojson_entry()
        )

        assert result == {
            "ids": [str(community.id)],
            "default": str(community.id),
        }

    def test_skip_restricted_files(
        self, communities_ids, community, cern_scientific_community
    ):
        """Records with restricted files are not included in the CERN Scientific community."""
        result = _build_communities(
            communities_ids, _record(), _dojson_entry(files_restricted=True)
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
            communities_ids, _record(resource_type="other"), _dojson_entry()
        )

        assert result == {
            "ids": [str(community.id)],
            "default": str(community.id),
        }

    def test_skip_when_stream_is_restricted(
        self, communities_ids, community, cern_scientific_community
    ):
        """Records on restricted migration streams are not included in the CERN Scientific community."""
        result = _build_communities(
            communities_ids, _record(restricted=True), _dojson_entry()
        )

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
            _build_communities(communities_ids, _record(), _dojson_entry())
