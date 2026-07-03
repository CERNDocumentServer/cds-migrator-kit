# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Tests for auto-inclusion in the CERN Research community."""

from unittest.mock import MagicMock

import pytest

from cds_migrator_kit.errors import MissingConfiguration
from cds_migrator_kit.rdm.records.transform.config import CDS_CERN_SCIENTIFIC_RESOURCE_TYPES
from cds_migrator_kit.rdm.records.transform.transform import CDSToRDMRecordTransform


def _test_record(
    access="public",
    resource_type="publication-preprint",
    communities=[],
    recid="123456",
):
    """Build a minimal CDSToRDMRecordEntry.transform() output for community tests."""
    metadata = {
        "title": "Test record",
        "publication_date": "2020-01-01",
    }
    if resource_type is not None:
        metadata["resource_type"] = {"id": resource_type}

    return {
        "recid": recid,
        "access": access,
        "communities": communities,
        "json": {
            "files": {"enabled": False},
            "metadata": metadata,
        },
    }


@pytest.fixture
def transform(tmp_path, community):
    """Transform instance with a collection community configured."""
    return CDSToRDMRecordTransform(
        files_dump_dir=tmp_path,
        missing_users=tmp_path,
        communities_ids=[str(community.id)],
        migration_logger=MagicMock(),
    )


class TestCommunitiesIds:
    """Test CDSToRDMRecordTransform._communities_ids()."""

    def test_adds_scientific_community_for_public_research_test_record(
        self, transform, community, cern_scientific_community
    ):
        """Public research records are included in the CERN Scientific community."""
        result = transform._communities_ids({}, _test_record())

        assert result == {
            "ids": [str(community.id), str(cern_scientific_community.id)],
            "default": str(community.id),
        }

    def test_keep_collection_community_as_default(
        self, transform, community, cern_scientific_community
    ):
        """Collection community remains the default when CERN Scientific community is added."""
        result = transform._communities_ids(
            {},
            _test_record(communities=["test-community"]),
        )

        assert result["default"] == str(community.id)
        assert result["ids"] == [
            str(community.id),
            "test-community",
            str(cern_scientific_community.id),
        ]

    @pytest.mark.parametrize("resource_type", CDS_CERN_SCIENTIFIC_RESOURCE_TYPES)
    def test_research_resource_types(
        self, transform, cern_scientific_community, resource_type
    ):
        """All configured public research resource types trigger inclusion."""
        result = transform._communities_ids(
            {}, _test_record(resource_type=resource_type)
        )

        assert str(cern_scientific_community.id) in result["ids"]
        assert cern_scientific_community.id != result["default"]

    def test_skip_restricted_test_record(
        self, transform, community, cern_scientific_community
    ):
        """Restricted records are not included in the CERN Research community."""
        result = transform._communities_ids({}, _test_record(access="restricted"))

        assert result == {
            "ids": [str(community.id)],
            "default": str(community.id),
        }

    def test_skip_non_research_resource_type(
        self, transform, community, cern_scientific_community
    ):
        """Non-research resource types are not included in the CERN Scientific community."""
        result = transform._communities_ids({}, _test_record(resource_type="other"))

        assert result == {
            "ids": [str(community.id)],
            "default": str(community.id),
        }

    def test_skip_when_stream_is_restricted(
        self, tmp_path, community, cern_scientific_community
    ):
        """Records on restricted migration streams are not included in the CERN Scientific community."""
        transform = CDSToRDMRecordTransform(
            files_dump_dir=tmp_path,
            missing_users=tmp_path,
            communities_ids=[str(community.id)],
            restricted=True,
            migration_logger=MagicMock(),
        )

        result = transform._communities_ids({}, _test_record())

        assert result == {
            "ids": [str(community.id)],
            "default": str(community.id),
        }

    def test_raise_when_cern_scientific_community_not_configured(
        self, test_app, transform, community
    ):
        """No CERN Scientific community is added when config is unset."""
        test_app.config["CDS_CERN_SCIENTIFIC_COMMUNITY_ID"] = None

        with pytest.raises(MissingConfiguration):
            transform._communities_ids({}, _test_record())
