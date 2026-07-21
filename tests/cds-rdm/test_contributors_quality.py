# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Tests for contributors.py quality module."""

import pytest

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.contributors import (
    extract_json_contributor_ids,
)


class TestExtractJsonContributorIdsOrcid:
    """Test the ORCID handling of extract_json_contributor_ids."""

    def test_valid_orcid_added(self):
        """Test that a valid ORCID is added as an identifier."""
        ids = extract_json_contributor_ids({"k": "0000-0002-1825-0097"})
        assert ids == [{"identifier": "0000-0002-1825-0097", "scheme": "orcid"}]

    def test_orcid_prefix_stripped(self):
        """Test that an 'ORCID:' prefix is stripped before validation."""
        ids = extract_json_contributor_ids({"k": "ORCID:0000-0002-1825-0097"})
        assert ids == [{"identifier": "0000-0002-1825-0097", "scheme": "orcid"}]

    def test_jacow_prefixed_value_ignored(self):
        """Test that a 'jacow-' prefixed value is ignored, not treated as an ORCID."""
        ids = extract_json_contributor_ids({"k": "JACOW-1234567"})
        assert ids == []

    def test_jacow_prefixed_value_ignored_lowercase(self):
        """Test that the jacow check is case-insensitive."""
        ids = extract_json_contributor_ids({"k": "jacow-1234567"})
        assert ids == []

    def test_jacow_prefixed_value_ignored_mixed_case(self):
        """Test that the jacow check is case-insensitive for mixed case."""
        ids = extract_json_contributor_ids({"k": "Jacow-AbC123"})
        assert ids == []

    def test_invalid_orcid_raises_error(self):
        """Test that an invalid, non-jacow ORCID raises UnexpectedValue."""
        with pytest.raises(UnexpectedValue):
            extract_json_contributor_ids({"k": "not-an-orcid"})

    def test_no_orcid_subfield_no_identifier_added(self):
        """Test that a missing ORCID subfield does not add an identifier or raise."""
        ids = extract_json_contributor_ids({})
        assert ids == []

    def test_empty_orcid_subfield_no_identifier_added(self):
        """Test that an empty ORCID subfield does not add an identifier or raise."""
        ids = extract_json_contributor_ids({"k": ""})
        assert ids == []

    def test_custom_orcid_subfield(self):
        """Test that a custom orcid_subfield is used instead of the default 'k'."""
        ids = extract_json_contributor_ids(
            {"j": "0000-0002-1825-0097"}, orcid_subfield="j"
        )
        assert ids == [{"identifier": "0000-0002-1825-0097", "scheme": "orcid"}]

    def test_custom_orcid_subfield_jacow_ignored(self):
        """Test that the jacow check also applies with a custom orcid_subfield."""
        ids = extract_json_contributor_ids({"j": "JACOW-1234567"}, orcid_subfield="j")
        assert ids == []


class TestExtractJsonContributorIdsOtherSources:
    """Test the non-ORCID identifier sources of extract_json_contributor_ids."""

    def test_inspire_author_id_from_0_subfield(self):
        """Test extraction of an INSPIRE author id from subfield '0'."""
        ids = extract_json_contributor_ids({"0": "AUTHOR|(INSPIRE)12345"})
        assert ids == [{"identifier": "12345", "scheme": "inspire_author"}]

    def test_cds_author_id_from_0_subfield(self):
        """Test extraction of a CDS author id from subfield '0'."""
        ids = extract_json_contributor_ids({"0": "AUTHOR|(CDS)98765"})
        assert ids == [{"identifier": "98765", "scheme": "cds"}]

    def test_cern_author_id_from_0_subfield(self):
        """Test extraction of a CERN (SzGeCERN) author id from subfield '0'."""
        ids = extract_json_contributor_ids({"0": "AUTHOR|(SzGeCERN)11111"})
        assert ids == [{"identifier": "11111", "scheme": "cern"}]

    def test_unmatched_0_subfield_ignored(self):
        """Test that an unrecognized subfield '0' value is silently ignored."""
        ids = extract_json_contributor_ids({"0": "SOMETHING-ELSE"})
        assert ids == []

    def test_inspire_id_from_i_subfield(self):
        """Test extraction of an INSPIRE id from subfield 'i'."""
        ids = extract_json_contributor_ids({"i": "INSPIRE-98765"})
        assert ids == [{"identifier": "INSPIRE-98765", "scheme": "inspire_author"}]

    def test_i_subfield_without_inspire_prefix_ignored(self):
        """Test that subfield 'i' without the INSPIRE- prefix is ignored."""
        ids = extract_json_contributor_ids({"i": "98765"})
        assert ids == []

    def test_all_sources_combined(self):
        """Test that identifiers from all sources are combined without duplicates."""
        ids = extract_json_contributor_ids(
            {
                "0": "AUTHOR|(INSPIRE)12345",
                "k": "0000-0002-1825-0097",
                "i": "INSPIRE-98765",
            }
        )
        assert {"identifier": "12345", "scheme": "inspire_author"} in ids
        assert {"identifier": "0000-0002-1825-0097", "scheme": "orcid"} in ids
        assert {"identifier": "INSPIRE-98765", "scheme": "inspire_author"} in ids
        assert len(ids) == 3

    def test_duplicate_0_subfield_ids_not_repeated(self):
        """Test that repeated identical ids in subfield '0' are not duplicated."""
        ids = extract_json_contributor_ids(
            {"0": ["AUTHOR|(CDS)98765", "AUTHOR|(CDS)98765"]}
        )
        assert ids == [{"identifier": "98765", "scheme": "cds"}]
