# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Tests for cds_migrator_kit/rdm/records/transform/mappers/metadata.py."""

from cds_migrator_kit.rdm.records.transform.mappers.base import (
    RecordTransformContext,
)
from cds_migrator_kit.rdm.records.transform.mappers.metadata import (
    TableOfContentsMapper,
)


def _ctx(dojson_entry):
    return RecordTransformContext(dojson_entry=dojson_entry, raw_dump_entry={})


class TestTableOfContentsMapper:
    """Test TableOfContentsMapper.map_value()."""

    def test_deduplicates_identical_entries(self):
        """Exact-duplicate descriptions (e.g. a MARC field repeated in the
        legacy record) collapse to a single entry."""
        desc = {"description": "Same abstract text.", "type": {"id": "other"}}
        dojson_entry = {"additional_descriptions": [desc, dict(desc), dict(desc)]}

        result = TableOfContentsMapper().map_value(_ctx(dojson_entry))

        assert result == [desc]

    def test_keeps_distinct_entries(self):
        """Descriptions that actually differ are all kept, in order."""
        desc_a = {"description": "Series info", "type": {"id": "series-information"}}
        desc_b = {"description": "Other note", "type": {"id": "other"}}
        dojson_entry = {"additional_descriptions": [desc_a, desc_b]}

        result = TableOfContentsMapper().map_value(_ctx(dojson_entry))

        assert result == [desc_a, desc_b]

    def test_same_text_different_type_is_not_deduplicated(self):
        """Same text under a different type is a distinct entry."""
        desc_a = {"description": "Same text", "type": {"id": "other"}}
        desc_b = {"description": "Same text", "type": {"id": "series-information"}}
        dojson_entry = {"additional_descriptions": [desc_a, desc_b]}

        result = TableOfContentsMapper().map_value(_ctx(dojson_entry))

        assert result == [desc_a, desc_b]

    def test_folds_table_of_content_in_before_deduplicating(self):
        """table_of_content is folded in, and still deduped against."""
        toc_entry = {
            "description": "1. Intro\n2. Results",
            "type": {"id": "table-of-contents"},
        }
        dojson_entry = {
            "table_of_content": "1. Intro\n2. Results",
            "additional_descriptions": [dict(toc_entry)],
        }

        result = TableOfContentsMapper().map_value(_ctx(dojson_entry))

        assert result == [toc_entry]
        assert "table_of_content" not in dojson_entry

    def test_no_descriptions_returns_falsy(self):
        """No additional_descriptions/table_of_content at all is a no-op."""
        result = TableOfContentsMapper().map_value(_ctx({}))

        assert not result
