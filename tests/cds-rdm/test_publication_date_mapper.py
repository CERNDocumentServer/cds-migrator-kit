# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Tests for PublicationDateMapper 260/269 date choice."""

import pytest

from cds_migrator_kit.rdm.records.transform.mappers.base import RecordTransformContext
from cds_migrator_kit.rdm.records.transform.mappers.metadata import (
    PublicationDateMapper,
)


def _choose(pub_260, preprint_269, resource_type="publication-technicalnote"):
    """Run PublicationDateMapper for the given 260/269 dates."""
    dojson = {
        "resource_type": {"id": resource_type},
        "status_week_date": "2020-01-01",
    }
    if pub_260 is not None:
        dojson["publication_date"] = pub_260
    if preprint_269 is not None:
        dojson["preprint_date"] = preprint_269
    return PublicationDateMapper().map_value(
        RecordTransformContext(dojson_entry=dojson, raw_dump_entry={"files": []})
    )


class TestPublicationDateMapper:
    """Test 260 vs 269 publication_date choice in PublicationDateMapper."""

    @pytest.mark.parametrize(
        "pub_260, preprint_269, expected",
        [
            # more data wins (day > month > year)
            ("2016", "2016-01", "2016-01"),
            ("2016", "2016-01-19", "2016-01-19"),
            ("2016-01", "2016-01-19", "2016-01-19"),
            ("2016-01", "2016", "2016-01"),
            ("2016-01-19", "2016", "2016-01-19"),
            ("2016-01-19", "2016-01", "2016-01-19"),
            # equal precision → keep 260
            ("2016", "2017", "2016"),
            ("2016-01", "2016-06", "2016-01"),
            ("2016-01-19", "2016-01-20", "2016-01-19"),
            # year-1 placeholder 260 loses to real 269
            ("0001-01-19", "2016-01-19", "2016-01-19"),
            ("1-01-19", "2016-01-19", "2016-01-19"),
            ("1-01-09", "2016-01-19", "2016-01-19"),
            ("0001-01-19", "2016-01", "2016-01"),
            ("0001-01-19", "2016", "2016"),
            # only one source
            ("2016-01", None, "2016-01"),
            (None, "2016-01-19", "2016-01-19"),
            ("2016/2017", None, "2016/2017"),
        ],
    )
    def test_non_article_chooses_date_with_more_data(
        self, pub_260, preprint_269, expected
    ):
        """Test that non-articles pick the 260/269 date with more data."""
        assert _choose(pub_260, preprint_269) == expected

    def test_article_always_keeps_260(self):
        """Test that articles keep 260 even when 269 has more data."""
        assert (
            _choose("2016", "2016-01-19", resource_type="publication-article") == "2016"
        )

    def test_year_one_placeholder_fails_edtf(self):
        """Unpadded year-1 dates are invalid EDTF and fail at load."""
        from babel_edtf import parse_edtf
        from edtf.parser.edtf_exceptions import EDTFParseException

        with pytest.raises(EDTFParseException):
            parse_edtf("1-01-09")
