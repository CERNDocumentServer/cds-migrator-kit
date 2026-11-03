# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Tests for the legacy-to-model query translation in ``collection_conf.py``.

These tests document how a legacy search query (the notation of the dump, used
by the ``queries`` of a collection configuration) is translated to the notation
a model ``__query__`` needs, and how the matcher then behaves on the result.
"""

import os
import textwrap

import pytest
from cds_dojson.matcher import Query

from cds_migrator_kit.rdm.collection_conf import conf_query, legacy_to_model_query


class TestLegacyToModelQuery:
    """How a legacy search query is translated to a model ``__query__``."""

    @pytest.mark.parametrize(
        "legacy,expected",
        [
            # --- field notation: the matcher needs the indicators and a dot ---
            # a bare 3-character tag is padded with two blank indicators
            ("980:ARTICLE", "980__:/(?i)ARTICLE/"),
            # a 4-character tag (tag + one indicator) gets one blank indicator
            ("690C:SCICOM", "690C_:/(?i)SCICOM/"),
            # a 6-character tag + subfield gets a dot before the subfield
            ("980__a:DUMMY", "980__.a:/(?i)DUMMY/"),
            ("710__5:EST", "710__.5:/(?i)EST/"),
            # --- values become case-insensitive regexes (the search ignores
            #     the case, the matcher does not) ---
            # an unquoted value: a case-insensitive substring search
            ("088:CERN-ALEPH", "088__:/(?i)CERN\\-ALEPH/"),
            # a quoted value: a case-insensitive but exact match (anchored)
            ('693__a:"CERN ISOLDE"', "693__.a:/(?i)^CERN\\ ISOLDE$/"),
            # a trailing wildcard: a case-insensitive prefix match
            ('037:"CLIC-Note*"', "037__:/(?i)^CLIC\\-Note.*/"),
            # wildcards on both sides: a case-insensitive substring match
            ("595:*ARCHIVE*", "595__:/(?i).*ARCHIVE.*/"),
            # --- operators, negation and parentheses are left as they are ---
            ("-980:DELETED", "-980__:/(?i)DELETED/"),
            (
                "980:ARTICLE OR 980:PREPRINT",
                "980__:/(?i)ARTICLE/ OR 980__:/(?i)PREPRINT/",
            ),
            (
                "710__5:EST 980:ARTICLE -980:CLINOT",
                "710__.5:/(?i)EST/ 980__:/(?i)ARTICLE/ -980__:/(?i)CLINOT/",
            ),
        ],
    )
    def test_translation(self, legacy, expected):
        """Each legacy query is translated to the expected model notation."""
        assert legacy_to_model_query(legacy) == expected

    def test_search_keyword_is_translated_but_warns(self):
        """A search keyword is not a MARC field: it is translated but warns."""
        with pytest.warns(UserWarning, match="search keyword"):
            result = legacy_to_model_query('collection:"PERI" OR 980:X')
        assert result == "collection:/(?i)^PERI$/ OR 980__:/(?i)X/"


class TestTranslatedQueryMatching:
    """The matcher accepts the records the legacy search would, case aside."""

    @staticmethod
    def _matches(legacy, record):
        """Translate ``legacy`` and tell whether the matcher accepts ``record``."""
        return Query(legacy_to_model_query(legacy)).match(record)

    @pytest.mark.parametrize("value", ["LCD-Notes", "LCD-NOTES", "lcd-notes"])
    def test_unquoted_value_is_case_insensitive(self, value):
        """An unquoted value matches every case, as the legacy search does."""
        assert self._matches("980:LCD-Notes", {"980__": {"a": value}})

    def test_unquoted_value_stays_a_substring_search(self):
        """An unquoted value keeps its substring search (``SCICOM`` in a longer tag)."""
        assert self._matches("980:SCICOM", {"980__": {"a": "SCICOMMPUBLSCC"}})

    def test_quoted_value_is_an_exact_match(self):
        """A quoted value is case-insensitive but exact: no substring match."""
        assert self._matches('595:"CERN-TH"', {"595__": {"a": "cern-th"}})
        assert not self._matches('595:"CERN-TH"', {"595__": {"a": "CERN-TH Group"}})

    def test_trailing_wildcard_is_a_prefix_match(self):
        """A trailing wildcard matches any case and any suffix."""
        assert self._matches('037:"CLIC-Note*"', {"037__": {"a": "clic-note-123"}})
        assert not self._matches('037:"CLIC-Note*"', {"037__": {"a": "DP-Note-1"}})

    def test_matches_a_repeated_field(self):
        """A value matches when any repeated instance of the field carries it."""
        record = {"980__": [{"a": "PREPRINT"}, {"a": "ARTICLE"}]}
        assert self._matches("980:ARTICLE", record)

    def test_negated_term_excludes_the_record(self):
        """A negated term keeps excluding the record (case-insensitively)."""
        assert not self._matches(
            "980:ARTICLE -980:CLINOT",
            {"980__": [{"a": "ARTICLE"}, {"a": "clinot"}]},
        )


class TestConfQuery:
    """Build a model ``__query__`` from the queries of a collection configuration."""

    @staticmethod
    def _write(confs_dir, name, body):
        """Write a ``<name>.yaml`` collection configuration in ``confs_dir``."""
        with open(os.path.join(confs_dir, name + ".yaml"), "w") as fp:
            fp.write(textwrap.dedent(body))

    def test_joins_queries_and_always_excludes_system_records(self, tmp_path):
        """The queries are OR-ed, each wrapped, and the system records excluded."""
        self._write(
            str(tmp_path),
            "demo",
            """
            name: demo
            queries:
              - q: '980:LCD-Notes AND 591__b:approved'
              - q: '980:CLICLIB'
            """,
        )
        assert conf_query("demo", confs_dir=str(tmp_path)) == (
            "((980__:/(?i)LCD\\-Notes/ AND 591__.b:/(?i)approved/) "
            "OR (980__:/(?i)CLICLIB/)) "
            "-980__:/(?i)DELETED/ -980__:/(?i)HIDDEN/ -980__.a:/(?i)DUMMY/"
        )

    def test_extra_exclusions_are_translated_and_appended(self, tmp_path):
        """The per-model exclusions are translated and added after the defaults."""
        self._write(
            str(tmp_path),
            "demo",
            """
            name: demo
            queries:
              - q: '980:CLICLIB'
            """,
        )
        result = conf_query("demo", "-690C_:SCICOM", confs_dir=str(tmp_path))
        assert result.endswith("-980__.a:/(?i)DUMMY/ -690C_:/(?i)SCICOM/")

    def test_missing_queries_raises(self, tmp_path):
        """A configuration without queries is an error."""
        self._write(
            str(tmp_path),
            "demo",
            """
            name: demo
            queries: []
            """,
        )
        with pytest.raises(ValueError, match="`queries` is not set"):
            conf_query("demo", confs_dir=str(tmp_path))

    def test_query_without_text_raises(self, tmp_path):
        """A query entry without its ``q`` text is an error."""
        self._write(
            str(tmp_path),
            "demo",
            """
            name: demo
            queries:
              - dump_file_prefix: demo
            """,
        )
        with pytest.raises(ValueError, match="has no text"):
            conf_query("demo", confs_dir=str(tmp_path))
