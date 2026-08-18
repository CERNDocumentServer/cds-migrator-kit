# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Tests for stats migration: event generation, query building, and load filtering."""

import logging
from copy import deepcopy
from unittest.mock import MagicMock, call, patch

import pytest

from cds_migrator_kit.rdm.stats.event_generator import (
    flag_robots_and_COUNTER,
    prepare_new_doc,
    process_download_event,
    process_pageview_event,
)
from cds_migrator_kit.rdm.stats.load import _QUERY_VIEWS, CDSRecordStatsLoad
from cds_migrator_kit.rdm.stats.search import generate_query

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

REC_WITH_FILES = {
    "legacy_recid": "1156138",
    "parent_recid": "0m8n6-qnx43",
    "latest_version": "hk1ez-6ar45",
    "versions": [
        {
            "new_recid": "hk1ez-6ar45",
            "version": 1,
            "files": [
                {
                    "legacy_file_id": 102798,
                    "bucket_id": "5d5bce60-cd0e-4cb0-bdc8-853b9f6e63da",
                    "file_key": "EPJC.54.365-370.pdf",
                    "file_id": "46285513-c05e-48a1-8748-b19e1f887443",
                    "size": "380884",
                }
            ],
        }
    ],
}

REC_NO_FILES = {
    "legacy_recid": "1156137",
    "parent_recid": "s7nz4-rq320",
    "latest_version": "x2a2h-k4p17",
    "versions": [{"new_recid": "x2a2h-k4p17", "version": 1, "files": []}],
}

LEGACY_TO_RDM_EVENTS_MAP = {
    "events.pageviews": {"type": "record-view", "query": deepcopy(_QUERY_VIEWS)},
    "events.downloads": {"type": "file-download", "query": deepcopy(_QUERY_VIEWS)},
}

LESS_THAN_DATE = "2025-01-01T00:00:00"

DOWNLOAD_EVENT = {
    "id_bibrec": 1156138,
    "event_type": "events.downloads",
    "file_format": "PDF",
    "country": "CH",
    "file_version": 1,
    "bot": False,
    "unique_session_id": "abc123",
    "visitor_id": "abc123",
    "timestamp": 1703779131037,
    "id_bibdoc": 102798,
}

PAGEVIEW_EVENT = {
    "id_bibrec": 1156138,
    "event_type": "events.pageviews",
    "country": "CH",
    "visitor_id": "xyz789",
    "unique_session_id": "xyz789",
    "bot": False,
    "timestamp": 1703880355592,
}


# ---------------------------------------------------------------------------
# generate_query
# ---------------------------------------------------------------------------


def test_generate_query_without_file_ids():
    q = generate_query(
        "events.pageviews", "1156138", LEGACY_TO_RDM_EVENTS_MAP, LESS_THAN_DATE
    )
    filters = q["query"]["bool"]["filter"]
    assert not any(
        "terms" in f for f in filters
    ), "No terms filter expected for pageviews"
    assert q["query"]["bool"]["must"][0]["match"]["id_bibrec"] == "1156138"
    assert q["query"]["bool"]["must"][1]["match"]["event_type"] == "events.pageviews"


def test_generate_query_with_file_ids():
    file_ids = [102798, 99999]
    q = generate_query(
        "events.downloads",
        "1156138",
        LEGACY_TO_RDM_EVENTS_MAP,
        LESS_THAN_DATE,
        file_ids=file_ids,
    )
    filters = q["query"]["bool"]["filter"]
    terms_filters = [f for f in filters if "terms" in f]
    assert len(terms_filters) == 1
    assert terms_filters[0]["terms"]["id_bibdoc"] == file_ids


def test_generate_query_timestamp_conversion():
    q = generate_query(
        "events.downloads", "1156138", LEGACY_TO_RDM_EVENTS_MAP, "2024-12-31T23:59:59"
    )
    ts = q["query"]["bool"]["filter"][0]["range"]["timestamp"]["lt"]
    assert isinstance(ts, int)
    assert ts > 0


def test_generate_query_does_not_mutate_template():
    original = deepcopy(LEGACY_TO_RDM_EVENTS_MAP)
    generate_query(
        "events.downloads",
        "1156138",
        LEGACY_TO_RDM_EVENTS_MAP,
        LESS_THAN_DATE,
        file_ids=[102798],
    )
    assert LEGACY_TO_RDM_EVENTS_MAP == original


# ---------------------------------------------------------------------------
# flag_robots_and_COUNTER
# ---------------------------------------------------------------------------


def test_flag_robots_known_bot():
    is_bot, before_counter = flag_robots_and_COUNTER({"bot": True})
    assert is_bot is True
    assert before_counter is False


def test_flag_robots_known_human():
    is_bot, before_counter = flag_robots_and_COUNTER({"bot": False})
    assert is_bot is False
    assert before_counter is False


def test_flag_robots_missing_bot_field():
    is_bot, before_counter = flag_robots_and_COUNTER({})
    assert is_bot is False
    assert before_counter is True


# ---------------------------------------------------------------------------
# process_download_event
# ---------------------------------------------------------------------------


def test_process_download_event_matched_file():
    logger = MagicMock()
    result = process_download_event(DOWNLOAD_EVENT, REC_WITH_FILES, logger)

    assert result["recid"] == "hk1ez-6ar45"
    assert result["parent_recid"] == "0m8n6-qnx43"
    assert result["bucket_id"] == "5d5bce60-cd0e-4cb0-bdc8-853b9f6e63da"
    assert result["file_id"] == "46285513-c05e-48a1-8748-b19e1f887443"
    assert result["file_key"] == "EPJC.54.365-370.pdf"
    assert result["is_robot"] is False
    assert result["before_COUNTER"] is False
    assert result["is_lcds"] is True
    assert result["country"] == "CH"
    logger.warning.assert_not_called()


def test_process_download_event_unmigrated_file():
    """Event for a bibdoc that does not exist in rec_context files → returns {}."""
    logger = MagicMock()
    event = {**DOWNLOAD_EVENT, "id_bibdoc": 99999}
    result = process_download_event(event, REC_WITH_FILES, logger)
    assert result == {}
    logger.warning.assert_called_once()


def test_process_download_event_missing_version():
    """Event for a file_version that doesn't exist in versions → returns {}."""
    logger = MagicMock()
    event = {**DOWNLOAD_EVENT, "file_version": 99}
    result = process_download_event(event, REC_WITH_FILES, logger)
    assert result == {}
    logger.warning.assert_called_once()


def test_process_download_event_before_counter():
    """Event without bot field is marked before_COUNTER."""
    logger = MagicMock()
    event = {k: v for k, v in DOWNLOAD_EVENT.items() if k != "bot"}
    result = process_download_event(event, REC_WITH_FILES, logger)
    assert result["before_COUNTER"] is True
    assert result["is_robot"] is False


# ---------------------------------------------------------------------------
# process_pageview_event
# ---------------------------------------------------------------------------


def test_process_pageview_event():
    logger = MagicMock()
    result = process_pageview_event(PAGEVIEW_EVENT, REC_WITH_FILES, logger)

    assert result["recid"] == "hk1ez-6ar45"
    assert result["parent_recid"] == "0m8n6-qnx43"
    assert result["is_robot"] is False
    assert result["before_COUNTER"] is False
    assert result["is_lcds"] is True
    assert result["country"] == "CH"
    logger.error.assert_not_called()


def test_process_pageview_event_always_goes_to_latest_version():
    """Pageviews are always attributed to the latest_version regardless of file."""
    logger = MagicMock()
    result = process_pageview_event(PAGEVIEW_EVENT, REC_WITH_FILES, logger)
    assert result["recid"] == REC_WITH_FILES["latest_version"]


def test_process_pageview_event_missing_latest_version():
    logger = MagicMock()
    ctx = {**REC_WITH_FILES, "latest_version": None}
    result = process_pageview_event(PAGEVIEW_EVENT, ctx, logger)
    assert result == {}
    logger.error.assert_called_once()


# ---------------------------------------------------------------------------
# prepare_new_doc
# ---------------------------------------------------------------------------


def _make_os_response(events, event_type):
    """Build a minimal OpenSearch hits response."""
    return {
        "hits": {
            "hits": [
                {
                    "_index": "cds-2024",
                    "_id": f"hit-{i}",
                    "_score": 1.0,
                    "_source": {**e, "event_type": event_type},
                }
                for i, e in enumerate(events)
            ]
        }
    }


def test_prepare_new_doc_download():
    logger = MagicMock()
    data = _make_os_response(
        [{k: v for k, v in DOWNLOAD_EVENT.items() if k != "event_type"}],
        "events.downloads",
    )
    docs = list(
        prepare_new_doc(
            data,
            REC_WITH_FILES,
            logger,
            "events.downloads",
            LEGACY_TO_RDM_EVENTS_MAP,
            "events-stats",
        )
    )
    assert len(docs) == 1
    doc = docs[0]
    assert doc["_op_type"] == "create"
    assert "file-download" in doc["_index"]
    assert doc["_id"].startswith("migrated_")
    assert doc["_source"]["is_lcds"] is True


def test_prepare_new_doc_pageview():
    logger = MagicMock()
    data = _make_os_response(
        [{k: v for k, v in PAGEVIEW_EVENT.items() if k != "event_type"}],
        "events.pageviews",
    )
    docs = list(
        prepare_new_doc(
            data,
            REC_WITH_FILES,
            logger,
            "events.pageviews",
            LEGACY_TO_RDM_EVENTS_MAP,
            "events-stats",
        )
    )
    assert len(docs) == 1
    doc = docs[0]
    assert "record-view" in doc["_index"]
    assert doc["_source"]["is_lcds"] is True


def test_prepare_new_doc_skips_unmigrated_file():
    """Events for files not in rec_context produce no output docs."""
    logger = MagicMock()
    event = {k: v for k, v in DOWNLOAD_EVENT.items() if k != "event_type"}
    event["id_bibdoc"] = 99999
    data = _make_os_response([event], "events.downloads")
    docs = list(
        prepare_new_doc(
            data,
            REC_WITH_FILES,
            logger,
            "events.downloads",
            LEGACY_TO_RDM_EVENTS_MAP,
            "events-stats",
        )
    )
    assert docs == []


def test_prepare_new_doc_year_in_index():
    """Index name includes the year extracted from the event timestamp."""
    logger = MagicMock()
    data = _make_os_response(
        [{k: v for k, v in DOWNLOAD_EVENT.items() if k != "event_type"}],
        "events.downloads",
    )
    docs = list(
        prepare_new_doc(
            data,
            REC_WITH_FILES,
            logger,
            "events.downloads",
            LEGACY_TO_RDM_EVENTS_MAP,
            "events-stats",
        )
    )
    assert "-2023" in docs[0]["_index"]


# ---------------------------------------------------------------------------
# CDSRecordStatsLoad — file_ids threaded through fetch and validate
# ---------------------------------------------------------------------------


def _make_load(dry_run=True):
    config = {
        "SRC_SEARCH_HOSTS": [{"host": "localhost", "port": 9200}],
        "DEST_SEARCH_HOSTS": [{"host": "localhost", "port": 9200}],
        "SRC_SEARCH_SIZE": 10,
        "SRC_SEARCH_SCROLL": "1h",
        "DEST_SEARCH_INDEX_PREFIX": "events-stats",
    }
    with patch("cds_migrator_kit.rdm.stats.load.OpenSearch"):
        load = CDSRecordStatsLoad(
            config=config, less_than_date=LESS_THAN_DATE, dry_run=dry_run
        )
    return load


def test_process_downloads_passes_file_ids_to_search():
    load = _make_load()
    migrated_file_ids = [
        f["legacy_file_id"] for v in REC_WITH_FILES["versions"] for f in v["files"]
    ]

    with patch("cds_migrator_kit.rdm.stats.load.os_search") as mock_search, patch(
        "cds_migrator_kit.rdm.stats.load.os_scroll"
    ) as mock_scroll:

        mock_search.return_value = {
            "_scroll_id": "sid1",
            "hits": {"hits": [], "total": {"value": 0}},
        }
        mock_scroll.return_value = {"_scroll_id": "sid1", "hits": {"hits": []}}
        load.src_os_client.clear_scroll = MagicMock()

        load._process_legacy_events_for_recid(
            REC_WITH_FILES["legacy_recid"], REC_WITH_FILES, "cds-2*", "events.downloads"
        )

    _, kwargs = mock_search.call_args
    assert kwargs.get("file_ids") == migrated_file_ids


def test_process_pageviews_passes_no_file_ids_to_search():
    load = _make_load()

    with patch("cds_migrator_kit.rdm.stats.load.os_search") as mock_search, patch(
        "cds_migrator_kit.rdm.stats.load.os_scroll"
    ) as mock_scroll:

        mock_search.return_value = {
            "_scroll_id": "sid1",
            "hits": {"hits": [], "total": {"value": 0}},
        }
        mock_scroll.return_value = {"_scroll_id": "sid1", "hits": {"hits": []}}
        load.src_os_client.clear_scroll = MagicMock()

        load._process_legacy_events_for_recid(
            REC_WITH_FILES["legacy_recid"], REC_WITH_FILES, "cds-2*", "events.pageviews"
        )

    _, kwargs = mock_search.call_args
    assert kwargs.get("file_ids") is None


def test_validate_downloads_uses_file_ids_in_legacy_count():
    load = _make_load()
    migrated_file_ids = [
        f["legacy_file_id"] for v in REC_WITH_FILES["versions"] for f in v["files"]
    ]

    with patch("cds_migrator_kit.rdm.stats.load.os_count") as mock_count, patch(
        "cds_migrator_kit.rdm.stats.load.generate_query"
    ) as mock_query:

        mock_count.return_value = {"count": 5}
        mock_query.return_value = {}
        load.dest_os_client.indices.refresh = MagicMock()

        load.validate_stats_for_recid(
            REC_WITH_FILES["legacy_recid"], REC_WITH_FILES, "events.downloads"
        )

    mock_query.assert_called_once()
    _, kwargs = mock_query.call_args
    assert kwargs.get("file_ids") == migrated_file_ids


def test_validate_pageviews_passes_no_file_ids():
    load = _make_load()

    with patch("cds_migrator_kit.rdm.stats.load.os_count") as mock_count, patch(
        "cds_migrator_kit.rdm.stats.load.generate_query"
    ) as mock_query:

        mock_count.return_value = {"count": 3}
        mock_query.return_value = {}
        load.dest_os_client.indices.refresh = MagicMock()

        load.validate_stats_for_recid(
            REC_WITH_FILES["legacy_recid"], REC_WITH_FILES, "events.pageviews"
        )

    mock_query.assert_called_once()
    _, kwargs = mock_query.call_args
    assert kwargs.get("file_ids") is None
