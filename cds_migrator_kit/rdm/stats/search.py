# -*- coding: utf-8 -*-
#
# Copyright (C) 2024 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM migration stats search module."""

import json
import time
from copy import deepcopy
from datetime import datetime

from opensearchpy.exceptions import OpenSearchException
from opensearchpy.helpers import BulkIndexError, parallel_bulk


def generate_query(doc_type, identifier, legacy_to_rdm_events_map, less_than_date):
    """Generate legacy query based on event type."""
    q = deepcopy(legacy_to_rdm_events_map[doc_type]["query"])
    q["query"]["bool"]["must"][0]["match"]["id_bibrec"] = identifier
    q["query"]["bool"]["must"][1]["match"]["event_type"] = doc_type

    # Convert to datetime object
    dt = datetime.strptime(less_than_date, "%Y-%m-%dT%H:%M:%S")
    timestamp_ms = int(dt.timestamp() * 1000)
    q["query"]["bool"]["filter"][0]["range"]["timestamp"]["lt"] = timestamp_ms

    return q


def os_search(
    src_os_client,
    index,
    doc_type,
    identifier,
    search_size,
    search_scroll,
    legacy_to_rdm_events_map,
    less_than_date,
):
    """Sear utility."""
    ex = None
    i = 0
    q = generate_query(doc_type, identifier, legacy_to_rdm_events_map, less_than_date)
    while i < 10:
        try:
            return src_os_client.search(
                index=index,
                size=search_size,
                scroll=search_scroll,
                body=q,
            )
        except OpenSearchException as _ex:
            i += 1
            ex = _ex
            time.sleep(10)
    raise ex


def os_scroll(src_os_client, scroll_id, scroll_size):
    """Scroll utility."""
    ex = None
    i = 0
    while i < 10:
        try:
            return src_os_client.scroll(scroll_id=scroll_id, scroll=scroll_size)
        except OpenSearchException as _ex:
            i += 1
            ex = _ex
            time.sleep(10)
    raise ex


def os_count(src_os_client, index, q):
    """Count utility."""
    ex = None
    i = 0
    while i < 10:
        try:
            return src_os_client.count(
                index=index,
                body=q,
            )
        except OpenSearchException as _ex:
            i += 1
            ex = _ex
            time.sleep(10)
    raise ex


def bulk_index_documents(
    client,
    documents,
    logger,
    chunk_size=500,
    max_chunk_bytes=50 * 1024 * 1024,
):
    """Index documents into Opensearch.

    Uses parallel_bulk with improved readability and error handling.
    """
    try:
        # Execute parallel_bulk with configuration for improved performance
        for ok, action in parallel_bulk(
            client,
            actions=documents,
            chunk_size=chunk_size,
            max_chunk_bytes=max_chunk_bytes,
            raise_on_error=True,  # Handle errors manually for better control
            raise_on_exception=True,
            ignore_status=409,  # Ignore 409 Conflict status for existing documents
        ):
            pass

    except BulkIndexError as e:
        for error in e.errors:
            _failed_doc = {
                "_op_type": "create",
                "_index": error["create"]["_index"],
                "_source": error["create"]["data"],
                "_id": error["create"]["_id"],
            }
            logger.error(f"Failed to index: {json.dumps(_failed_doc)}")
