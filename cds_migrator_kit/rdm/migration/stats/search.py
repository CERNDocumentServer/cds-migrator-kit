# -*- coding: utf-8 -*-
#
# Copyright (C) 2024 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM migration stats search module."""

import time

from copy import deepcopy
from opensearchpy import OpenSearch
from opensearchpy.exceptions import OpenSearchException


def generate_query(doc_type, identifier, legacy_to_rdm_events_map):
    """Generate legacy query based on event type."""
    q = deepcopy(legacy_to_rdm_events_map[doc_type]["query"])
    q["query"]["bool"]["must"][0]["match"]["id_bibrec"] = identifier
    q["query"]["bool"]["must"][1]["match"]["event_type"] = doc_type

    return q


def os_search(
    src_os_client,
    index,
    doc_type,
    identifier,
    search_size,
    search_scroll,
    legacy_to_rdm_events_map,
):
    ex = None
    i = 0
    q = generate_query(doc_type, identifier, legacy_to_rdm_events_map)
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
