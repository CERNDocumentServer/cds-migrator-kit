# -*- coding: utf-8 -*-
#
# Copyright (C) 2024 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM migration load module."""
import logging
import os
import json

from invenio_rdm_migrator.load.base import Load

from cds_migrator_kit.rdm.migration.stats.event_generator import prepare_new_doc
from cds_migrator_kit.rdm.migration.stats.log import StatsLogger
from cds_migrator_kit.rdm.migration.stats.search import (
    generate_query,
    os_count,
    os_scroll,
    os_search,
)


logger = StatsLogger.get_logger()

from opensearchpy import OpenSearch
from opensearchpy.exceptions import OpenSearchException
from opensearchpy.helpers import bulk

_QUERY_VIEWS = {
    "query": {
        "bool": {
            "must": [
                {"match": {"id_bibrec": "<recid>"}},
                {"match": {"event_type": "<type>"}},
            ]
        }
    }
}


class CDSRecordStatsLoad(Load):
    """CDSRecordStatsLoad."""

    LEGACY_TO_RDM_EVENTS_MAP = {
        "events.pageviews": {
            "type": "record-view",
            "query": _QUERY_VIEWS,
        },
        "events.downloads": {
            "type": "file-download",
            "query": _QUERY_VIEWS,
        },
    }

    def __init__(
        self,
        config,
        dry_run=False,
    ):
        """Constructor."""
        self.config = config
        self.dry_run = dry_run
        self._init_config(config)

    def _init_config(self, config):
        self.src_os_client = OpenSearch(hosts=config["SRC_SEARCH_HOSTS"])
        self.dest_os_client = OpenSearch(hosts=config["DEST_SEARCH_HOSTS"])

    def _prepare(self, entry):
        """Prepare the record."""
        pass

    def _generate_new_events(self, data, rec_context, logger, doc_type):
        try:
            new_docs_generated = prepare_new_doc(
                data,
                rec_context,
                logger,
                doc_type,
                self.LEGACY_TO_RDM_EVENTS_MAP,
                self.config["DEST_SEARCH_INDEX_PREFIX"],
            )
            if self.dry_run:
                for new_doc in new_docs:
                    logger.info(json.dumps(new_doc))
            else:
                bulk(self.dest_os_client, new_docs_generated, raise_on_error=True)
        except Exception as ex:
            logger.error(ex)

    def _process_legacy_events_for_recid(self, recid, rec_context, index, event_type):
        data = os_search(
            self.src_os_client,
            index,
            event_type,
            recid,
            self.config["SRC_SEARCH_SIZE"],
            self.config["SRC_SEARCH_SCROLL"],
            self.LEGACY_TO_RDM_EVENTS_MAP,
        )
        # Get the scroll ID
        sid = data["_scroll_id"]
        scroll_size = len(data["hits"]["hits"])
        total = data["hits"]["total"]["value"]
        logger.info("Total number of results for id: {0} <{1}>".format(total, recid))

        self._generate_new_events(data, rec_context, logger, doc_type=event_type)

        tot_chunks = total // self.config["SRC_SEARCH_SIZE"]
        if total % self.config["SRC_SEARCH_SIZE"] > 0:
            tot_chunks += 1

        i = 0
        while scroll_size > 0:
            i += 1
            logger.info("Getting results {0}/{1}".format(i, tot_chunks))

            data = os_scroll(self.src_os_client, sid, self.config["SRC_SEARCH_SCROLL"])

            # Update the scroll ID
            sid = data["_scroll_id"]

            # Get the number of results that returned in the last scroll
            scroll_size = len(data["hits"]["hits"])

            if total == 0:
                continue

            self._generate_new_events(data, rec_context, logger, doc_type=event_type)

        self.src_os_client.clear_scroll(scroll_id=sid)

        logger.info("Done!")

    def validate_stats_for_recid(self, recid, record, event_type):
        """Validate that stats in legacy and new RDM."""
        legacy_total = os_count(
            self.src_os_client,
            "cds*",
            q=generate_query(event_type, recid, self.LEGACY_TO_RDM_EVENTS_MAP),
        )
        logger.info(
            f"Total amount of records of type `{event_type}` in legacy: {legacy_total['count']}"
        )
        _index = f"{self.config['DEST_SEARCH_INDEX_PREFIX']}-{self.LEGACY_TO_RDM_EVENTS_MAP[event_type]['type']}*"
        # refresh index
        self.dest_os_client.indices.refresh(index=_index)
        new_total = os_count(
            self.dest_os_client,
            _index,
            q={
                "query": {
                    "bool": {
                        "must": [{"match": {"parent_recid": record["parent_recid"]}}]
                    }
                }
            },
        )
        logger.info(
            f"Total amount of records of type `{event_type}` in RDM: {new_total['count']}"
        )

        try:
            assert legacy_total["count"] == new_total["count"]
            logger.warning(
                f"Successfully migrated statistics for {recid} `{event_type}` in RDM: {new_total['count']}"
            )
        except AssertionError as e:
            logger.error(
                f"Not all events of type {event_type} were migrated for record: {recid}"
            )

    def _load(self, entry):
        """Use the services to load the entries."""
        if entry:
            event_type, record = entry
            recid = record["legacy_recid"]
            try:
                self._process_legacy_events_for_recid(recid, record, "cds*", event_type)
                self.validate_stats_for_recid(recid, record, event_type)
            except Exception as ex:
                logger.error(ex)

    def _cleanup(self, *args, **kwargs):
        """Cleanup the entries."""
        pass
