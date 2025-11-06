# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos migration stats events generator module."""
import json
from copy import deepcopy
from datetime import datetime


def flag_robots_and_COUNTER(entry):
    """Return a tuple of booleans.

    First element is the value of is_bot and the second the flag to mark the event as
    `before_COUNTER` as we don't have information if the registered event was from a bot
    or not.
    """
    is_bot_missing = "bot" not in entry
    if is_bot_missing:
        # when is_bot is missing, we assume that it was a human
        # but we mark this uncertainty with before_COUNTER =True
        is_bot = False
        before_COUNTER = True
    else:
        is_bot = entry["bot"]
        before_COUNTER = False
    return is_bot, before_COUNTER


def process_pageview_event(entry, rec_context, logger):
    """Entry from legacy stat events format.

    {
        "_index": "cds-2023",
        "_id": "AYy3LvO8Bd18JHv_G38-",
        "_score": 1.5853858,
        "_source": {
          "id_bibrec": 2884810,
          "event_type": "events.pageviews",
          "level": 20,
          "country": "CH",
          "visitor_id": "6899357c893c50f658069e1e4738cc7f656fb58f80def18d8294c313",
          "unique_session_id": "6899357c893c50f658069e1e4738cc7f656fb58f80def18d8294c313",
          "bot": false,
          "timestamp": 1703880355592
        }
    }

    rec_context:
    {
        "legacy_recid":"1454924",
        "cds_videos_recid":"119",
        "videos_record_uuid"":"a5ee8ac2-4150-4f9e-9a97-3ab28ac9803f",
        ""
    }
    """
    """We log the legacy page views events to record."""
    # filter out robot events
    is_bot, before_COUNTER = flag_robots_and_COUNTER(entry)

    # Convert timestamp to strict_date_hour_minute_second format
    timestamp = entry["timestamp"]
    timestamp = datetime.utcfromtimestamp(timestamp / 1000).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )

    assert str(entry["id_bibrec"]) == str(rec_context["legacy_recid"])

    videos_recid = rec_context["cds_videos_recid"]
    videos_recid_uuid = rec_context["videos_record_uuid"]
    pid_type = "recid"

    return {
        "timestamp": timestamp,
        "record_id": videos_recid_uuid,
        "pid_type": pid_type,
        "pid_value": videos_recid,
        "is_robot": is_bot,
        "visitor_id": entry.get("visitor_id", ""),
        "unique_id": f"{pid_type}_{videos_recid}",
        "unique_session_id": entry["unique_session_id"],
        "updated_timestamp": datetime.utcnow().isoformat(),
        # Mark the event as migrated
        "is_cds": True,
        # Mark if event was COUNTER compliant
        "before_COUNTER": before_COUNTER,
    }


def prepare_new_doc(
    data,
    rec_context,
    logger,
    doc_type,
    legacy_to_rdm_events_map,
    dest_search_index_prefix,
):
    """Produce a new statistic event for the destination cluster."""
    for doc in data["hits"]["hits"]:
        try:
            new_doc = deepcopy(doc)
            # remove to avoid reindexing
            new_doc["_id"] = f"migrated_{new_doc['_id']}"

            new_doc.pop("_score", None)

            event_type = new_doc["_source"].pop("event_type", None)

            if event_type != doc_type:
                raise Exception("Inconsistent doc type")

            if event_type == "events.pageviews":
                processed_doc = process_pageview_event(
                    new_doc["_source"], rec_context, logger
                )
                index_type = legacy_to_rdm_events_map[event_type]["type"]
            else:
                continue

            if not processed_doc:
                logger.warning(
                    "[SKIPPING] index: {0} - type: {1} - id: {2} - source: {3}".format(
                        doc["_index"],
                        doc["_source"]["event_type"],
                        doc["_id"],
                        doc["_source"],
                    )
                )
                continue
            logger.info("Processed: {0}".format(doc["_id"]))

            # Retrieve year from timestamp
            date_object = datetime.fromisoformat(processed_doc["timestamp"])
            year = f"{date_object.year:4}"

            yield {
                "_op_type": "create",
                "_index": f"{dest_search_index_prefix}-{index_type}-{year}",
                "_source": processed_doc,
                "_id": new_doc["_id"],
            }
        except Exception as ex:
            logger.error(
                "index: {0} - type: {1} - id: {2} - source: {3} error: {4}".format(
                    doc["_index"],
                    doc["_source"]["event_type"],
                    doc["_id"],
                    doc["_source"],
                    str(ex),
                )
            )
            continue
