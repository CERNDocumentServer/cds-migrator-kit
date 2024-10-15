from datetime import datetime
from copy import deepcopy

from .config import LEGACY_TO_RDM_EVENTS_MAP, DEST_SEARCH_INDEX_PREFIX


def process_download_event(entry, rec_context):
    """Entry from legacy stat events format.

    {
        "id_bibrec": 2884810,
        "event_type": "events.downloads",
        "file_format": "PDF",
        "country": "US",
        "file_version": 1,
        "bot": true,
        "unique_session_id": "d871bbbe43affd349103397a14badf1a6942ffaf7165eb330e270b32",
        "visitor_id": "d871bbbe43affd349103397a14badf1a6942ffaf7165eb330e270b32",
        "timestamp": 1703779131037,
        "id_bibdoc": 2502296,
        "level": 20
    }

    rec_context:
    {
        "legacy_recid": "2884810",
        "parent_recid": "zts3q-6ef46",
        "latest_version": "1mae4-skq89",
        "versions": [
            {
                "new_recid": "1mae4-skq89",
                "version": 2,
                "files": [
                    {
                        "legacy_file_id": 1568736,
                        "bucket_id": "155be22f-3038-49e0-9f17-9518eaac783a",
                        "file_key": "Summer student program report.pdf",
                        "file_id": "06cdb9d2-635f-4dbe-89fe-4b27afddeaa2",
                        "size": "1690854"
                    }
                ]
            }
        ]
    }
    """
    # Convert timestamp to strict_date_hour_minute_second format
    timestamp = entry["timestamp"]
    timestamp = datetime.utcfromtimestamp(timestamp / 1000).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )
    assert str(entry["id_bibrec"]) == str(rec_context["legacy_recid"])

    # Find the file version and assume the record version in the new system
    _legacy_file_version = entry["file_version"]
    _record_version = [
        rec for rec in rec_context["versions"] if rec["version"] == _legacy_file_version
    ]
    if _record_version:
        _record_version = _record_version[0]

    _file_context = list(
        filter(
            lambda x: (
                x if str(x["legacy_file_id"]) == str(entry["id_bibdoc"]) else False
            ),
            _record_version["files"],
        )
    )

    if not _file_context:
        return {}
    else:
        _file_context = _file_context[0]

    return {
        "timestamp": timestamp,
        # Note: bucket_id created on migration
        "bucket_id": _file_context["bucket_id"],
        "file_id": _file_context["file_id"],
        "file_key": _file_context["file_key"],
        "size": _file_context["size"],
        # Note id_bibrec is here with the old style
        "recid": _record_version["new_recid"],
        "parent_recid": rec_context["parent_recid"],
        "referrer": None,
        "via_api": False,
        "is_robot": entry.get("bot", False),
        "country": entry.get("country", ""),
        "visitor_id": entry["visitor_id"],
        "unique_session_id": entry["unique_session_id"],
        # Note: id_bibrec doesn't have the new format pids
        "unique_id": f"ui_{_record_version['new_recid']}",
    }


def process_pageview_event(entry, rec_context):
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
        "legacy_recid": "2884810",
        "parent_recid": "zts3q-6ef46",
        "latest_version": "1mae4-skq89"
        "versions": [
            {
                "new_recid": "1mae4-skq89",
                "version": 2,
                "files": [
                    {
                        "legacy_file_id": 1568736,
                        "bucket_id": "155be22f-3038-49e0-9f17-9518eaac783a",
                        "file_key": "Summer student program report.pdf",
                        "file_id": "06cdb9d2-635f-4dbe-89fe-4b27afddeaa2",
                        "size": "1690854"
                    }
                ]
            }
        ]
    }
    """
    """We log the legacy page views events **ALWAYS TO THE LATEST VERSION**"""
    # Convert timestamp to strict_date_hour_minute_second format
    timestamp = entry["timestamp"]
    timestamp = datetime.utcfromtimestamp(timestamp / 1000).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )

    assert str(entry["id_bibrec"]) == str(rec_context["legacy_recid"])

    if not rec_context.get("latest_version"):
        print("Check recid, ", json.dumps(rec_context))
        return {}

    return {
        "timestamp": timestamp,
        # Note id_bibrec is here with the old style
        "recid": rec_context["latest_version"],
        "parent_recid": rec_context["parent_recid"],
        "referrer": None,
        "via_api": False,
        "is_robot": entry.get("bot", False),
        "visitor_id": entry["visitor_id"],
        # Note: id_bibrec doesn't have the new format pids
        "unique_id": f"ui_{rec_context['latest_version']}",
        "unique_session_id": entry["unique_session_id"],
        "country": entry.get("country", ""),
    }


def prepare_new_doc(data, rec_context, logger, doc_type):
    """Produce a new statistic event for the destination cluster."""
    for doc in data["hits"]["hits"]:
        try:
            new_doc = deepcopy(doc)

            # remove to avoid reindexing
            new_doc.pop("_id", None)
            new_doc.pop("_score", None)

            event_type = new_doc["_source"].pop("event_type", None)

            if event_type != doc_type:
                raise Exception("Inconsistent doc type")
            processed_doc = {}
            if event_type == "events.downloads":
                processed_doc = process_download_event(new_doc["_source"], rec_context)
                index_type = LEGACY_TO_RDM_EVENTS_MAP[event_type]["type"]
            elif event_type == "events.pageviews":
                processed_doc = process_pageview_event(new_doc["_source"], rec_context)
                index_type = LEGACY_TO_RDM_EVENTS_MAP[event_type]["type"]
            else:
                continue

            logger.info("Processed: {0}".format(doc["_id"]))

            # Retrieve year and month from timestamp
            date_object = datetime.fromisoformat(processed_doc["timestamp"])
            year = f"{date_object.year:4}"
            month = f"{date_object.month:02}"

            yield {
                "_op_type": "index",
                "_index": f"{DEST_SEARCH_INDEX_PREFIX}-{index_type}-{year}-{month}",
                "_source": processed_doc,
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
