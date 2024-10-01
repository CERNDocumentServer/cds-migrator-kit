import os
import time
import logging
import json

import queue as _queue
from multiprocessing import Pool, Queue
from opensearchpy.helpers import bulk

from .config import (
    EVENT_TYPES,
    LEGACY_INDICES,
    ROOT_PATH,
    RECID_LIST_FILE,
    SRC_SEARCH_SIZE,
)
from .log import setup_logger
from .event_generator import prepare_new_doc
from .search import src_os_client, dest_os_client, os_search, os_scroll


def generate_new_events(os_client, data, rec_context, logger, doc_type, dry_run=True):
    try:
        new_docs = prepare_new_doc(data, rec_context, logger, doc_type)
        if dry_run:
            for new_doc in new_docs:
                logger.info(json.dumps(new_doc))
        else:
            bulk(os_client, new_docs, raise_on_error=True)
    except Exception as ex:
        logger.error(ex)


def run_process(index, t, recid, rec_context, dry_run=True):
    logger = logging.getLogger("{0}-{1}-logger".format(index, t))
    if not logger.handlers:
        # Avoid adding multiple handlers
        logger = setup_logger(
            "{0}-{1}-logger".format(index, t), "{0}-{1}.log".format(index, t)
        )
    logger.info("Started! <{0}>".format(recid))
    logger.info("Record context! <{0}>".format(json.dumps(rec_context)))

    try:
        data = os_search(index, t, recid)

        # Get the scroll ID
        sid = data["_scroll_id"]
        scroll_size = len(data["hits"]["hits"])
        total = data["hits"]["total"]["value"]
        logger.info("Total number of results for id: {0} <{1}>".format(total, recid))
        generate_new_events(
            dest_os_client, data, rec_context, logger, doc_type=t, dry_run=dry_run
        )
        tot_chunks = total // SRC_SEARCH_SIZE
        if total % SRC_SEARCH_SIZE > 0:
            tot_chunks += 1

        i = 0
        while scroll_size > 0:
            i += 1
            logger.info("Getting results {0}/{1}".format(i, tot_chunks))

            data = os_scroll(sid)

            # Update the scroll ID
            sid = data["_scroll_id"]

            # Get the number of results that returned in the last scroll
            scroll_size = len(data["hits"]["hits"])

            if total == 0:
                continue

            generate_new_events(
                dest_os_client,
                data,
                rec_context,
                logger,
                doc_type=t,
                dry_run=dry_run,
            )
        src_os_client.clear_scroll(scroll_id=sid)
        logger.info("Done!")
    except Exception as ex:
        logger.error(ex)


def run(dry_run=True):
    """
    Legacy record format:
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
    if not os.path.exists(ROOT_PATH):
        os.mkdir(ROOT_PATH)

    # Timing the method
    start_time = time.time()

    with open(RECID_LIST_FILE, "r") as file:
        try:
            records = json.load(file)
            for legacy_record in records:
                for index_name in LEGACY_INDICES:
                    for t in EVENT_TYPES:
                        run_process(
                            index_name,
                            t,
                            legacy_record["legacy_recid"],
                            legacy_record,
                            dry_run=dry_run,
                        )
        except json.JSONDecodeError:
            print("Error decoding JSON")

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time} seconds")
