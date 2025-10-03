# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos transform step module."""
import datetime
import logging
import os
import re
from pathlib import Path

import arrow
from flask import current_app
from invenio_accounts.models import User
from invenio_pidstore.models import PersistentIdentifier
from invenio_rdm_migrator.streams.records.transform import (
    RDMRecordEntry,
    RDMRecordTransform,
)
from sqlalchemy.exc import NoResultFound

from cds_migrator_kit.errors import (
    ManualImportRequired,
    MissingRequiredField,
    RestrictedFileDetected,
    UnexpectedValue,
)
from cds_migrator_kit.reports.log import MigrationProgressLogger, RecordStateLogger
from cds_migrator_kit.transform.dumper import CDSRecordDump
from cds_migrator_kit.transform.errors import LossyConversion
from cds_migrator_kit.videos.weblecture_migration.transform import (
    videos_migrator_marc21,
)
from cds_migrator_kit.videos.weblecture_migration.transform.transform_files import (
    TransformFiles,
)
from cds_migrator_kit.videos.weblecture_migration.transform.xml_processing.quality.collections import (
    append_collection_hierarchy,
)
from cds_migrator_kit.videos.weblecture_migration.transform.xml_processing.quality.identifiers import (
    get_new_indico_id,
    transform_legacy_urls,
)

cli_logger = logging.getLogger("migrator")


def is_multiple_video_record(is_multiple_video_record, multiple_video_record):
    """Return if the record is a multiple video record.
    Conditions:
    - Always multiple masters
    - If one indico link masters should include the indico id in the path (one or same length dates)
    - if multiple dates, length should be equal with masters (one or same length indico links)
    """
    if not is_multiple_video_record:
        return False

    dates_count = len(multiple_video_record["dates"])
    files_count = len(multiple_video_record["files"])
    # Filter out base IDs that are prefixes of other IDs
    filtered_indico_ids = [
        id
        for id in multiple_video_record["indico_ids"]
        if not any(
            other_id.startswith(id) and other_id != id
            for other_id in multiple_video_record["indico_ids"]
        )
    ]
    indico_ids_count = len(filtered_indico_ids)

    # Must have data in all fields
    if dates_count == 0 or files_count == 0 or indico_ids_count == 0:
        return False

    return True


def map_multiple_video_record(multiple_video_record):
    """Map multiple video record into list of dicts with file/date/link info.

    Returns:
        List[dict]: each dict has
            {
              "files": [...],
              "event_id": str,
              "url": str | None,
              "date": str | None,
              "location": str | None
            },
            common: {
                dates: [...],
                links: [...],
                indico_ids: [...],
            }
    """

    dates = multiple_video_record["dates"]
    files = multiple_video_record["files"]
    links = multiple_video_record["indico_links"]
    indico_ids = multiple_video_record["indico_ids"]

    dates_count = len(dates)

    # Filter out base IDs that are prefixes of other IDs
    filtered_indico_ids = sorted(
        id
        for id in indico_ids
        if not any(
            other_id.startswith(id) and other_id != id for other_id in indico_ids
        )
    )

    mapped = []
    matched_dates = []
    matched_links = []
    matched_indico_ids = []
    for file_group in files:
        file_indico_id = file_group["master_path"].split("/")[-1]
        indico_id = next(
            (
                id
                for id in filtered_indico_ids
                if id in file_indico_id or str(get_new_indico_id(file_indico_id)) == id
            ),
            None,
        )
        if not indico_id:
            raise ManualImportRequired(
                f"Multi video record: No matching indico id for {file_group['master_path']}",
                stage="transform",
                priority="critical",
            )
        # remove contribution for link and id
        iid = re.split(r"[cs]", indico_id, 1)[0]
        new_indico_id = get_new_indico_id(iid)
        link = next(
            (
                l
                for l in links
                if indico_id in l["event_id"]
                or (new_indico_id and str(new_indico_id) in l["event_id"])
            ),
            None,
        )
        # still missing just generate link with indico id
        if not link:
            event_id = new_indico_id if new_indico_id else indico_id
            link = {
                "url": f"https://indico.cern.ch/event/{event_id}",
            }
        date = next(
            (
                d
                for d in dates
                if d.get("event_id") == indico_id or d.get("event_id") == file_indico_id
            ),
            None,
        )
        if not date:
            # if date is missing try to get it from link
            if not link.get("date"):
                import pdb

                pdb.set_trace()
                raise ManualImportRequired(
                    f"Multi video record: No matching date for {indico_id}",
                    stage="transform",
                    priority="critical",
                )
            date = {"date": link.get("date")}
        matched_dates.append(date["date"])
        matched_links.append(link["url"])
        matched_indico_ids.append(indico_id)
        mapped.append(
            {
                "files": file_group,
                "event_id": new_indico_id if new_indico_id else indico_id,
                "url": link["url"],
                "date": date["date"],
                "location": date.get("location"),
            }
        )

    # extra dates
    common = {}
    # extra dates
    if len(matched_dates) != dates_count:
        all_dates = [d["date"] for d in dates]
        extra_dates = [d for d in all_dates if d not in matched_dates]
        if extra_dates:
            common["dates"] = [d for d in all_dates if d not in matched_dates]

    # extra links
    if len(matched_links) != len(links):
        all_links = [l["url"] for l in links]
        extra_links = [link for link in all_links if link not in matched_links]
        if extra_links:
            common["links"] = extra_links

    # extra indico ids
    if len(matched_indico_ids) != len(filtered_indico_ids):
        extra_indico_ids = [
            i for i in filtered_indico_ids if i not in matched_indico_ids
        ]
        if extra_indico_ids:
            common["indico_ids"] = extra_indico_ids

    return mapped, common

