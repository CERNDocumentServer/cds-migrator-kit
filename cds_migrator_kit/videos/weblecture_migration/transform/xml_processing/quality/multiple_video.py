# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos transform step module."""
import logging
import re

from cds_migrator_kit.errors import ManualImportRequired
from cds_migrator_kit.videos.weblecture_migration.transform.xml_processing.quality.identifiers import (
    get_new_indico_id,
)

cli_logger = logging.getLogger("migrator")


def transform_multiple_video_record(multiple_video_record):
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
        file_event_id = re.split(r"[cs_]", file_indico_id, 1)[0]

        indico_id = next(
            (
                id
                for id in filtered_indico_ids
                if id in file_indico_id
                or str(get_new_indico_id(file_indico_id)) == id
                or str(get_new_indico_id(file_event_id)) == id
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
        iid = re.split(r"[cs_]", indico_id, 1)[0]
        new_indico_id = get_new_indico_id(iid)
        link = next(
            (
                l
                for l in links
                if iid in l["event_id"]
                or (new_indico_id and str(new_indico_id) in l["event_id"])
            ),
            None,
        )
        # still missing just generate link with indico id
        if not link:
            event_id = new_indico_id if new_indico_id else iid
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
            # try to get using indico link
            if link.get("date"):
                date = {"date": link.get("date")}
            # only one date, can be used for all
            elif dates_count == 1:
                date = {"date": dates[0]["date"]}
            else:
                raise ManualImportRequired(
                    f"Multi video record: No matching date for {indico_id}",
                    stage="transform",
                    priority="critical",
                )
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

    # raise if anything is missing
    for record in mapped:
        # raise if any date, event_id, or files is missing
        if (
            not record.get("date")
            or not record.get("event_id")
            or not record.get("files")
            or not record.get("url")
        ):
            raise ManualImportRequired(
                message="Multiple video record needs curation. Date, event_id, or files is missing",
                stage="transform",
                priority="critical",
            )
    return mapped, common


def transform_multiple_video_wihtout_indico(multiple_video_record):
    """Map multiple video record into list of dicts with file/date/link info.

    Returns:
        List[dict]: each dict has
            {
              "files": [...],
              "date": str | None,
              "location": str | None
            },
            None
    """
    dates = multiple_video_record["dates"]
    files = multiple_video_record["files"]

    mapped = []
    matched_dates = []
    for file_group in files:
        file_indico_id = file_group["master_path"].split("/")[-1]
        if len(dates) == 1:
            date = {
                "date": dates[0]["date"],
                "location": dates[0].get("location", None),
            }
        else:
            raise ManualImportRequired(
                f"Multi video record: No matching date for {file_indico_id}",
                stage="transform",
                priority="critical",
            )
        matched_dates.append(date["date"])
        mapped.append(
            {
                "files": file_group,
                "date": date["date"],
                "location": date.get("location"),
            }
        )

    return mapped, None
