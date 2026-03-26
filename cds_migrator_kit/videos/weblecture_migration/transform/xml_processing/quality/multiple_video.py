# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos transform step module."""
import copy
import logging
import re

import pycountry

from cds_migrator_kit.errors import ManualImportRequired, UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.parsers import clean_str
from cds_migrator_kit.videos.weblecture_migration.transform.xml_processing.quality.identifiers import (
    get_new_indico_id,
    transform_legacy_urls,
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
            if multiple_video_record["recid"] == 468903:
                # use publication date for missing dates
                mapped.append(
                    {
                        "files": file_group,
                        "date": "2000-08-07",
                    }
                )
                continue
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
        # raise if any date or files is missing
        if not record.get("date") or not record.get("files"):
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


def parse_entry(entry):
    """Parse entry into code and value."""
    left, value = entry.split(":", 1)
    code = left.split("__")[-1].split("_")[-1]
    return code, value


def grouped_values_with_code(entries):
    """
    Group flat entries into logical MARC-like groups.
    A new group starts when a new 9 subfield appears.
    """
    groups = []
    current_parsed = {}
    current_raw = []

    for entry in entries:
        code, value = parse_entry(entry)

        if code == "9" and current_raw:
            groups.append({"parsed": current_parsed, "raw": current_raw})
            current_parsed = {}
            current_raw = []

        current_parsed[code] = value
        current_raw.append(entry)

    if current_raw:
        groups.append({"parsed": current_parsed, "raw": current_raw})

    return groups


def get_single_selector(event_id):
    """
    Examples:
        CERN-VIDEO-C-123-A      -> "a"
        CERN-VIDEO-C-402-A_pt1  -> "a"
        CERN-VIDEO-C-402-A_pt2  -> "a"
        CERN-VIDEO-C-123-B-C    -> None
    """
    if not event_id:
        return None

    last_part = event_id.split("-")[-1]
    match = re.fullmatch(r"([A-Za-z])(?:_pt\d+)?", last_part)
    if match:
        return match.group(1).lower()

    return None


def match_with_code(entries, event_id, value_code="a"):
    """
    Returns:
        matches: matched values
        matched_groups: raw groups that matched this record

    Rules:
    - groups without selector 8 are ignored for matching
    - groups with selector but without target value are ignored for matching
    - combined ids like ...-B-C do not match anything
    - A_pt1 / A_pt2 both match selector a
    """
    matches = []
    matched_groups = []
    event_selector = get_single_selector(event_id)

    for group in grouped_values_with_code(entries):
        parsed = group["parsed"]
        raw = group["raw"]

        selector = parsed.get("8")
        value = parsed.get(value_code)

        if not selector:
            continue

        if value is None:
            continue

        if event_selector and selector.lower() == event_selector:
            if value != "":
                matches.append(value)
            matched_groups.append(tuple(raw))

    return matches, matched_groups


def remove_matched_groups(entries, matched_groups):
    """Remove groups that were matched at least once."""
    matched_groups = set(matched_groups)
    remaining_entries = []

    for group in grouped_values_with_code(entries):
        raw_tuple = tuple(group["raw"])
        if raw_tuple not in matched_groups:
            remaining_entries.extend(group["raw"])

    return remaining_entries


def normalize_languages(raw_langs):
    """Normalize languages."""
    normalized = []

    for r in raw_langs:
        try:
            lang = pycountry.languages.lookup(clean_str(r).lower()).alpha_2.lower()
        except Exception:
            raise UnexpectedValue(field="041__", subfield="a", value=r)

        if lang not in normalized:
            normalized.append(lang)

    return normalized


def try_to_match_metadata(multiple_video_records, _curation):
    """Try to match curated metadata for multiple video record."""
    curation = copy.deepcopy(_curation)
    mapped_multiple_video_records = copy.deepcopy(multiple_video_records)

    digitized_description = curation.get("digitized_description", [])
    digitized_language = curation.get("digitized_language", [])
    digitized_keywords = curation.get("digitized_keywords", [])
    report_numbers = _curation.get("legacy_report_number", [])

    matched_description_groups = set()
    matched_language_groups = set()
    matched_keyword_groups = set()
    matched_report_numbers = set()

    for mapped_multiple_video_record in mapped_multiple_video_records:
        files = mapped_multiple_video_record["files"]
        event_id = mapped_multiple_video_record.get("event_id")

        # If indico id is present, we can't match
        if event_id:
            continue

        event_id = files.get("master_path", "").split("/")[-1]
        if not event_id:
            continue

        matched_digitized_description, description_groups = match_with_code(
            digitized_description, event_id, value_code="a"
        )
        matched_digitized_language, language_groups = match_with_code(
            digitized_language, event_id, value_code="a"
        )
        matched_digitized_keywords, keyword_groups = match_with_code(
            digitized_keywords, event_id, value_code="a"
        )

        matched_description_groups.update(description_groups)
        matched_language_groups.update(language_groups)
        matched_keyword_groups.update(keyword_groups)

        if matched_digitized_description:
            mapped_multiple_video_record["description"] = matched_digitized_description

        if matched_digitized_language:
            mapped_multiple_video_record["language"] = normalize_languages(
                matched_digitized_language
            )

        if matched_digitized_keywords:
            mapped_multiple_video_record["keywords"] = matched_digitized_keywords

        matched_report_number = next(
            (
                report_number
                for report_number in report_numbers
                if report_number == event_id
            ),
            None,
        )
        if matched_report_number:
            mapped_multiple_video_record["report_number"] = [matched_report_number]
            matched_report_numbers.add(matched_report_number)

    # Remove matched groups only after all records have been processed
    digitized_description = remove_matched_groups(
        digitized_description, matched_description_groups
    )
    digitized_language = remove_matched_groups(
        digitized_language, matched_language_groups
    )
    digitized_keywords = remove_matched_groups(
        digitized_keywords, matched_keyword_groups
    )
    report_numbers = [
        report_number
        for report_number in report_numbers
        if report_number not in matched_report_numbers
    ]

    # update curation
    if digitized_description:
        curation["digitized_description"] = digitized_description
    else:
        curation.pop("digitized_description", None)
    if digitized_language:
        curation["digitized_language"] = digitized_language
    else:
        curation.pop("digitized_language", None)
    if digitized_keywords:
        curation["digitized_keywords"] = digitized_keywords
    else:
        curation.pop("digitized_keywords", None)
    if report_numbers:
        curation["legacy_report_number"] = report_numbers
    else:
        curation.pop("legacy_report_number", None)

    return mapped_multiple_video_records, curation


def update_metadata_multiple_video_record(record, common_metadata):
    """Update metadata for multiple video record."""
    # Copy common metadata
    metadata = copy.deepcopy(common_metadata)

    # Use the correct metadata for each record
    event_id = record.get("event_id")
    url = record.get("url")
    date = record["date"]
    location = record.get("location")
    descriptions = record.get("description")
    lang = record.get("language")
    keywords = record.get("keywords")
    report_number = record.get("report_number")

    if lang and lang != [metadata.get("language")]:
        # If it's same ignore
        raise UnexpectedValue(field="language", subfield="a", value=lang, stage="load")
    if report_number:
        if metadata.get("report_number") and report_number != metadata.get(
            "report_number"
        ):
            raise UnexpectedValue(
                field="report_number", subfield="a", value=report_number, stage="load"
            )
        else:
            metadata["report_number"] = report_number
    if descriptions:
        additional_descriptions = metadata.get("additional_descriptions", [])
        for description in descriptions:
            additional_descriptions.append(
                {
                    "description": description,
                    "type": "Other",
                    "lang": "en",
                }
            )
        metadata["additional_descriptions"] = additional_descriptions
    if keywords:
        keyword_objects = metadata.get("keywords", [])
        keyword_names = [keyword.get("name") for keyword in keyword_objects]
        for new_keyword_name in keywords:
            if new_keyword_name not in keyword_names:
                keyword_objects.append(
                    {
                        "name": new_keyword_name,
                    }
                )
        metadata["keywords"] = keyword_objects

    related_identifiers = list(metadata.get("related_identifiers", []))
    if event_id:
        # Insert event_id at the beginning
        related_identifiers.insert(
            0,
            {
                "scheme": "Indico",
                "identifier": str(event_id),
                "relation_type": "IsPartOf",
            },
        )
    if url:
        url = transform_legacy_urls(url, type="indico")
        url_identifier = {
            "scheme": "URL",
            "identifier": url,
            "relation_type": "IsPartOf",
        }
        if url_identifier not in related_identifiers:
            related_identifiers.append(url_identifier)

    metadata["related_identifiers"] = related_identifiers
    metadata["date"] = date
    if location:
        metadata["location"] = location

    return metadata
