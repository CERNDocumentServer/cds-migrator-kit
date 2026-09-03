# -*- coding: utf-8 -*-
#
# Copyright (C) 2022-2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""``metadata`` field mappers for CDS to RDM record transformation."""
from dateutil.parser import parse

from cds_migrator_kit.errors import MissingRequiredField, UnexpectedValue
from cds_migrator_kit.rdm.migration_config import RDM_RECORDS_IDENTIFIERS_SCHEMES
from cds_migrator_kit.rdm.records.transform.config import (
    IDENTIFIERS_SCHEMES_TO_DROP,
    IDENTIFIERS_VALUES_TO_DROP,
)
from cds_migrator_kit.rdm.records.transform.mappers.base import FieldMapper


class ResourceTypeMapper(FieldMapper):
    """Maps resource_type, requiring it to have been resolved upstream."""

    id = "resource_type"

    def map_value(self, ctx):
        """Return resource_type, dropping the upstream ranking scratch key."""
        dojson_entry = ctx.dojson_entry
        # `_resource_type_rank` is bookkeeping for the 980__/697C_
        # resource_type rule and research_committee.py's report-number
        # detection (see research.py:resource_type) - drop it before it
        # reaches the final record.
        dojson_entry.pop("_resource_type_rank", None)
        try:
            return dojson_entry["resource_type"]
        except KeyError:
            raise MissingRequiredField(message="resource_type", field="980")


class TitleMapper(FieldMapper):
    """Maps title, falling back to the meeting title for conference proceedings."""

    id = "title"

    def map_value(self, ctx):
        """Return title, or the 111__a meeting title as a fallback."""
        dojson_entry = ctx.dojson_entry
        title = dojson_entry.get("title")
        if title:
            return title
        # 245 (title) is sometimes absent on conference proceedings
        # records; fall back to the conference name (111__a) stored on
        # the first meeting entry.
        resource_type = ctx.metadata.get("resource_type") or {}
        if resource_type.get("id") == "publication-conferenceproceeding":
            meetings = dojson_entry.get("custom_fields", {}).get("meeting:meeting", [])
            for meeting_entry in meetings:
                meeting_title = meeting_entry.get("title")
                if meeting_title:
                    return meeting_title
        return title


def _date_precision(date_str):
    """Return how granular a normalized date string is (year=1, month=2, day=3)."""
    if not date_str:
        return 0
    return len(date_str.split("-"))


def _is_more_accurate(candidate, current):
    """Return True if `candidate` has finer granularity than `current`."""
    return _date_precision(candidate) > _date_precision(current)


class PublicationDateMapper(FieldMapper):
    """Maps publication_date, preferring 260 (article) or 269 (preprint).

    - resource_type == "publication-article": publication_date (260)
      always wins; preprint_date (269), if present, becomes a secondary
      "submitted"/"preprint" entry in `dates`.
    - any other resource_type: preprint_date (269) wins when present,
      unless publication_date (260) is also present and at least as
      accurate (day > month > year) - in that case publication_date wins
      instead. Whichever one loses, if present, becomes a secondary entry
      in `dates` ("available"/"published" for publication_date,
      "submitted"/"preprint" for preprint_date).

    Falls back to status week or file creation date when neither applies.
    """

    id = "publication_date"

    def map_value(self, ctx):
        """Return publication_date, requiring at least one date source."""
        dojson_entry = ctx.dojson_entry
        pub_date = dojson_entry.get("publication_date")
        # `preprint_date` is bookkeeping produced by the 269 rules (see
        # xml_processing/rules/base.py) - drop it before it reaches the
        # final record.
        preprint_date = dojson_entry.pop("preprint_date", None)
        resource_type = (dojson_entry.get("resource_type") or {}).get("id")

        if resource_type == "publication-article":
            if preprint_date:
                dojson_entry.setdefault("dates", []).append(
                    {
                        "date": preprint_date,
                        "type": {"id": "submitted"},
                        "description": "preprint",
                    }
                )
        elif preprint_date:
            if pub_date and not _is_more_accurate(preprint_date, pub_date):
                # publication_date is present and at least as accurate -
                # keep it, preprint_date becomes the secondary entry.
                dojson_entry.setdefault("dates", []).append(
                    {
                        "date": preprint_date,
                        "type": {"id": "submitted"},
                        "description": "preprint",
                    }
                )
            else:
                if pub_date:
                    dojson_entry.setdefault("dates", []).append(
                        {
                            "date": pub_date,
                            "type": {"id": "available"},
                            "description": "published",
                        }
                    )
                pub_date = preprint_date

        created = dojson_entry.get("status_week_date")
        files = ctx.raw_dump_entry["files"]
        if not (pub_date or created or files):
            raise MissingRequiredField(
                message="missing creation or publication date", field="916"
            )
        if not pub_date:
            if created:
                pub_date = dojson_entry["status_week_date"]
            elif not created and files:
                pub_date = parse(files[0]["creation_date"]).date().isoformat()
        return pub_date


class SubjectsMapper(FieldMapper):
    """Maps subjects, dropping placeholder "xx"/"talk" entries."""

    id = "subjects"

    def map_value(self, ctx):
        """Return the subjects list with placeholder entries removed."""
        subjects = ctx.dojson_entry.get("subjects")
        if subjects:
            for subject in reversed(subjects):
                if subject.get("subject", "").lower() in ["xx", "talk"]:
                    subjects.remove(subject)
                elif subject.get("id", "").lower() in ["xx", "talk"]:
                    subjects.remove(subject)
        return subjects


class TableOfContentsMapper(FieldMapper):
    """Folds table_of_content into additional_descriptions."""

    id = "additional_descriptions"

    def map_value(self, ctx):
        """Move table_of_content into additional_descriptions and return it."""
        dojson_entry = ctx.dojson_entry
        toc = dojson_entry.get("table_of_content", [])
        additional_desc = dojson_entry.get("additional_descriptions", [])
        if toc:
            additional_desc.append(
                {"description": toc, "type": {"id": "table-of-contents"}}
            )
            dojson_entry["additional_descriptions"] = additional_desc
            dojson_entry.pop("table_of_content")
        return dojson_entry.get("additional_descriptions")


class IdentifiersMapper(FieldMapper):
    """Maps identifiers, dropping unwanted schemes and validating the rest."""

    id = "identifiers"

    def map_value(self, ctx):
        """Return identifiers filtered/validated against known schemes."""
        identifiers = ctx.dojson_entry.get("identifiers", [])
        for item in reversed(identifiers):
            # drop unwanted schemes
            if item is None or "scheme" not in item:
                raise UnexpectedValue(
                    field="identifiers",
                    value=item,
                    subfield="9",
                    message="IDENTIFIER SCHEME MISSING",
                    priority="warning",
                    stage="transform",
                )
            if (
                item["scheme"].upper() in IDENTIFIERS_SCHEMES_TO_DROP
                or IDENTIFIERS_VALUES_TO_DROP in item["identifier"]
            ):
                identifiers.remove(item)
                continue
            if item["scheme"] not in RDM_RECORDS_IDENTIFIERS_SCHEMES.keys():
                raise UnexpectedValue(
                    field="identifiers",
                    subfield="9",
                    message="IDENTIFIER SCHEME INVALID",
                    priority="warning",
                    stage="transform",
                    value=item,
                )
        return identifiers


# Fields that pass through unchanged from dojson_entry - kept explicit in the
# composed list (mappers/config equivalent) rather than open-ended, so the
# "forgotten metadata key" completeness check in
# CDSToRDMRecordTransform._check_forgotten_keys() still catches any newly
# introduced dojson_entry key nobody has mapped yet.
PASSTHROUGH_METADATA_FIELDS = (
    "description",
    "publisher",
    "additional_titles",
    "languages",
    "dates",
    "funding",
    "related_identifiers",
    "rights",
    "copyright",
)
