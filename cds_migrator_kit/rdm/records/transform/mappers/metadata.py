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
        json_entry = ctx.json_entry
        # `_resource_type_rank` is bookkeeping for the 980__/697C_
        # resource_type rule and research_committee.py's report-number
        # detection (see research.py:resource_type) - drop it before it
        # reaches the final record.
        json_entry.pop("_resource_type_rank", None)
        try:
            return json_entry["resource_type"]
        except KeyError:
            raise MissingRequiredField(message="resource_type", field="980")


class TitleMapper(FieldMapper):
    """Maps title, falling back to the meeting title for conference proceedings."""

    id = "title"

    def map_value(self, ctx):
        """Return title, or the 111__a meeting title as a fallback."""
        json_entry = ctx.json_entry
        title = json_entry.get("title")
        if title:
            return title
        # 245 (title) is sometimes absent on conference proceedings
        # records; fall back to the conference name (111__a) stored on
        # the first meeting entry.
        resource_type = ctx.metadata.get("resource_type") or {}
        if resource_type.get("id") == "publication-conferenceproceeding":
            meetings = json_entry.get("custom_fields", {}).get("meeting:meeting", [])
            for meeting_entry in meetings:
                meeting_title = meeting_entry.get("title")
                if meeting_title:
                    return meeting_title
        return title


class PublicationDateMapper(FieldMapper):
    """Maps publication_date, falling back to status week or file creation date."""

    id = "publication_date"

    def map_value(self, ctx):
        """Return publication_date, requiring at least one date source."""
        json_entry = ctx.json_entry
        pub_date = json_entry.get("publication_date")
        created = json_entry.get("status_week_date")
        files = ctx.entry["files"]
        if not (pub_date or created or files):
            raise MissingRequiredField(
                message="missing creation or publication date", field="916"
            )
        if not pub_date:
            if created:
                pub_date = json_entry["status_week_date"]
            elif not created and files:
                pub_date = parse(files[0]["creation_date"]).date().isoformat()
        return pub_date


class SubjectsMapper(FieldMapper):
    """Maps subjects, dropping placeholder "xx"/"talk" entries."""

    id = "subjects"

    def map_value(self, ctx):
        """Return the subjects list with placeholder entries removed."""
        subjects = ctx.json_entry.get("subjects")
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
        json_entry = ctx.json_entry
        toc = json_entry.get("table_of_content", [])
        additional_desc = json_entry.get("additional_descriptions", [])
        if toc:
            additional_desc.append(
                {"description": toc, "type": {"id": "table-of-contents"}}
            )
            json_entry["additional_descriptions"] = additional_desc
            json_entry.pop("table_of_content")
        return json_entry.get("additional_descriptions")


class IdentifiersMapper(FieldMapper):
    """Maps identifiers, dropping unwanted schemes and validating the rest."""

    id = "identifiers"

    def map_value(self, ctx):
        """Return identifiers filtered/validated against known schemes."""
        identifiers = ctx.json_entry.get("identifiers", [])
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


# Fields that pass through unchanged from json_entry - kept explicit in the
# composed list (mappers/config equivalent) rather than open-ended, so the
# "forgotten metadata key" completeness check in RecordEntry._metadata
# still catches any newly introduced json_entry key nobody has mapped yet.
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
