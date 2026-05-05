# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM ECO rules."""

import re

from dojson.utils import IgnoreKey, for_each_value, force_list

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.rdm.records.transform.xml_processing.rules.it import (
    corporate_author,
)
from cds_migrator_kit.transform.xml_processing.quality.decorators import (
    require,
)
from cds_migrator_kit.transform.xml_processing.quality.parsers import (
    StringValue,
)
from cds_migrator_kit.transform.xml_processing.rules.base import (
    languages as base_languages,
)

from ...models.eco import eco_model as model
from .base import identifiers
from .base import note as base_note
from .base import report_number, urls
from .bulletin_issue import (
    additional_descriptions,
    additional_titles_bulletin,
    rel_identifiers,
    translated_description,
    urls_bulletin,
)
from .it import corporate_author
from .publications import internal_notes, journal, organisation, related_identifiers

model.over("additional_titles", "(^246_[1_])", override=True)(
    additional_titles_bulletin
)
model.over("additional_descriptions", "(^500__)")(additional_descriptions)
model.over("additional_descriptions", "(^590__)")(translated_description)
model.over("internal_notes", "^562__")(internal_notes)
model.over("contributors", "^901__")(organisation)
model.over("creators", "(^110__)")(corporate_author)
model.over("eco_urls", "^8564[1_]", override=True)(urls_bulletin)


@model.over("internal_notes", "^595__")
@for_each_value
def internal_notes(self, key, value):
    """Translates internal notes."""
    subject_notes = force_list(value.get("s", ""))
    if subject_notes:
        # add them as subjects
        subjects = self.get("subjects", [])
        for note in subject_notes:
            subjects.append({"subject": note})
        self["subjects"] = subjects
    base_note(self, key, value)
    raise IgnoreKey("internal_notes")


@model.over("eco_report_number", "(^037__)|(^088__)", override=True)
@for_each_value
def eco_report_number(self, key, value):
    """Translates report number."""
    identifier = value.get("a", "")
    # Check it's email TODO: how to handle?
    if key == "088__" and "@" in identifier:
        pass
    else:
        _identifier = report_number(self, key, value)
        identifiers = self.get("identifiers", [])

        if _identifier and _identifier not in identifiers:
            identifiers += _identifier
        self["identifiers"] = identifiers
    raise IgnoreKey("eco_report_number")


@model.over("eco_related_identifiers", "(^962__)", override=True)
@for_each_value
def eco_related_identifiers(self, key, value):
    """Translates related identifiers."""
    scheme = value.get("l", "")
    if scheme:
        rel_identifiers(self, key, value)
        raise IgnoreKey("eco_related_identifiers")
    rel_identifier = related_identifiers(self, key, value)
    if rel_identifier:
        rel_id = rel_identifier[0]
        rel_ids = self.get("related_identifiers", [])
        if rel_id not in rel_ids:
            rel_ids.append(rel_id)
            self["related_identifiers"] = rel_ids
    raise IgnoreKey("eco_related_identifiers")


@model.over("eco_identifiers", "^035__", override=True)
@for_each_value
def eco_identifiers(self, key, value):
    """Translates identifiers."""
    original_scheme = StringValue(value.get("9", "")).parse()
    scheme = original_scheme.lower()

    # TODO: handle photo identifier
    if scheme == "phopho":
        id_value = StringValue(value.get("a", "")).parse()
        new_id = {"scheme": "photo", "identifier": id_value}
        raise IgnoreKey("eco_identifiers")
    identifiers(self, key, value)
    raise IgnoreKey("eco_identifiers")


@model.over("resource_type", "^980__", override=True)
def resource_type(self, key, value):
    """Translates resource_type."""
    value = value.get("a") if "a" in value else value.get("b")
    if value:
        value = value.strip().lower()

    mapping = {
        "poster": {"id": "poster"},
        "brochure": {"id": "publication-brochure"},
        "note": {"id": "publication-technicalnote"},
        "conferencepaper": {"id": "publication-conferencepaper"},
    }

    try:
        return mapping[value]
    except KeyError:
        raise UnexpectedValue("Unknown resource type (ECO)", field=key, value=value)


@model.over("collection", "^690C_", override=True)
@for_each_value
def collection(self, key, value):
    """Translates collection."""
    collection = value.get("a", "")
    if collection.strip().upper() == "CERN":
        raise IgnoreKey("collection")
    if collection.strip().upper() not in [
        "POSTER",
        "PREPRINT",
    ]:
        raise UnexpectedValue(subfield="a", field=key, value=value)
    subjects = self.get("subjects", [])
    subjects.append(
        {
            "subject": f"collection:{collection}",
        }
    )
    self["subjects"] = subjects
    raise IgnoreKey("collection")


@model.over("related_ids", "^773__")
@for_each_value
def related_ids(self, key, value):
    """Translated related links."""
    related_link = value.get("u", "")
    if not related_link:
        _custom_fields = journal(self, key, value)
        self["custom_fields"] = _custom_fields
        raise IgnoreKey("related_ids")

    # Transform like the base `urls` rule
    rel_ids = urls(self, key, value)
    if not rel_ids:
        raise IgnoreKey("related_ids")
    rel_id = rel_ids[0]
    related_identifiers = self.get("related_identifiers", [])
    if rel_id not in related_identifiers:
        related_identifiers.append(rel_id)
    self["related_identifiers"] = related_identifiers

    raise IgnoreKey("related_ids")


@model.over("submitter_info", "^923__")
@for_each_value
def submitter_info(self, key, value):
    """Translates submitter information."""
    submitter_info = value.get("r", "")
    names = submitter_info.strip().split(",")

    if len(names) == 2:
        names = {"family_name": names[0].strip(), "given_name": names[1].strip()}
    else:
        names = {"family_name": names[0].strip()}
    contributor = {
        "person_or_org": {
            "type": "personal",
            **names,
        },
        "role": {"id": "contactperson"},
    }
    contributors = self.get("contributors", [])
    contributors.append(contributor)
    self["contributors"] = contributors
    raise IgnoreKey("submitter_info")


@model.over("languages", "^041__", override=True)
@for_each_value
@require(["a"])
def language(self, key, value):
    """Translates languages fields."""
    langs = value.get("a")
    languages = self.get("languages", [])
    if "-" in langs or "/" in langs:
        # https://cds.cern.ch/record/921930/export/xm
        language_codes = re.split(r"[-/]+", langs)
        for lang in language_codes:
            if not lang:
                continue
            new_langs = base_languages(self, key, {"a": lang})
            languages.extend(new_langs)
    else:
        new_langs = base_languages(self, key, value)
        languages.extend(new_langs)
    self["languages"] = languages
    raise IgnoreKey("language")


@model.over("field_993", "^993__", override=True)
@for_each_value
def field_993(self, key, value):
    """Translates field 993 as a keyword."""
    value = value.get("q", "")
    if value and value not in ["Project Management"]:
        raise UnexpectedValue(field=key, subfield="a", value=value)
    _subjects = self.get("subjects", [])
    subject = {
        "subject": value,
    }
    _subjects.append(subject)
    self["subjects"] = _subjects
    raise IgnoreKey("field_993")
