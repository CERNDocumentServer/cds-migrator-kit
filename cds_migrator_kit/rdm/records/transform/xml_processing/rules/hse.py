# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM HSE rules."""

from dojson.errors import IgnoreKey
from dojson.utils import filter_values, force_list

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.decorators import (
    for_each_value,
)
from cds_migrator_kit.transform.xml_processing.quality.parsers import StringValue
from cds_migrator_kit.transform.xml_processing.rules.base import (
    description as base_description,
)

from ...models.hse import hse_model as model
from .base import aleph_number as base_aleph_number
from .base import copyrights as base_copyright
from .base import identifiers as base_identifiers
from .base import licenses as base_licenses
from .base import note as base_note
from .base import report_number as base_report_number
from .base import title as base_title
from .hr import corpo_author
from .it import imprint

model.over("creators", "(^110__)")(corpo_author)
model.over("imprint_info", "(^250__)")(imprint)  # Only one record: 156663


@model.over("collection", "^690C_", override=True)
@for_each_value
def collection(self, key, value):
    """Translates collection field."""
    collection = value.get("a").strip().lower()
    if collection not in ["cern", "preprint", "article", "report"]:
        raise UnexpectedValue(subfield="a", value=value, field=key)
    raise IgnoreKey("collection")


@model.over("physical_copies", "^964__", override=True)
@for_each_value
def physical_copies(self, key, value):
    """Translates physical copies field."""
    number_of_copies = value.get("a").strip()
    if number_of_copies and number_of_copies not in [
        "0002",
        "0001",
        "0004",
        "0003",
        "0005",
    ]:
        raise UnexpectedValue(subfield="a", value=number_of_copies, field=key)
    raise IgnoreKey("physical_copies")


@model.over("physical_medium", "^340__", override=True)
@for_each_value
def physical_medium(self, key, value):
    """Translates physical medium field."""
    physical_medium = value.get("a").strip()
    if physical_medium and physical_medium not in ["paper"]:
        raise UnexpectedValue(subfield="a", value=physical_medium, field=key)
    raise IgnoreKey("physical_medium")


@model.over("access_note", "^506__", override=True)
@for_each_value
def access_note(self, key, value):
    """Translates physical medium field."""
    access_note = value.get("a").strip()
    if access_note and access_note.lower() not in ["free", "restricted"]:
        raise UnexpectedValue(subfield="a", value=access_note, field=key)
    raise IgnoreKey("access_note")


@model.over("internal_notes", "^595__")
@for_each_value
def note_hse(self, key, value):
    """Translates notes. HSE Articles query is 595__:CERN-HSE."""
    _note = force_list(value.get("a", ""))
    if _note == ("CERN-HSE",):
        self["resource_type"] = {"id": "publication-article"}
        raise IgnoreKey("internal_notes")
    else:
        return base_note(self, key, value)


@model.over("resource_type", "^980__", override=True)
def resource_type(self, key, value):
    """Translates resource_type."""
    value = value.get("a")
    if value:
        value = value.strip().lower()
    if value in ["rp_restricted"]:
        raise IgnoreKey("resource_type")
    # TODO: what if more than one resource type exists?
    map = {
        "conferencepaper": {"id": "publication-conferencepaper"},
        "article": {"id": "publication-article"},
        "preprint": {"id": "publication-preprint"},
        "report": {"id": "publication-report"},
    }
    try:
        return map[value]
    except KeyError:
        raise UnexpectedValue("Unknown resource type (HSE)", field=key, value=value)


@model.over("identifiers", "(^035__)|(^037__)|(^088__)|(^970__)", override=True)
@for_each_value
def identifiers(self, key, value):
    """Translates identifiers."""
    if key == "035__":
        identifier = value.get("a").strip()
        scheme = StringValue(value.get("9", "")).parse().lower()
        if scheme and scheme == "edms":
            new_id = {
                # TODO: for edms do we need scheme type or we can use url?
                "identifier": identifier,
                "scheme": "edms",
                # TODO: check relation and resource type
            }
            return new_id
        else:
            new_id = base_identifiers(self, key, value)
    elif key in ("037__", "088__"):
        # TODO: two records have 'b' value: 2041604, 2040156
        new_id = base_report_number(self, key, value)
    elif key == "970__":
        new_id = base_aleph_number(self, key, value)
    if new_id:
        return new_id[0]
    raise IgnoreKey("identifiers")


@model.over("rights", "^540__", override=True)
@for_each_value
@filter_values
def licenses(self, key, value):
    """Translates rights."""
    material = value.get("3")
    if material and material.lower() not in ["publication"]:
        raise UnexpectedValue(subfield="3", value=material, field=key)
    license = base_licenses(self, key, value)
    return license[0]


@model.over("copyright", "^542__", override=True)
def copyrights(self, key, value):
    """Translates copyright."""
    material = value.get("3")
    if material and material.lower() not in ["publication"]:
        raise UnexpectedValue(subfield="3", value=material, field=key)
    return base_copyright(self, key, value)


@model.over("description", "^520__", override=True)
def description(self, key, value):
    """Translates description."""
    abstract_number = value.get("9")
    if abstract_number and abstract_number not in ["JACoW", "submitter"]:
        raise UnexpectedValue(subfield="9", value=abstract_number, field=key)
    return base_description(self, key, value)


@model.over("title", "^245__", override=True)
def title(self, key, value):
    """Translates title."""
    value_9 = value.get("9")
    if value_9 and value_9 not in ["submitter"]:
        raise UnexpectedValue(subfield="9", value=value_9, field=key)
    return base_title(self, key, value)
