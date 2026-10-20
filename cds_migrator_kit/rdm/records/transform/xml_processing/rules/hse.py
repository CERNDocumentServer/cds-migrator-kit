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
from .hr import corpo_author
from .it import imprint

model.over("creators", "(^110__)")(corpo_author)
model.over("imprint_info", "(^250__)")(imprint)  # Only one record: 156663


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


@model.over("internal_notes", "^595__")
@for_each_value
def note_hse(self, key, value):
    """Translates notes. HSE Articles query is 595__:CERN-HSE."""
    _note = (value.get("a") or "").strip()
    if _note in ("CERN-HSE", "To be check by Salome"):
        raise IgnoreKey("internal_notes")
    else:
        return base_note(self, key, value)


def resolve_resource_type(values, priority):
    """Pick the resource type of the highest priority legacy value.

    ``priority`` maps legacy value -> RDM id, ordered first = highest
    priority. Values that are not listed are ignored. Returns ``None`` if no
    value is listed.
    """
    legacy = list(priority)
    candidates = [v for v in values if v in priority]
    if not candidates:
        return None
    return {"id": priority[min(candidates, key=legacy.index)]}


# first has highest priority
RESOURCE_TYPES = {
    "conferencepaper": "publication-conferencepaper",
    "article": "publication-article",
    "preprint": "publication-preprint",
    "report": "publication-report",
}


@model.over("resource_type", "^980__", override=True)
def resource_type(self, key, value):
    """Translates resource_type."""
    values = [(v.get("a") or "").strip().lower() for v in force_list(value)]
    resource_type = resolve_resource_type(values, RESOURCE_TYPES)
    if not resource_type:
        # HSE papers have no default resource type
        raise UnexpectedValue("Missing resource type (HSE)", field=key, value=value)
    return resource_type


@model.over("identifiers", "(^035__)|(^037__)|(^088__)|(^970__)", override=True)
@for_each_value
def identifiers(self, key, value):
    """Translates identifiers."""
    if key == "035__":
        identifier = value.get("a").strip()
        scheme = StringValue(value.get("9", "")).parse().lower()
        if scheme and scheme == "edms":
            new_id = {
                "identifier": identifier,
                "scheme": "edms",
                "relation_type": {"id": "isvariantformof"},
                "resource_type": {"id": "publication-other"},
            }
        else:
            new_id = base_identifiers(self, key, value)
        related_works = self.get("related_identifiers", [])
        if new_id not in related_works:
            related_works.append(new_id)
            self["related_identifiers"] = related_works
            raise IgnoreKey("identifiers")
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
    # TODO: can we ignore?
    if abstract_number and abstract_number not in ["JACoW", "submitter"]:
        raise UnexpectedValue(subfield="9", value=abstract_number, field=key)
    return base_description(self, key, value)
