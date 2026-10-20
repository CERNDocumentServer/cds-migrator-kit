# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM HSE rules."""

from dojson.errors import IgnoreKey
# from dojson.utils import filter_values, force_list

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.decorators import (
    for_each_value,
)
# from cds_migrator_kit.transform.xml_processing.rules.base import (
#     description as base_description,
# )

from ...models.hse import hse_model as model
# from .base import copyrights as base_copyright
# from .base import licenses as base_licenses
from .base import note as base_note
from .hr import corpo_author
from .it import imprint
from .research import resource_type as research_resource_type

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


@model.over("internal_notes", "^595__")
@for_each_value
def note_hse(self, key, value):
    """Translates notes. HSE Articles query is 595__:CERN-HSE."""
    _note = (value.get("a") or "").strip()
    if _note in ("CERN-HSE", "To be check by Salome"):
        raise IgnoreKey("internal_notes")
    else:
        return base_note(self, key, value)


# Reuse the generic 980__/697C_ resource type rule. HSE papers have no default,
# a record without a match is rejected by ``ResourceTypeMapper``.
model.over("resource_type", "(^980__)|(^697C_)", override=True)(research_resource_type)


# @model.over("rights", "^540__", override=True)
# @for_each_value
# @filter_values
# def licenses(self, key, value):
#     """Translates rights."""
#     material = value.get("3")
#     if material and material.lower() not in ["publication"]:
#         raise UnexpectedValue(subfield="3", value=material, field=key)
#     license = base_licenses(self, key, value)
#     return license[0]


# @model.over("copyright", "^542__", override=True)
# def copyrights(self, key, value):
#     """Translates copyright."""
#     material = value.get("3")
#     if material and material.lower() not in ["publication"]:
#         raise UnexpectedValue(subfield="3", value=material, field=key)
#     return base_copyright(self, key, value)
# @model.over("description", "^520__", override=True)
# def description(self, key, value):
#     """Translates description."""
#     abstract_number = value.get("9")
#     # TODO: can we ignore?
#     if abstract_number and abstract_number not in ["JACoW", "submitter"]:
#         raise UnexpectedValue(subfield="9", value=abstract_number, field=key)
#     return base_description(self, key, value)
