# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM Radio Protection rules."""

from ...models.radio_protection import radio_protection_model as model
from .research import resource_type as research_resource_type

# The rules (including the 980__/697C_ resource type) are inherited from the HSE
# model, only the model defaults differ (resource_type: publication-preprint).

# Radio Protection also has the document type in the 490__a series. Clean the
# value to the legacy document types known by the generic rule, it shares the
# rank with 980__ so the highest priority value wins. Replaces the inherited
# series rule.
SERIES_TO_DOCUMENT_TYPE = {
    "internal report": "report",
    "rp annual report": "report",
    "divisionnal report": "report",
    "technical inspection": "report",
    "technical notes": "technical note",
    "technical memorandum": "technical note",
}


@model.over("resource_type", "^490__", override_tag=True)
def series_resource_type(self, key, value):
    """Translates resource_type from the 490__a series."""
    series = (value.get("a") or "").strip().lower()
    cleaned = {"a": SERIES_TO_DOCUMENT_TYPE.get(series, series)}
    return research_resource_type(self, key, cleaned)
