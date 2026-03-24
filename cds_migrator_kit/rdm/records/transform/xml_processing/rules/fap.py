# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM FAP (Finance and Administrative Processes) rules."""

from dojson.errors import IgnoreKey
from dojson.utils import for_each_value

from cds_migrator_kit.errors import UnexpectedValue

from ...models.fap import fap_model as model


@model.over("collection", "^690C_")
@for_each_value
def collection(self, key, value):
    """Translates document type field."""
    collection = value.get("a").strip().lower()
    # TODO: can we drop them?
    if collection not in ["cern", "intnote", "publfap"]:
        raise UnexpectedValue(subfield="a", value=value, field=key)
    raise IgnoreKey("collection")


@model.over("resource_type", "^980__", override=True)
def resource_type(self, key, value):
    """Translates resource_type."""
    value = value.get("a")
    if value:
        value = value.strip().upper()
    if value != "INTNOTEFAPPUBL":
        raise UnexpectedValue("Unknown resource type (FAP)", field=key, value=value)
    raise IgnoreKey("resource_type")
