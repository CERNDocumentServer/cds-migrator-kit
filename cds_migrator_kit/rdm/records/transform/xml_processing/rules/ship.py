# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM SHIP research rules."""

from dojson.errors import IgnoreKey
from dojson.utils import for_each_value

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.parsers import StringValue

from ...models.ship import ship_research_model as model
from .faser_publication import spokesperson

model.over("request_reviewers", "(^905__|^906__)", override=True)(spokesperson)


@model.over("document_type", "^594__")
def document_type(self, key, value):
    """Confirmed with SIS, we can ignore this since the record already has a resource type."""
    document_type = value.get("a").strip().lower()
    if document_type and document_type not in [
        "int",
        "conferencepaper",
        "note",
    ]:
        raise UnexpectedValue(f"Invalid document type: {document_type}")
    raise IgnoreKey("document_type")
