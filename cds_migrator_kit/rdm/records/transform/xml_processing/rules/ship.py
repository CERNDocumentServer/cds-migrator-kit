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
    """Translates document type. TODO: can we ignore this?"""
    document_type = value.get("a").strip().lower()
    if document_type and document_type not in ["int", "conferencepaper", "note", ]:
        raise UnexpectedValue(f"Invalid document type: {document_type}")
    raise IgnoreKey("document_type")


@model.over("related_identifiers", "(^78502|^78002)")
@for_each_value
def related_works(self, key, value):
    """Translates related identifiers."""
    description = StringValue(value.get("i")).parse().strip().lower()
    recid = value.get("w")
    rel_ids = self.get("related_identifiers", [])
    if "superseded by" == description:
        relation_type = "isobsoletedby"
    elif "supersedes" == description:
        relation_type = "obsoletes"
    else:
        raise UnexpectedValue(f"Invalid relation type: {description}")
    new_id = {
        "identifier": recid,
        "scheme": "cds",
        "relation_type": {"id": relation_type},
        "resource_type": {"id": "other"},
    }
    if new_id not in rel_ids:
        return new_id

    raise IgnoreKey("related_identifiers")

