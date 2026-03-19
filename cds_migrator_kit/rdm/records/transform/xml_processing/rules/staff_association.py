# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM Staff Association rules."""

from cds_migrator_kit.errors import UnexpectedValue

from ...models.staff_association import staff_association_model as model
from .bulletin_issue import (
    additional_descriptions,
    additional_titles_bulletin,
    bull_related_identifiers,
    bulletin_report_number,
    collection,
    creators,
    custom_fields_journal,
    description,
    imprint_info,
    issue_number,
    journal,
    rel_identifiers,
    subjects_bulletin,
    translated_description,
    urls_bulletin,
    urls_bulletin_bis,
)

# Re-register all shared bulletin rules onto staff_association_model
model.over("creators", "^100__", override=True)(creators)
model.over("additional_titles", "(^246_[1_])", override=True)(
    additional_titles_bulletin
)
model.over("description", "^520__", override=True)(description)
model.over("collection", "^690C_", override=True)(collection)
model.over("publication_date", "(^260__)", override=True)(imprint_info)
model.over("custom_fields", "(^773__)")(journal)
model.over("additional_descriptions", "(^500__)")(additional_descriptions)
model.over("additional_descriptions", "(^590__)")(translated_description)
model.over("subjects", "(^650[12_][7_])|(^6531_)", override=True)(subjects_bulletin)
model.over("url_identifiers", "^8564_", override=True)(urls_bulletin)
model.over("urls_bulletin", "^856__")(urls_bulletin_bis)
model.over("custom_fields_journal", "(^916__)", override=True)(custom_fields_journal)
model.over("bulletin_report_number", "(^037__)|(^088__)", override=True)(
    bulletin_report_number
)
model.over("custom_fields", "(^925__)")(issue_number)
model.over("bull_related_identifiers_1", "(^941__)")(bull_related_identifiers)
model.over("bull_related_identifiers_2", "(^962__)", override=True)(rel_identifiers)


@model.over("resource_type", "^980__", override=True)
def resource_type(self, key, value):
    """Translates resource_type."""
    value = value.get("a") if "a" in value else value.get("b")
    if value:
        value = value.lower()
    if value == "bulletinstaff":
        # TODO what is the resource type?
        return {"id": "publication-periodicalarticle"}
    raise UnexpectedValue(
        "Unknown resource type (STAFF ASSOCIATION)", field=key, value=value
    )
