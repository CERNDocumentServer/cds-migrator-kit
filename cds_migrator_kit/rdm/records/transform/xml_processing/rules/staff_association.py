# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM Staff Association rules."""

from dojson.errors import IgnoreKey

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.rdm.records.transform.xml_processing.rules.base import (
    additional_titles,
)
from cds_migrator_kit.transform.xml_processing.quality.decorators import for_each_value
from cds_migrator_kit.transform.xml_processing.rules.base import (
    record_submitter as base_submitter,
)

from ...models.staff_association import staff_association_model as model
from .bulletin_issue import collection
from .publications import internal_notes

model.over("internal_notes", "^562__")(internal_notes)
model.over("additional_titles", "(^242__)")(additional_titles)


@model.over("resource_type", "^980__", override=True)
def resource_type(self, key, value):
    """Translates resource_type."""
    value = value.get("a") if "a" in value else value.get("b")
    if value:
        value = value.lower()
    if value in ["bulletinstaff", "staffassociation"]:
        return {"id": "publication-periodicalarticle"}
    raise UnexpectedValue(
        "Unknown resource type (STAFF ASSOCIATION)", field=key, value=value
    )


@model.over("collection", "^690C_", override=True)
@for_each_value
def staff_association_collection(self, key, value):
    """Translates collection field."""
    collection_a = value.get("a", "").strip().lower()
    # Drop sa documents
    if collection_a == "sa documents":
        raise IgnoreKey("collection")
    collection(self, key, value)


# Known 859__a values that are staff association / bulletin names or typos.
_IGNORED_STAFF_ASSOCIATION_SUBMITTERS = {
    "",
    " Staff.Bulletin@cern.ch",
    "Association du personnel",
    "Mutual Aid Fund",
    "STAFF ASSOCIATION",
    "Saff.Bulletin@cern.ch",
    "Satff.Bulletin@cern.ch",
    "Satff.bulletin@cern.ch",
    "Staff Association",
    "Staff. Bulletin@cern.ch",
    "Staff. bulletin@cern.ch",
    "Staff.Asscociation@cern.ch",
    "Staff.Association@cern.ch",
    "Staff.Bulletin-editors@cern.ch",
    "Staff.Bulletin@cern.ch",
    "Staff.Kindergarten@cern.ch",
    "Staff.association@cern.ch",
    "Staff.bulletin@cern.ch",
    "Staff.bulletins@cern.ch",
    "Staff:Bulletin@cern.ch",
    "bulletin-editors@cern.ch",
    "cern.bulletin@cern.ch",
    "staff-bulletin@cern.ch",
    "staff.asociation@cern.ch",
    "staff.association",
    "staff.association@cern.ch",
    "staff.bullelin@cern.ch",
    "staff.bulletin@Cern.ch",
    "staff.bulletin@cern.",
    "staff.bulletin@cern.ch",
    "staff.bulletin@ern.ch",
    "staff.bulletins@cern.ch",
    "staff.buttetin@cern.ch",
    "statt.bulletin@cern.ch",
    "stff.bulletin@cern.ch",
}


@model.over("submitter", "(^859__)", override=True)
def staff_contact_person(self, key, value):
    """Translates contact person field from submitters tag to populate additional descriptions field."""
    contact_person = value.get("a", "")
    if contact_person and contact_person not in _IGNORED_STAFF_ASSOCIATION_SUBMITTERS:
        self.setdefault(
            "additional_descriptions", []
        )  # In case the field already exists, don't overwrite it
        self["additional_descriptions"].append(
            {
                "description": f"<p>Contact: {contact_person}</p>",
                "type": {
                    "id": "other",
                },
            }
        )
    submitter = base_submitter(self, key, value)
    return submitter
