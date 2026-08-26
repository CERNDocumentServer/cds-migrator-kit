# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.
#
from dojson.errors import IgnoreKey

from cds_migrator_kit.transform.xml_processing.quality.decorators import (
    for_each_value,
    require,
)
from cds_migrator_kit.transform.xml_processing.quality.parsers import StringValue

from ...models.at import at_model as model


@model.over("contributors", "^541__")
@for_each_value
def contact_person(self, key, value):
    contact_person = value.get("a", None)
    if contact_person is None:
        raise IgnoreKey("contributors")

    contact_person = StringValue(contact_person).parse()

    # Sometimes the contact person is stored as "<Name>, <Role>" where the role is some position within the project.
    parts = contact_person.split(",")
    if len(parts) == 2:
        name, role = parts
        contact_person = name.strip()

    contributor = {
        "person_or_org": {
            "type": "personal",
            "name": contact_person,
            "family_name": contact_person,
        },
        "role": {"id": "contactperson"},
    }
    return contributor
