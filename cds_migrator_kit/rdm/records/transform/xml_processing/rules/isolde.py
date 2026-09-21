# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM ISOLDE migration rules."""

from dojson.errors import IgnoreKey

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.decorators import for_each_value
from cds_migrator_kit.transform.xml_processing.quality.parsers import StringValue

from ...models.isolde import isolde_model as model
from .base import identifiers as _base_identifiers


@model.over("identifiers", "^035__", override_tag=True)
@for_each_value
def identifiers(self, key, value):
    """Translates 035__ identifiers.

    - scheme empty or 'CERN ISOLDE': stored as related_identifier with scheme
      'other' and identifier pattern 'ISOLDE:<a>', relation 'references'.
    - everything else (incl. CERCER → aleph): delegated to the base rule.
    """
    scheme = value.get("9", "").strip()
    system_control_number = value.get("a", "").strip()

    if not scheme or scheme.upper() == "CERN ISOLDE":
        related_identifiers = self.get("related_identifiers", [])
        new_id = {
            "identifier": f"ISOLDE:{system_control_number}",
            "scheme": "other",
            "relation_type": {"id": "references"},
        }
        if new_id not in related_identifiers:
            related_identifiers.append(new_id)
            self["related_identifiers"] = related_identifiers
        raise IgnoreKey("identifiers")

    return _base_identifiers.__wrapped__(self, key, value)


@model.over("medium", "^340__")
@for_each_value
def medium(self, key, value):
    """Ignores 340__a when value is 'paper', raises for anything else."""
    a_value = value.get("a", "").strip().lower()
    if a_value == "paper":
        raise IgnoreKey("medium")
    raise UnexpectedValue(field=key, subfield="a", value=value, stage="transform")


@model.over("additional_descriptions", "^852__")
@for_each_value
def holdings(self, key, value):
    """852 holdings: selectively ignores known location values.

    - 852__c == 'CERN ARC Library': ignored.
    - 852__h (depot code): ignored; records will be bulk-updated post-migration
      once the ATOM/CLC target mapping is established.
    - anything else: raises UnexpectedValue to surface unknown cases.
    """
    h_value = StringValue(value.get("h", "")).parse()
    c_value = StringValue(value.get("c", "")).parse()

    # Depot location: check the value exists, then ignore.
    # Records will be bulk-updated post-migration via ATOM/CLC mapping.
    if h_value:
        raise IgnoreKey("additional_descriptions")

    if c_value and c_value.lower() == "cern arc library":
        raise IgnoreKey("additional_descriptions")

    if h_value and "cern depot" in h_value.lower():
        raise IgnoreKey("additional_descriptions")

    if c_value:
        raise UnexpectedValue(field=key, subfield="c", value=value, stage="transform")

    if h_value:
        raise UnexpectedValue(field=key, subfield="h", value=value, stage="transform")

    raise IgnoreKey("additional_descriptions")
