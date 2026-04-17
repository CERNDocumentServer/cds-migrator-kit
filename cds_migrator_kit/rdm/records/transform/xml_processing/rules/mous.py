# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM MoUs rules."""

import re
from datetime import datetime

from dojson.errors import IgnoreKey
from dojson.utils import for_each_value

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.parsers import StringValue

from ...models.mous import mous_model as model
from .it import corporate_author

model.over("creators", "^110__")(corporate_author)


@model.over("collection", "^690C_")
@for_each_value
def collection(self, key, value):
    """Translates document type field."""
    collection = value.get("a").strip().lower()
    # TODO: what they mean? how can we use them?
    if collection not in [
        "signatory",
        "addendum",
        "amendment",
        "initial",
        "letter",
        "tracking list",
        "signatory page",
        "select:",
        "amendment to mou",
        "initial mou",
        "mou information",
        "information",
    ]:
        raise UnexpectedValue(subfield="a", value=value, field=key)
    raise IgnoreKey("collection")


@model.over("dates", "^925__")
@for_each_value
def dates(self, key, value):
    """Translates dates field."""

    def parse_date(date_str: str) -> str:
        date_str = date_str.strip()
        try:
            dt = datetime.fromisoformat(date_str)
            return dt.date().isoformat()
        except ValueError:
            pass

        for fmt in ("%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y", "%Y-%m-%d", "%d.%m.%Y"):
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.date().isoformat()
            except ValueError:
                continue

        if re.fullmatch(r"\d{4}", date_str):
            return date_str

        if re.fullmatch(r"\d{4}-\d{2}", date_str):
            return date_str

        raise UnexpectedValue(
            f"Unsupported date format: {date_str}", field=key, value=value
        )

    dates = self.get("dates", [])
    creation_date = value.get("a")
    if creation_date:
        creation_date = parse_date(creation_date)
        dates.append({"date": creation_date, "type": {"id": "created"}})

    signature_date = value.get("b")
    if signature_date:
        signature_date = parse_date(signature_date)
        # TODO:What is the type for signature date?
        dates.append({"date": signature_date, "type": {"id": "issued"}})
    if not creation_date and not signature_date:
        raise UnexpectedValue(subfield="a", value=value, field=key)
    self["dates"] = dates
    raise IgnoreKey("dates")


@model.over("parent_mou", "(^773__)")
def parent_mou(self, key, value):
    """Translates parent MOU field."""
    parent__mou_recid = value.get("w", "").strip()
    parent_mou_report_number = value.get("r", "").strip()
    if not parent__mou_recid:
        raise UnexpectedValue(subfield="r", value=value, field=key)
    rel_ids = self.get("related_identifiers", [])
    # TODO: we can add this as a related identifier?
    if parent_mou_report_number:
        new_id = {
            "scheme": "cdsrn",
            "identifier": parent_mou_report_number,
            "relation_type": {"id": "ispartof"},
            "resource_type": {"id": "publication-other"},
        }
        if new_id not in rel_ids:
            rel_ids.append(new_id)
    if parent__mou_recid:
        parent_mou_recid = {
            "scheme": "cds",
            "identifier": parent__mou_recid,
            "relation_type": {"id": "ispartof"},
            "resource_type": {"id": "publication-other"},
        }
        if parent_mou_recid not in rel_ids:
            rel_ids.append(parent_mou_recid)
    self["related_identifiers"] = rel_ids
    raise IgnoreKey("parent_mou")


@model.over("funding", "(^536__)")
def funding(self, key, value):
    """Translates funding field."""
    # TODO: how can transform we have only a subfield?
    programme = value.get("a")
    # if programme:
    #     raise UnexpectedValue("Unexpected programme value", field=key, value=value)
    raise IgnoreKey("funding")


@model.over("official_reference", "^036__")
@for_each_value
def official_reference(self, key, value):
    """Translates official reference fields."""
    # TODO: example record: https://cds.cern.ch/record/2774726/export/xm
    identifier = value.get("a", "").strip()
    existing_ids = self.get("identifiers", [])

    n_value = value.get("n", "").strip().lower()
    if n_value and n_value != "internal reference":
        raise UnexpectedValue(field=key, value=value, subfield="n")

    if not identifier:
        raise UnexpectedValue(subfield="a", value=value, field=key)
    new_id = {"scheme": "cdsrn", "identifier": identifier}
    if new_id not in existing_ids:
        existing_ids.append(new_id)
        self["identifiers"] = existing_ids
    raise IgnoreKey("identifiers")


@model.over("identifiers", "(^037__)|(^970__)", override=True)
@for_each_value
def identifiers(self, key, value):
    """Translates identifiers."""
    if key == "037__":
        identifier = value.get("a", "").strip()
        existing_ids = self.get("identifiers", [])

        n_value = value.get("n", "").strip().lower()
        if n_value and n_value not in ["internal reference", "resources review boards"]:
            raise UnexpectedValue(field=key, value=value, subfield="n")
        original_scheme = StringValue(value.get("9", "")).parse()
        scheme = original_scheme.lower()
        if scheme:
            raise UnexpectedValue(field=key, value=value, subfield="9")
        if not identifier:
            raise UnexpectedValue(subfield="a", value=value, field=key)
        if identifier in existing_ids:
            return {"scheme": "cdsrn", "identifier": identifier}

    elif key == "970__":
        value_9 = value.get("9", "").strip().lower()
        identifier = value.get("a", "").strip()
        if value_9:
            if value_9 not in ["cern sharepoint"]:
                raise UnexpectedValue(field=key, value=value, subfield="9")
            else:
                # TODO: how can we use this? it's sharepoint id?
                new_id = {"scheme": "cdsrn", "identifier": identifier}
        else:
            raise UnexpectedValue(field=key, value=value, subfield="9")
    raise IgnoreKey("identifiers")
