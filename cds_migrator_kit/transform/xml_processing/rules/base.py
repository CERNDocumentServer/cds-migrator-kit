# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM migration rules module."""

import datetime

import pycountry
from cds_dojson.marc21.fields.utils import out_strip
from dateutil.parser import parse
from dateutil.parser._parser import ParserError
from dojson.errors import IgnoreKey
from dojson.utils import filter_values, flatten, force_list

from ....transform.xml_processing.models.base import model
from cds_migrator_kit.errors import UnexpectedValue
from ..quality.contributors import (
    extract_json_contributor_ids,
    get_contributor_affiliations,
    get_contributor_role,
)
from ..quality.decorators import (
    filter_list_values,
    for_each_value,
    require,
    strip_output,
)
from ..quality.parsers import StringValue, clean_str, clean_val


@model.over("legacy_recid", "^001")
def recid(self, key, value):
    """Record Identifier."""
    self["recid"] = value
    return int(value)


@model.over("agency_code", "^003")
def agency_code(self, key, value):
    """Control number identifier."""
    if isinstance(value, str):
        return value
    else:
        raise IgnoreKey("agency_code")


@model.over("title", "^245__")
def title(self, key, value):
    """Translates title."""
    title = StringValue(value.get("a"))
    title.required()
    return title.parse()


@model.over("description", "^520__")
def description(self, key, value):
    """Translates description."""
    description_text = StringValue(value.get("a")).parse()

    return description_text


@model.over("languages", "^041__")
@for_each_value
@require(["a"])
@strip_output
def languages(self, key, value):
    """Translates languages fields."""
    lang = clean_str(value.get("a"))
    if lang:
        lang = lang.lower()
    try:
        return {"id": pycountry.languages.lookup(lang).alpha_3.lower()}
    except (KeyError, AttributeError, LookupError):
        raise UnexpectedValue(field=key, subfield="a")


def process_contributors(key, value):
    """Utility processing contributors XML."""
    role = value.get("e")
    if role:
        role = get_contributor_role("e", role)
    beard = value.get("9")
    if beard is not None and beard != "#BEARD#":
        # checking if anything else stored in this field
        # historically it was some kind of automatic script tagging
        # and it should be ignored if value == #BEARD#
        raise UnexpectedValue(field=key, subfield="9", value=beard)
    affiliations = get_contributor_affiliations(value)

    names = value.get("a")
    if type(names) == tuple or names is None:
        raise UnexpectedValue(field=key, subfield="a", value=names)

    names = names.strip().split(",")

    if len(names) == 2:
        names = {"family_name": names[0].strip(), "given_name": names[1].strip()}
    else:
        names = {"family_name": names[0].strip()}
    contributor = {
        "person_or_org": {
            "type": "personal",
            **names,
            "identifiers": extract_json_contributor_ids(value),
        }
    }
    if role:
        contributor.update({"role": {"id": role}})
    elif not role and key == "700__":
        # creator does not require role, so if the key == 100 role can be skipped
        contributor.update({"role": {"id": "other"}})

    if affiliations:
        contributor.update({"affiliations": affiliations})

    return contributor


@model.over("creators", "^100__")
@for_each_value
@require(["a"])
def creators(self, key, value):
    """Translates the creators field."""
    return process_contributors(key, value)


@model.over("contributors", "^700__")
@for_each_value
@require(["a"])
def contributors(self, key, value):
    """Translates contributors."""
    return process_contributors(key, value)


@model.over("submitter", "(^859__)")
def record_submitter(self, key, value):
    """Translate record submitter."""
    submitter = value.get("f")
    if type(submitter) is tuple:
        submitter = submitter[0]
        raise UnexpectedValue(field=key, subfield="f", value=value.get("f"))
        # TODO handle all the other submitters
    if submitter:
        submitter = submitter.lower()
    return submitter
