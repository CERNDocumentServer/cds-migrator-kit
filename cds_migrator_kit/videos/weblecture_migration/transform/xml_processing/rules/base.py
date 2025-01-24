# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos migration rules module."""

import datetime

import pycountry
from cds_dojson.marc21.fields.utils import out_strip
from dateutil.parser import parse
from dateutil.parser._parser import ParserError
from dojson.errors import IgnoreKey
from dojson.utils import filter_values, flatten, force_list

# from ..dates import get_week_start
from cds_migrator_kit.rdm.migration.transform.xml_processing.errors import (
    UnexpectedValue,
)
from cds_migrator_kit.rdm.migration.transform.xml_processing.quality.decorators import (
    filter_list_values,
    for_each_value,
    require,
    strip_output,
)
from cds_migrator_kit.rdm.migration.transform.xml_processing.quality.parsers import (
    StringValue,
    clean_str,
    clean_val,
)

from ...models.base import model
from ..quality.contributors import get_contributor_role


@model.over("legacy_recid", "^001")
def recid(self, key, value):
    """Record Identifier."""
    self["recid"] = value  # TODO recid??
    return int(value)


@model.over("title", "^245__")
def title(self, key, value):
    """Translates title."""
    title = StringValue(value.get("a"))
    if value.get("b"):
        title = StringValue(value.get("a") + " : " + value.get("b"))
    title.required()
    return {"title": title.parse()}


@model.over("description", "^520__")
def description(self, key, value):
    """Translates description."""
    description_text = StringValue(value.get("a")).parse()

    return description_text


@model.over("language", "^041__")
@require(["a"])
@strip_output
def language(self, key, value):
    """Translates language field."""
    lang = clean_str(value.get("a"))
    if lang:
        lang = lang.lower()
    try:
        return pycountry.languages.lookup(lang).alpha_2.lower()
    except (KeyError, AttributeError, LookupError):
        raise UnexpectedValue(field=key, subfield="a")


@model.over("contributors", "^100__")
@for_each_value
@require(["a"])
def creators(self, key, value):
    """Translates the creators field."""
    role = value.get("e")
    if role:
        role = get_contributor_role("e", role)
    beard = value.get("9")
    if beard is not None and beard != "#BEARD#":
        # checking if anything else stored in this field
        # historically it was some kind of automatic script tagging
        # and it should be ignored if value == #BEARD#
        raise UnexpectedValue(field=key, subfield="9", value=beard)
    name = value.get("a").strip()
    contributor = {"name": name}
    if role:
        contributor.update({"role": role})
    # TODO contributor affiliation will be implemented

    return contributor


@model.over("contributors", "^700__")
@require(["a"])
def contributors(self, key, value):
    """Translates contributors."""
    return creators(self, key, value)
