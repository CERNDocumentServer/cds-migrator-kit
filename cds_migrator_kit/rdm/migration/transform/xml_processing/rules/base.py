# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM migration rules module."""

import datetime

import pycountry
from dojson.errors import IgnoreKey
from dojson.utils import filter_values, flatten, force_list
from dateutil.parser import parse
from dateutil.parser._parser import ParserError
from ...models.base import model
from ..quality.contributors import extract_json_contributor_ids, get_contributor_role, \
    get_contributor_affiliations
from ..dates import get_week_start
from ..errors import UnexpectedValue
from ..quality.decorators import (
    filter_list_values,
    for_each_value,
    require,
    strip_output,
)
from ..quality.parsers import clean_str, clean_val


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


@model.over("_created", "(^916__)")
@require(["w"])
def created(self, key, value):
    """Translates created information to fields."""
    if "s" in value:
        source = clean_val("s", value, str)
        if source != "n":
            raise UnexpectedValue(subfield="s", key="key", value=value)
    date_values = value.get("w")
    if not date_values or not date_values[0]:
        return datetime.date.today().isoformat()
    if isinstance(date_values, list):
        date = min(date_values)
    else:
        date = int(date_values)
    try:
        if date:
            if not (100000 < int(date) < 999999):
                raise UnexpectedValue("Wrong date format", field=key, subfield='w')
            year, week = str(date)[:4], str(date)[4:]
            date = get_week_start(int(year), int(week))
            if date < datetime.date.today():
                return date.isoformat()
            else:
                return datetime.date.today().isoformat()
    except ValueError:
        return datetime.date.today().isoformat()


@model.over("title", "^245__")
def title(self, key, value):
    """Translates title."""
    return value.get("a", "TODO")


@model.over("description", "^520__")
def description(self, key, value):
    """Translates description."""
    description_text = value.get("a")

    return description_text


@model.over("additional_descriptions", "(^500__)|(^246__)")
@for_each_value
@require(["a"])
def additional_descriptions(self, key, value):
    """Translates additional description."""
    description_text = value.get("a")

    if key == "500__":
        additional_description = {
            "description": description_text,
            "type": {
                "id": "other",  # what's with the lang
            }
        }
    elif key == "246__":
        _abbreviations = []
        is_abbreviation = value.get("i") == "Abbreviation"
        _abbreviations.append(description_text)

        if is_abbreviation:
            additional_description = {
                "description": "Abbreviations: " + "; ".join(_abbreviations),
                "type": {
                    "id": "other",  # what's with the lang
                }
            }

    return additional_description


def publisher(self, key, value):
    """Translates publisher."""
    publisher = value.get("b")
    if publisher:
        self["publisher"] = publisher
    else:
        raise IgnoreKey("publisher")

def publication_date(self, key, value):
    """Translates publication_date."""
    publication_date_str = value.get("c")
    try:
        date_obj = parse(publication_date_str)
        self["publication_date"] = date_obj.strftime("%Y-%m-%d")
        return
    except ParserError:
        raise UnexpectedValue(field="publication_date", message=f"Can't parse provided publication date. Value: {publication_date_str}")


@model.over("imprint", "^269__")
def imprint(self, key, value):
    """Translates imprint - WARNING - also publisher and publication_date."""

    imprint = {
        "place": value.get("a"),
    }

    if not self.get("publication_date"):
        publication_date(self, key, value)

    if not self.get("publisher"):
        publisher(self, key, value)

    return imprint


@model.over("creators", "(^100__)|(^700__)")
@for_each_value
@require(["a"])
def creators(self, key, value):
    """Translates the creators field."""
    role = get_contributor_role("e", value.get("e", "author"))
    affiliations = get_contributor_affiliations(value)
    contributor = {
        "person_or_org": {
            "type": "personal",
            "family_name": value.get("a"),
            "identifiers": extract_json_contributor_ids(value),
        }
    }
    if role:
        contributor.update({"role": {"id": role}})  # VOCABULARY ID

    if affiliations:
        contributor.update({"affiliations": affiliations})

    return contributor


@model.over("contributors", "^700__")
@for_each_value
@require(["a"])
def contributors(self, key, value):
    """Translates contributors."""
    return creators(self, key, value)


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
        return pycountry.languages.lookup(lang).alpha_3.upper()
    except (KeyError, AttributeError, LookupError):
        raise UnexpectedValue(field=key, subfield="a")


@model.over("subjects", "(^6931_)|(^650[1_][7_])|(^653[1_]_)")
@require(["a"])
@filter_list_values
def subjects(self, key, value):
    """Translates subjects fields."""
    _subjects = self.get("subjects", [])
    subject_value = value.get("a")
    subject_scheme = value.get("2") or value.get("9")

    if subject_scheme and subject_scheme.lower() != "szgecern":
        raise UnexpectedValue(field=key, subfield="2")
    if key == "65017" or key == "6531_":
        if subject_value:
            subject = {
                "id": subject_value,
                "subject": subject_value,
                "scheme": "CERN",
            }
            _subjects.append(subject)
    return _subjects


@model.over("custom_fields", "(^693__)")
def custom_fields(self, key, value):
    """Translates custom fields."""

    _custom_fields = self.get("custom_fields", {})

    if key == "693__":
        experiment = value.get("e")
        _custom_fields["cern:experiment"] = experiment
    return _custom_fields
