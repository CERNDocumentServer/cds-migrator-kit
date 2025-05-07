# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos migration rules module."""

import datetime

import pycountry

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.rdm.records.transform.xml_processing.rules.base import (
    created as base_created,
)
from cds_migrator_kit.transform.xml_processing.quality.dates import get_week_start
from cds_migrator_kit.transform.xml_processing.quality.decorators import (
    for_each_value,
    require,
    strip_output,
)
from cds_migrator_kit.transform.xml_processing.quality.parsers import (
    StringValue,
    clean_str,
    clean_val,
)

from ...models.base import model
from ..quality.contributors import get_contributor


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
    return get_contributor(key, value)


@model.over("contributors", "^700__")
@for_each_value
@require(["a"])
def contributors(self, key, value):
    """Translates contributors."""
    return get_contributor(key, value)


@model.over("submitter", "(^859__)|(^856__)")
@require(["f"])
def record_submitter(self, key, value):
    """Translate record submitter."""
    submitter = value.get("f")
    if type(submitter) is tuple:
        submitter = submitter[0]
        raise UnexpectedValue(field=key, subfield="f", value=value.get("f"))
    if submitter:
        submitter = submitter.lower()
    return submitter


@model.over("_created", "(^916__)")
@require(["w"])
def created(self, key, value):
    """Translates created information to fields."""
    return base_created(self, key, value)


@model.over("keywords", "^653[12_]_")
@require(["a"])
@for_each_value
def keywords(self, key, value):
    """Translates keywords from tag 6531."""
    keyword = value.get("a", "").strip()
    provenance = value.get("9", "").strip()
    if provenance and provenance not in ["CERN", "review"]:
        # checking if anything else stored in this field
        raise UnexpectedValue(field=key, subfield="9", value=provenance)

    if keyword:
        return {"name": keyword}


@model.over("accelerator_experiment", "^693__")
@for_each_value
def accelerator_experiment(self, key, value):
    """Translates accelerator_experiment from tag 693."""
    accelerator = value.get("a", "").strip()
    experiment = value.get("e", "").strip()
    project = value.get("p", "").strip()
    study = value.get("s", "").strip()
    facility = value.get("f", "").strip()

    return {
        k: v
        for k, v in {
            "accelerator": accelerator,
            "experiment": experiment,
            "project": project,
            "study": study,
            "facility": facility,
        }.items()
        if v
    }
