# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos migration rules module."""

import datetime

import pycountry

from cds_migrator_kit.errors import MissingRequiredField, UnexpectedValue
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
    self["recid"] = value
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
def languages(self, key, value):
    """Translates language field."""
    raw_lang = value.get("a")
    raw_lang = raw_lang if isinstance(raw_lang, (list, tuple)) else [raw_lang]

    try:
        langs = [
            pycountry.languages.lookup(clean_str(r).lower()).alpha_2.lower()
            for r in raw_lang
        ]
    except Exception:
        raise UnexpectedValue(field=key, subfield="a", value=raw_lang)

    if not langs:
        raise MissingRequiredField(field=key, subfield="a", value=raw_lang)

    self["additional_languages"].extend(langs[1:])
    return langs[0]


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


@model.over("files", "^8567_")
@for_each_value
def files(self, key, value):
    """Detects files."""
    source = value.get("2")
    if source and source.strip() != "MediaArchive":
        # Check if anything else stored
        raise UnexpectedValue(field=key, subfield="2", value=source)

    file = {}

    # Master path
    master_path = value.get("d", "").strip()
    if master_path:
        if master_path.startswith("/mnt/master_share"):
            file["master_path"] = master_path
            file_type = value.get("x", "").strip()
            if file_type and file_type != "Absolute master path":
                # Check if anything else stored
                raise UnexpectedValue(field=key, subfield="x", value=file_type)
        else:
            # Raise error if anything else stored
            raise UnexpectedValue(field=key, subfield="d", value=master_path)

    # File with url/path
    url = value.get("u", "").strip()
    if url:
        if url.startswith("/"):
            file["path"] = url  # Relative path
        elif url.startswith("https://lecturemedia.cern.ch"):
            file["url"] = url
            file["path"] = url.replace("https://lecturemedia.cern.ch", "")
        else:
            # Check if anything else stored
            raise UnexpectedValue(field=key, subfield="u", value=url)
        file_type = value.get("x")
        if file_type:
            file["type"] = file_type.strip()

        description = value.get("y")
        if description:
            file["description"] = description.strip()

    return file
