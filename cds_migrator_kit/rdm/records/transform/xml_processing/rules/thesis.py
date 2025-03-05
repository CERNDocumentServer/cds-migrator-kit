# -*- coding: utf-8 -*-
#
# This file is part of CERN Document Server.
# Copyright (C) 2025 CERN.
#
# Invenio is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the
# License, or (at your option) any later version.
#
# Invenio is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Invenio; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.
"""CDS-RDM migration rules module."""
from dateutil.parser import ParserError, parse
from dojson.errors import IgnoreKey

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.decorators import (
    filter_list_values,
    for_each_value,
    require,
    strip_output,
)
from cds_migrator_kit.transform.xml_processing.quality.parsers import StringValue
from cds_migrator_kit.transform.xml_processing.rules.base import process_contributors

from ...models.thesis import thesis_model as model


@model.over("contributors", "^701__")
@for_each_value
@require(["a"])
def contributors(self, key, value):
    """Translates contributors."""
    return process_contributors(key, value)


@model.over("collection", "^690C_")
@for_each_value
def collection(self, key, value):
    """Translates collection field."""
    collection = value.get("a").strip().lower()
    if collection == "cern":
        raise IgnoreKey("collection")
    if value.get("a").strip().lower() != "thesis":
        raise UnexpectedValue(subfield="a", key=key, value=value, field="690C_")
    raise IgnoreKey("collection")


@model.over("publication_date", "(^260__)|(^250__)")
def imprint_info(self, key, value):
    """Translates imprint - WARNING - also publisher and publication_date.

    In case of summer student notes this field contains only date
    but it needs to be reimplemented for the base set of rules -
    it will contain also imprint place
    """
    publication_date_str = value.get("c")
    _publisher = value.get("b")
    place = value.get("a")
    _custom_fields = self.get("custom_fields", {})
    imprint = _custom_fields.get("imprint:imprint", {})

    if _publisher and not self.get("publisher"):
        self["publisher"] = _publisher

    if key.startswith("250"):
        edition = StringValue(value.get("a")).parse()
        imprint["edition"] = edition
    if place:
        imprint["place"] = place

    self["custom_fields"]["imprint:imprint"] = imprint
    try:
        date_obj = parse(publication_date_str)
        return date_obj.strftime("%Y-%m-%d")
    except (ParserError, TypeError) as e:
        raise UnexpectedValue(
            field=key,
            value=value,
            message=f"Can't parse provided publication date. Value: {publication_date_str}",
        )


@model.over("custom_fields", "(^502__)")
def thesis(self, key, value):
    """Translates custom thesis fields."""
    _custom_fields = self.get("custom_fields", {})
    thesis_fields = _custom_fields.get("thesis:thesis", {})
    dates = self.get("dates", [])

    th_type = StringValue(value.get("a")).parse()
    uni = StringValue(value.get("b")).parse()

    thesis_fields["type"] = th_type
    thesis_fields["university"] = uni

    _custom_fields["thesis:thesis"] = thesis_fields

    submission_date = value.get("c")
    try:
        submission_date = parse(submission_date)
        dates.append({"date": submission_date, "type": "submitted"})
    except (ParserError, TypeError) as e:
        raise UnexpectedValue(
            field=key,
            value=value,
            message=f"Can't parse provided publication date. Value: {submission_date}",
        )

    return _custom_fields


@model.over("custom_fields", "(^773__)")
def journal(self, key, value):
    _custom_fields = self.get("custom_fields", {})
    journal_fields = _custom_fields.get("journal:journal", {})

    # year = StringValue(value.get("y")).parse()

    journal_fields["title"] = StringValue(value.get("p")).parse()
    journal_fields["issue"] = StringValue(value.get("n")).parse()
    journal_fields["volume"] = StringValue(value.get("v")).parse()
    journal_fields["pages"] = StringValue(value.get("c")).parse()

    _custom_fields["journal:journal"] = journal_fields
    return _custom_fields
