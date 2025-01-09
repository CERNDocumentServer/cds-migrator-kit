# -*- coding: utf-8 -*-
#
# This file is part of CERN Document Server.
# Copyright (C) 2024 CERN.
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
"""Common RDM fields."""
from dateutil.parser import ParserError, parse
from dojson.errors import IgnoreKey
from dojson.utils import force_list

# ATTENTION when COPYING! important which model you use as decorator
from ...models.summer_student_report import model
from ..errors import MissingRequiredField, UnexpectedValue
from ..quality.decorators import for_each_value
from ..quality.parsers import StringValue


@model.over("contributors", "^270__")
@for_each_value
def contact_person(self, key, value):
    """Translates contact person."""
    contributor = {
        "person_or_org": {
            "type": "personal",
            "name": StringValue(value.get("p")).parse(),
            "family_name": StringValue(value.get("p")).parse(),
        },
        "role": {"id": "contactperson"},
    }
    return contributor


@model.over("contributors", "^906__")
@for_each_value
def supervisor(self, key, value):
    """Translates supervisor."""
    supervisor = StringValue(value.get("p")).parse()
    if not supervisor:
        raise MissingRequiredField(field=key, subfield="p", priority="warning")
    contributor = {
        "person_or_org": {
            "type": "personal",
            "name": StringValue(value.get("p")).parse(),
            "family_name": StringValue(value.get("p")).parse(),
        },
        "role": {"id": "supervisor"},
    }

    return contributor


@model.over("contributors", "^710__")
@for_each_value
def corporate_author(self, key, value):
    """Translates corporate author."""
    if "g" in value:
        contributor = {
            "person_or_org": {
                "type": "organizational",
                "name": StringValue(value.get("g")).parse(),
                "family_name": StringValue(value.get("g")).parse(),
            },
            "role": {"id": "hostinginstitution"},
        }
        return contributor
    if "5" in value:
        department = StringValue(value.get("5")).parse()
        departments = self.get("custom_fields", {}).get("cern:departments", [])
        if department and department not in departments:
            departments.append(department)
        self["custom_fields"]["cern:departments"] = departments
        raise IgnoreKey("contributors")
    raise IgnoreKey("contributors")


@model.over("internal_notes", "^562__")
@for_each_value
def note(self, key, value):
    """Translates notes."""
    return {"note": StringValue(value.get("c")).parse()}


@model.over("custom_fields", "^690C_")
def department(self, key, value):
    """Translates department."""
    values = force_list(value.get("a"))
    for v in values:
        if "PUBL" in v:
            department = v.replace("PUBL", "").strip()
            departments = self.get("custom_fields", {}).get("cern:departments", [])
            if department and department not in departments:
                departments.append(department)

            self["custom_fields"]["cern:departments"] = departments
    raise IgnoreKey("custom_fields")


@model.over("publication_date", "^269__")
def imprint_info(self, key, value):
    """Translates imprint - WARNING - also publisher and publication_date.

    In case of summer student notes this field contains only date
    but it needs to be reimplemented for the base set of rules -
    it will contain also imprint place
    """
    publication_date_str = value.get("c")
    _publisher = value.get("b")

    if _publisher and not self.get("publisher"):
        self["publisher"] = _publisher

    try:
        date_obj = parse(publication_date_str)
        return date_obj.strftime("%Y-%m-%d")
    except ParserError:
        raise UnexpectedValue(
            field=key,
            value=value,
            message=f"Can't parse provided publication date. Value: {publication_date_str}",
        )
