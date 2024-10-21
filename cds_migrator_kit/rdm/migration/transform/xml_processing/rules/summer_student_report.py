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

from cds_dojson.marc21.fields.utils import clean_val, out_strip
from dojson.errors import IgnoreKey

from ..errors import UnexpectedValue, MissingRequiredField
from ..quality.decorators import for_each_value
from ..quality.parsers import StringValue
# ATTENTION when COPYING! important which model you use as decorator
from ...models.summer_student_report import model


@model.over("contributors", "^270__")
@for_each_value
def contact_person(self, key, value):
    contributor = {
        "person_or_org": {
            "type": "personal",
            "name": StringValue(value.get("p")).parse(),
            "family_name": StringValue(value.get("p")).parse(),
        },
        "role": {"id": "contactperson"}
    }
    return contributor


@model.over("contributors", "^906__")
@for_each_value
def supervisor(self, key, value):
    supervisor = StringValue(value.get("p"))
    if not supervisor:
        raise MissingRequiredField(field=key, subfield="p",
                                   priority="warning")
    contributor = {
        "person_or_org": {
            "type": "personal",
            "name": StringValue(value.get("p")).parse(),
            "family_name": StringValue(value.get("p")).parse(),
        },
        "role": {"id": "supervisor"}
    }

    return contributor


@model.over("contributors", "^710__")
@for_each_value
def corporate_author(self, key, value):
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
        self.get("custom_fields", {}).get("cern:departments", []).append(department)
        raise IgnoreKey


@model.over("internal_notes", "^562__")
@for_each_value
def note(self, key, value):
    return StringValue(value.get("c")).parse()
