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
from dojson.utils import force_list

# ATTENTION when COPYING! important which model you use as decorator
from ...models.people import model


@model.over("email", "^371__")
@out_strip
def email(self, key, value):
    """Translates report_number fields."""
    self["department"] = clean_val("i", value, str)
    return clean_val("m", value, str)


@model.over("person_id", "^035__")
def person_id(self, key, value):
    """Translate person id."""
    # _person_id = clean_val("a", value, str)
    _ids = force_list(value.get("a"))
    for i in _ids:
        if "AUTHOR|(INSPIRE)" in i:
            pass
        else:
            person_id = i.replace("AUTHOR|(SzGeCERN)", "").strip()
            return person_id


@model.over("surname", "^1001_")
def surname(self, key, value):
    """Translate surname."""
    return clean_val("a", value, str)


@model.over("given_names", "^1000_")
def given_names(self, key, value):
    """Translate given names."""
    return clean_val("a", value, str)
