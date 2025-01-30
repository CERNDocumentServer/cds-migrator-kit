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

from dojson.errors import IgnoreKey

from ...models.thesis import model
from ..errors import UnexpectedValue
from ..quality.decorators import (
    filter_list_values,
    for_each_value,
    require,
    strip_output,
)
from .base import process_contributors


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
