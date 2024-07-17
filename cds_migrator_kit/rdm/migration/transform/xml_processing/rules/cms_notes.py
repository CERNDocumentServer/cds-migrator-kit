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

from ..models.note import model
from .contributors import extract_json_contributor_ids, get_contributor_role
from .dates import get_week_start
from .errors import UnexpectedValue
from .quality.decorators import (
    filter_list_values,
    for_each_value,
    require,
    strip_output,
)
from .quality.parsers import clean_str


@model.over("communities", "^980__")
@require(["a"])
def communities(self, key, value):
    """Translates communities."""
    return ["cms-notes"]
