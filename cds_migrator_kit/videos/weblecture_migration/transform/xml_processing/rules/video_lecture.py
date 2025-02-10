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
"""Common Videos fields."""
from dateutil.parser import ParserError, parse
from cds_migrator_kit.transform.xml_processing.quality.decorators import (
    for_each_value,
    require,
)

# ATTENTION when COPYING! important which model you use as decorator
from ...models.video_lecture import model


@model.over("date", "^518__")
@for_each_value
def date(self, key, value):
    """Translates date from tag 518."""

    def parse_date(date_str):
        """Parses a date string into 'YYYY-MM-DD' format."""
        try:
            if len(date_str) < 10:  # Too short to have the full date info
                return None
            parsed_date = parse(date_str)
            return parsed_date.strftime("%Y-%m-%d")
        except ParserError:
            return

    # List of possible subfields containing dates
    possible_date_fields = [
        value.get("d"),  # 518 'd' subfield (e.g., '2024-11-19T14:00:00')
        value.get("c"),  # 269 'c' subfield (e.g., '1993-08-09')
        value.get("a"),  # 518 'a' subfield (e.g., 'CERN, Geneva, 23 - 27 Nov 1998')
    ]

    for date_field in possible_date_fields:
        if date_field:
            parsed_date = parse_date(date_field)
            if parsed_date:  # If parsing succeeds, return the formatted date
                return parsed_date


@model.over("date", "^269__")
def imprint(self, key, value):
    """Translates date from tag 269."""
    return date(self, key, value)


@model.over("contributors", "^511__")
@for_each_value
@require(["a"])
def performer(self, key, value):
    """Translates performer."""
    name = value.get("a").strip()
    role = value.get("e")
    contributor = {"name": name, "role": "Performer"}  # TODO or "Participant"
    # TODO contributor affiliation will be implemented
    return contributor


@model.over("contributors", "^906__")
@for_each_value
@require(["p"])
def event_speakers(self, key, value):
    """Translates event_speakers."""
    name = value.get("p").strip()
    contributor = {"name": name, "role": "Speaker"}
    # TODO contributor affiliation will be implemented
    return contributor
