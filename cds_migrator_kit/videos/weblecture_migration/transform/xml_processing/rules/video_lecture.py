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
"""Common Videos fields."""
import re

from dateutil.parser import ParserError, parse

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.decorators import (
    for_each_value,
    require,
)
from cds_migrator_kit.videos.weblecture_migration.transform.xml_processing.quality.contributors import (
    get_contributor,
)

# ATTENTION when COPYING! important which model you use as decorator
from ...models.video_lecture import model


def parse_date(date_str):
    """Parses a date string into 'YYYY-MM-DD' format.

    Returns None if the string is missing, too short, too long,
    or if it contains incomplete or ambiguous date information.

    Examples:
    - Some values only contain year (e.g., "1998")
    - Some values has date range (e.g., "23 - 27 Nov 1998")
    """
    if not date_str:
        return None
    if len(date_str) < 10 or len(date_str) > 13:
        # Too short/long to have the full date info
        return None
    try:
        parsed_date = parse(date_str)
        return parsed_date.strftime("%Y-%m-%d")
    except ParserError:
        return


@model.over("date", "^518__")
@for_each_value
def date(self, key, value):
    """Translates date from tag 518."""
    # 518 'd' subfield, take the first 10 char, it might have another character (e.g., 2008-03-11T14:00:00Z)
    parsed_date = parse_date((value.get("d") or "")[:10])
    if parsed_date:
        return parsed_date
    # 518 'a' subfield (e.g., 'CERN, Geneva, 23 - 27 Nov 1998')
    parsed_date = parse_date(value.get("a", "").split(",")[-1])
    if parsed_date:
        return parsed_date


@model.over("date", "^269__")
@for_each_value
def imprint(self, key, value):
    """Translates date from tag 269."""
    name = value.get("b")
    if name and name.strip().upper() != "CERN":
        # checking if anything else stored in this field
        # and it should be ignored if value == CERN
        raise UnexpectedValue(field=key, subfield="b", value=name)
    place = value.get("a")
    if place and place.strip().upper() != "GENEVA":
        # checking if anything else stored in this field
        # and it should be ignored if value == Geneva
        raise UnexpectedValue(field=key, subfield="a", value=place)

    date_field = value.get("c")  # 269 'c' subfield (e.g., '1993-08-09')
    parsed_date = parse_date(date_field)
    if parsed_date:  # If parsing succeeds, return the formatted date
        return parsed_date


@model.over("contributors", "^511__")
@for_each_value
@require(["a"])
def performer(self, key, value):
    """Translates performer/Participant."""
    role = value.get("e")
    if role and role.strip().lower() != "speaker":
        # checking if anything else stored in this field
        raise UnexpectedValue("Different role found", field=key, subfield="e", value=role)
    return get_contributor(key, value, contributor_role="Performer")


@model.over("contributors", "^906__")
@for_each_value
@require(["p"])
def event_speakers(self, key, value):
    """Translates event_speakers."""
    return get_contributor(key, value, contributor_role="Speaker", name=value.get("p").strip())


@model.over("url_files", "^8564_")
@for_each_value
@require(["u"])
def url_files(self, key, value):
    """Detects 8564 files."""
    url = value.get("u")
    if "digital-memory" in url:
        return {
            "digitized": {
                "url": url,
                "format": value.get("q"),
                "link_text": value.get("y"),
                "public_note": value.get("z"),
                "nonpublic_note": value.get("x"),
                "md5_checksum": value.get("w"),
                "source": value.get("2"),
            }
        }
    elif "indico" in url or "agenda" in url:
        indico_link = {"url": url}

        # Try to get event id
        match_id = re.search(r"(?:ida=|confId=|event/)([\w\d]+)", url)
        if match_id:
            event_id = match_id.group(1)
            if event_id:
                indico_link["event_id"] = event_id

        # Try to get the date from text
        text = value.get("y")
        if text:
            indico_link["text"] = text
        match_date = re.search(r"(?:Talk\s*)?(\d{1,2}\s\w{3}\s\d{4})", text)
        if match_date:
            parsed_date = parse_date(match_date.group(1))
            if parsed_date:
                indico_link["date"] = parsed_date

        return {"indico": indico_link}

    url_file = {"url_file": {"url": url}}
    text = value.get("y")
    if text:
        url_file["url_file"]["text"] = text

    nonpublic_note = value.get("x")
    if nonpublic_note:
        url_file["url_file"]["nonpublic_note"] = nonpublic_note
    return url_file


@model.over("internal_notes", "^500__")
@for_each_value
@require(["a"])
def internal_notes(self, key, value):
    """Detects internal notes."""
    note = value.get("a").strip()
    if value.get("9"):
        note = value.get("9").strip() + " : " + value.get("a").strip()
    internal_note = {"note": note}

    parts = note.split(",")
    match_date = parts[-1].strip() if len(parts) > 1 else ""
    if match_date:
        parsed_date = parse_date(match_date)
        if parsed_date:
            internal_note.update({"date": parsed_date})

    return internal_note


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
