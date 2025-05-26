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

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.reports.log import RDMJsonLogger
from cds_migrator_kit.transform.xml_processing.quality.decorators import (
    for_each_value,
    require,
)
from cds_migrator_kit.transform.xml_processing.quality.parsers import StringValue

# ATTENTION when COPYING! important which model you use as decorator
from ...models.video_lecture import model
from ..quality.contributors import (
    get_contributor,
)
from ..quality.dates import parse_date


@model.over("date", "^518__")
@for_each_value
def date(self, key, value):
    """Translates date from tag 518."""
    # Lecture informations
    event_id = value.get("g", "").strip()
    location = value.get("r", "").strip()

    lecture_info = {
        k: v for k, v in {"event_id": event_id, "location": location}.items() if v
    }
    if lecture_info:
        self["lecture_infos"].append(lecture_info)

    # Date: 518 'd' subfield
    parsed_date = parse_date(value.get("d", "").strip())
    if parsed_date:
        return parsed_date
    # 518 'a' subfield (e.g., 'CERN, Geneva, 23 - 27 Nov 1998')
    parsed_date = parse_date(value.get("a", "").split(",")[-1])
    if parsed_date:
        return parsed_date


@model.over("publication_date", "^269__")
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
def related_person(self, key, value):
    """Translates related person."""
    role = value.get("e", "").strip().lower()
    contributor_role = "" if role else "RelatedPerson"
    return get_contributor(key, value, contributor_role=contributor_role)


@model.over("event_speakers", "^906__")
@for_each_value
@require(["p"])
def event_speakers(self, key, value):
    """Translates event_speakers."""
    name = value.get("p").strip()
    if name:  # Drop empty values https://cds.cern.ch/record/1563786/
        return get_contributor(key, value, contributor_role="Speaker", name=name)
    return None


@model.over("url_files", "^8564_")
@for_each_value
@require(["u"])
def url_files(self, key, value):
    """Detects 8564 files."""
    url = value.get("u")
    if "digital-memory" in url:
        return {
            "digitized": {
                k: v
                for k, v in {
                    "url": url,
                    "format": value.get("q", ""),
                    "link_text": value.get("y", ""),
                    "public_note": value.get("z", ""),
                    "nonpublic_note": value.get("x", ""),
                    "md5_checksum": value.get("w", ""),
                    "source": value.get("2", ""),
                }.items()
                if v
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
        text = value.get("y", "")
        if text:
            indico_link["text"] = text
        match_date = re.sub(r"^Talk\s*", "", text)
        if match_date:
            parsed_date = parse_date(match_date)
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


@model.over("notes", "^500__")
@for_each_value
@require(["a"])
def notes(self, key, value):
    """Detects notes."""
    note_str = value.get("a").strip()
    if value.get("9"):
        note_str = value.get("9").strip() + " : " + value.get("a").strip()
    note = {"note": note_str}

    parts = note_str.split(",")
    match_date = parts[-1].strip() if len(parts) > 1 else ""
    if match_date:
        parsed_date = parse_date(match_date)
        if parsed_date:
            note.update({"date": parsed_date})

    return note


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


@model.over("lecture_created", "^961__")
def creation_date(self, key, value):
    """Translate record creation date.

    - tag 961, subfield code x for creation date
    - tag 961, subfield code c for modification_date

    It can also store:
    - library 'l' subfield
    - hour 'h' subfield
    - cataloguer 'a' subfield (name of the curator)
    - cataloguer level 'b' subfield (curator?)
    """
    # 961 'x' subfield
    creation_date = value.get("x", "").strip()
    parsed_creation_date = parse_date(creation_date)
    if not parsed_creation_date:
        # Check if anything else stored
        raise UnexpectedValue(field=key, subfield="x", value=creation_date)
    # 961 'c' subfield
    modification_date = value.get("c", "").strip()
    parsed_modification_date = parse_date(modification_date)
    if modification_date and not parsed_modification_date:
        # Check if anything else stored
        raise UnexpectedValue(field=key, subfield="c", value=modification_date)
    return parsed_creation_date


@model.over("indico_information", "^111__")
def indico_information(self, key, value):
    """Translates indico_informations.

    111__z: End date, ignored.
    """
    title = value.get("a", "").strip()
    event_id = value.get("g", "").strip()
    location = value.get("c", "").strip()
    start_date = value.get("9", "").strip()
    parsed_date = parse_date(start_date)

    return {
        k: v
        for k, v in {
            "title": title,
            "start_date": parsed_date,
            "event_id": event_id,
            "location": location,
        }.items()
        if v
    }


@model.over("contributors", "^270__")
@for_each_value
@require(["p"])
def contact_person(self, key, value):
    """Translates contact person."""
    name = value.get("p", "").strip()
    # Drop empty 270 tag: https://cds.cern.ch/record/2897088
    if name:
        return get_contributor(key, value, name=name, contributor_role="ContactPerson")
    return None


@model.over("contributors", "^710__")
@for_each_value
def collaboration(self, key, value):
    """Translates collaboration."""
    corporate_name = value.get("a", "").strip()
    collaboration = value.get("g", "").strip()
    cern_department = value.get("5", "").strip()

    # Add with role 'Producer'
    if corporate_name:
        if corporate_name in ["CERN, Geneva", "CERN. Geneva"]:
            corporate_name = "CERN"
        self["contributors"].append(
            get_contributor(
                key, value, name=corporate_name, contributor_role="Producer"
            )
        )

    # Add it in curation
    if cern_department:
        if self["_curation"].get("department"):
            raise UnexpectedValue(
                field=key, subfield="5", message="Multiple departments!"
            )
        self["_curation"]["department"] = cern_department

    if collaboration:
        return get_contributor(
            key, value, name=collaboration, contributor_role="ResearchGroup"
        )
    return None


@model.over("report_number", "^088__")
@for_each_value
def report_number(self, key, value):
    """Translates report number."""
    identifier = value.get("a", "")
    identifier = StringValue(identifier).parse()
    provenance = value.get("9", "")
    z_value = value.get("z", "")
    if z_value:
        raise UnexpectedValue(field=key, subfield="z", value=z_value)
    if identifier and provenance:
        raise UnexpectedValue(
            message="Report number: two values!", field=key, value=value
        )
    if not identifier and not provenance:
        raise UnexpectedValue(
            message="Report number: missing identifier!", field=key, value=value
        )
    return identifier or provenance


@model.over("related_identifiers", "^035__")
@for_each_value
@require(["a"])
def system_control_number(self, key, value):
    """
    Translates system control number.

    Possible schema values:
    - Indico, Agendamaker, AgendaMaker, CERCER, CERN annual report.
    Transform these schemas, rest will be dropped:
    - Indico, Agendamaker, AgendaMaker
    """
    schema = value.get("9", "")
    identifier = value.get("a", "")
    identifier = StringValue(identifier).parse()

    # TODO Check with indico if we can convert the values to URL
    if schema in ["Indico", "Agendamaker", "AgendaMaker"]:
        return {
            "scheme": "Indico",
            "identifier": identifier,
            "relation_type": "IsPartOf",
        }

    # Some identifiers: '0329956CERCER' https://cds.cern.ch/record/403279
    elif schema in ["CERCER", "CERN annual report"] or any(
        identifier.endswith(suffix) for suffix in ("CERCER", "CER")
    ):
        return None
    else:  # Check if any other value stored
        raise UnexpectedValue(
            message="Unkown system control number schema!", field=key, value=schema
        )


@model.over("contributors", "^110__")
@require(["a"])
@for_each_value
def corporate_author(self, key, value):
    """Translates corporate_author."""
    corporate_name = value.get("a", "").strip()

    if corporate_name in ["CERN, Geneva", "CERN. Geneva"]:
        corporate_name = "CERN"

    if corporate_name:
        return get_contributor(
            key, value, name=corporate_name, contributor_role="Producer"
        )
    return None


@model.over("subject_categories", "(^65017)|(^65027)")
@for_each_value
def subject_categories(self, key, value):
    """Translates subject_category as keywords from tag 65017,65027."""
    keyword = value.get("a", "").strip()
    source = value.get("2", "").strip()
    if source and source != "SzGeCERN":
        # checking if anything else stored in this field
        raise UnexpectedValue(field=key, subfield="9", value=source)

    if keyword:
        return {"name": keyword}


@model.over("additional_titles", "^246__")
@for_each_value
def additional_titles(self, key, value):
    """Translates additional_titles and volumes."""
    additional_title = {}
    title = value.get("a", "").strip()
    title_remainder = value.get("b", "").strip()
    lang = value.get("i", "").strip()
    part = value.get("n", "").strip()
    volume = value.get("p", "").strip()

    if title:
        formatted_title = f"{title} : {title_remainder}" if title_remainder else title
        additional_title["title"] = formatted_title
        if lang:
            if lang != "Titre français":
                raise UnexpectedValue(field=key, subfield="i", value=lang)
            additional_title["lang"] = "fr"

    if volume:
        formatted_volume = f"{part} : {volume}" if part else volume
        additional_title["volume"] = formatted_volume

    if additional_title:
        return additional_title


@model.over("additional_descriptions", "^590__")
@for_each_value
def additional_descriptions(self, key, value):
    """Translates additional_descriptions."""
    description = value.get("a", "").strip()
    if description:
        return {"description": description, "type": "Other", "lang": "fr"}
    return None
