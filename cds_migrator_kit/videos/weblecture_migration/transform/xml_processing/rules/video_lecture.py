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

from dojson.errors import IgnoreKey
from flask import current_app
from idutils.validators import is_doi

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.reports.log import RDMJsonLogger
from cds_migrator_kit.transform.xml_processing.quality.decorators import (
    for_each_value,
    require,
)
from cds_migrator_kit.transform.xml_processing.quality.parsers import StringValue
from cds_migrator_kit.videos.weblecture_migration.transform.xml_processing.quality.collections import (
    append_collection_hierarchy,
)
from cds_migrator_kit.videos.weblecture_migration.transform.xml_processing.quality.curation import (
    transform_subfields,
)

# ATTENTION when COPYING! important which model you use as decorator
from ...models.video_lecture import model
from ..quality.contributors import (
    get_contributor,
)
from ..quality.dates import parse_date
from ..quality.identifiers import get_new_indico_id, transform_legacy_urls


@model.over("date", "^518__")
@for_each_value
def date(self, key, value):
    """Translates date from tag 518."""
    # Lecture informations
    event_id = value.get("g", "").strip()
    location = value.get("r", "").strip()

    # Try to convert new id
    new_id = get_new_indico_id(event_id)
    if new_id:
        event_id = str(new_id)

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
    place = value.get("a")

    # Transform as contributor if different than CERN Geneva
    producer = " ".join(
        part
        for part in (place, name)
        if part and part.upper() not in {"GENEVA", "CERN"}
    )
    if producer:
        self["contributors"].append({"name": producer, "role": "Producer"})

    date_field = value.get("c")  # 269 'c' subfield (e.g., '1993-08-09')
    parsed_date = parse_date(date_field)
    if parsed_date:  # If parsing succeeds, return the formatted date
        return parsed_date


@model.over("_", "^260__")
@for_each_value
def tag260(self, key, value):
    """Check tag 269."""

    def validate_location(val, subfield):
        if val and val.upper() not in {"GENEVA", "CERN"}:
            raise UnexpectedValue(
                field=key, subfield=subfield, value=val, message="Unexpected tag 260"
            )

    date_field = value.get("c")  # more detailed in 269__c, drop
    name = value.get("b")
    place = value.get("a")

    validate_location(place, "a")
    validate_location(name, "b")
    IgnoreKey("_")


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

    def format_field(val):
        if isinstance(val, (list, tuple)):
            return "\n".join(str(v) for v in val if v)
        return val if val else None

    url = value.get("u")
    if "digital-memory" in url:
        return {
            "digitized": {
                k: v
                for k, v in {
                    "url": url,
                    "format": format_field(value.get("q")),
                    "link_text": format_field(value.get("y")),
                    "public_note": format_field(value.get("z")),
                    "nonpublic_note": format_field(value.get("x")),
                    "md5_checksum": format_field(value.get("w")),
                    "source": format_field(value.get("2")),
                }.items()
                if v
            }
        }
    elif "indico" in url or "agenda" in url:
        indico_link = {}
        url = transform_legacy_urls(url, type="indico")
        if url:
            indico_link["url"] = url

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

    url_file = {"url_file": {}}
    url = transform_legacy_urls(url)
    if url:
        url_file["url_file"]["url"] = url
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

    # Try to convert new id
    event_id = re.split(r"[cs]", event_id, 1)[0]
    new_id = get_new_indico_id(event_id)
    if new_id:
        event_id = str(new_id)

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
        existing = self["_curation"].get("department")
        if existing:
            self["_curation"]["department"] = f"{existing}, {cern_department}"
        else:
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
    if z_value and z_value != "1/1":
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
    rel_ids = self.get("related_identifiers", [])

    if schema in ["Indico", "Agendamaker", "AgendaMaker"]:
        if schema == "AgendaMaker":
            self["collections"] = append_collection_hierarchy(
                self["collections"], "Lectures::Video Lectures"
            )

        # Try to convert new id and exlude the contribution
        identifier = re.split(r"[cs]", identifier, 1)[0]
        id = get_new_indico_id(identifier)
        if id:
            identifier = str(id)

        rel_id = {
            "scheme": "Indico",
            "identifier": identifier,
            "relation_type": "IsPartOf",
        }
        if rel_id not in rel_ids:
            return rel_id
        return None

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
            # Transform as TranslatedTitle
            if lang == "Titre français":
                additional_title["lang"] = "fr"
            # Transform as AlternativeTitle
            elif lang not in ["Previous title", "Also quoted as"]:
                raise UnexpectedValue(field=key, subfield="i", value=lang)

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


@model.over("license", "^540__")
@for_each_value
def license(self, key, value):
    """Translates license."""
    license = value.get("a", "").strip()
    credit = value.get("b", "").strip()
    url = value.get("u", "").strip()
    material = value.get("3", "").strip()

    return {
        k: v
        for k, v in {
            "url": url,
            "credit": credit,
            "license": license,
            "material": material,
        }.items()
        if v
    }


@model.over("copyright", "^542__")
def copyright(self, key, value):
    """Translates copyright."""
    holder = value.get("d", "").strip()
    statement = value.get("f", "").strip()
    year = value.get("g", "").strip()
    material = value.get("3", "").strip()

    # Drop material
    if material and material not in ["publication", "Report"]:
        raise UnexpectedValue(field=key, subfield="3", value=material)

    full_holder = f"{year} © {holder}. {statement}".strip()
    if not full_holder:
        raise UnexpectedValue(message="Holder is missing for copyright!")

    copyright = {"holder": full_holder}
    if year:
        copyright["year"] = year
    if "CERN" in holder:
        copyright["url"] = "http://copyright.web.cern.ch"

    return copyright


@model.over("related_identifiers", "^962__")
@for_each_value
def presented_at(self, key, value):
    """Translates related identifiers."""
    recid = value.get("b")
    material = value.get("n", "").lower().strip()  # drop
    rel_ids = self.get("related_identifiers", [])
    res_type = "Event"
    if material and material.lower() == "book":
        res_type = "Book"
    if not recid:
        raise UnexpectedValue(message="Identifier is missing!", field=key)
    new_id = {
        "identifier": recid,
        "scheme": "CDS",
        "relation_type": "IsPartOf",
        "resource_type": res_type,
    }

    if new_id not in rel_ids:
        return new_id
    return None


@model.over("related_identifiers", "^773__")
@for_each_value
def published_in(self, key, value):
    """Translates related identifiers."""
    recid = value.get("r", "")
    doi = value.get("a", "")  # it's also recid
    title = value.get("p", "")  # drop?
    url = value.get("u", "")

    # Should be one identifier
    identifiers = [i for i in [recid, doi, url] if i]
    if len(identifiers) > 1:
        raise UnexpectedValue(message="Multiple identifiers found!", field=key)

    if url:
        new_id = {
            "identifier": url,
            "scheme": "URL",
            "relation_type": "IsVariantFormOf",
        }
    elif recid or doi:
        cds_id = recid or doi
        scheme = "CDS"
        if is_doi(cds_id):
            scheme = "DOI"
        new_id = {
            "identifier": cds_id,
            "scheme": scheme,
            "relation_type": "IsVariantFormOf",
        }
    else:
        raise UnexpectedValue(message="No identifier found!", field=key)

    rel_ids = self.get("related_identifiers", [])
    if new_id not in rel_ids:
        return new_id
    return None


@model.over("related_identifiers", "^7870_")
@for_each_value
def related_document(self, key, value):
    """Translates related identifiers."""
    recid = value.get("w", "")
    report_number = value.get("r", "")  # drop
    relation = value.get("i", "")

    res_type = None
    if relation:
        if relation.lower() == "conference paper":
            res_type = "ConferencePaper"
        elif relation.lower() == "yellow report":
            res_type = "Report"
        else:
            raise UnexpectedValue(message="Unknown relation!", field=key)

    if recid:
        new_id = {
            "identifier": recid,
            "scheme": "CDS",
            "relation_type": "IsVariantFormOf",
        }
    else:
        raise UnexpectedValue(message="No identifier found!", field=key)
    if res_type:
        new_id["resource_type"] = res_type

    rel_ids = self.get("related_identifiers", [])
    if new_id not in rel_ids:
        return new_id
    return None


@model.over("system_number", "^970__")
def related_document(self, key, value):
    """Translates related identifiers."""
    system_number = value.get("a", "")
    if any(system_number.endswith(suffix) for suffix in ("CERCER", "CER")):
        return None
    elif system_number.startswith("INDICO."):
        # Extract indico id without contribution
        raw_id = system_number.split(".", 1)[1]
        indico_id = re.split(r"[cs]", raw_id, 1)[0]
        # Try to convert new id
        new_id = get_new_indico_id(indico_id)
        if new_id:
            indico_id = str(new_id)
        return indico_id
    else:
        raise UnexpectedValue(field=key, subfield="a")


def append_transformed_subfields(self, key, value, field_name, subfield_name=None):
    """Helper to append transformed subfields to a curation field."""
    curation = self["_curation"]
    transformed = transform_subfields(key, value)

    if subfield_name:
        existing_values = curation.setdefault(field_name, {})
        legacy_field = existing_values.get(subfield_name, [])
        legacy_field.extend(transformed)
        if legacy_field:
            curation[field_name][subfield_name] = legacy_field
    else:
        existing_values = curation.get(field_name, [])
        existing_values.extend(transformed)
        if existing_values:
            curation[field_name] = existing_values


@model.over("physical_location", "^852__")
@for_each_value
def physical_location(self, key, value):
    """Translates physical location."""
    append_transformed_subfields(self, key, value, "physical_location")


@model.over("physical_medium", "^340__")
@for_each_value
def physical_medium(self, key, value):
    """Translates physical medium."""
    if value.get("a") == "Streaming video":
        self["collections"] = append_collection_hierarchy(
            self["collections"], "Lectures::Video Lectures"
        )
        value = dict(value)
        del value["a"]
    append_transformed_subfields(self, key, value, "physical_medium")


@model.over("internal_note", "^595__")
@for_each_value
def internal_note(self, key, value):
    """Translates internal note."""
    append_transformed_subfields(self, key, value, "internal_note")


@model.over("964", "^964__")
@for_each_value
def physical_location(self, key, value):
    """Translates tag 964."""
    append_transformed_subfields(self, key, value, "legacy_marc_fields", "964")


@model.over("583", "^583__")
@for_each_value
def physical_location(self, key, value):
    """Translates tag 583."""
    append_transformed_subfields(self, key, value, "legacy_marc_fields", "583")


@model.over("336", "^336__")
@for_each_value
def physical_location(self, key, value):
    """Translates tag 336."""
    append_transformed_subfields(self, key, value, "legacy_marc_fields", "336")


@model.over("306", "^306__")
@for_each_value
def curation_duration(self, key, value):
    """Translates tag 306."""
    append_transformed_subfields(self, key, value, "legacy_marc_fields", "306")


@model.over("doi", "^0247_")
def doi(self, key, value):
    """Translates DOI."""
    doi = value.get("a", "")
    title = value.get("2", "")
    type = value.get("q", "")

    if title and title.upper() != "DOI":
        raise UnexpectedValue(field=key, value=title)
    if type and type != "ebook":
        raise UnexpectedValue(field=key, value=title)

    if not is_doi(doi):
        raise UnexpectedValue(message="It's not a DOI!", field=key, value=doi)

    # Use as DOI
    if doi.startswith("10.17181"):
        return doi
    else:
        alternate_identifiers = self["alternate_identifiers"]
        alternate_identifiers.append({"scheme": "DOI", "value": doi})
        self["alternate_identifiers"] = alternate_identifiers
        IgnoreKey("doi")


@model.over("collections", "^980__")
@for_each_value
def collection_tags(self, key, value):
    """Translates collection_tags."""
    collection_mapping = current_app.config["COLLECTION_MAPPING"]

    primary = value.get("a", "").strip()
    secondary = value.get("b", "").strip()

    if primary and primary not in collection_mapping:
        raise UnexpectedValue(field=key, value={"a": primary})
    if secondary and secondary not in collection_mapping:
        raise UnexpectedValue(field=key, value={"b": secondary})

    primary_tag = collection_mapping.get(primary, "")
    secondary_tag = collection_mapping.get(secondary, "")

    if primary_tag:
        self["collections"] = append_collection_hierarchy(
            self["collections"], primary_tag
        )
    if secondary_tag:
        self["collections"] = append_collection_hierarchy(
            self["collections"], secondary_tag
        )


@model.over("additional_descriptions", "^490__")
@for_each_value
def series(self, key, value):
    """Translates collection_tags."""
    series = value.get("a", "").strip()
    volume = value.get("v", "").strip()

    if not series:
        raise UnexpectedValue(field=key, message="Missing series information!")

    # Add collection if it's CAS
    if series == "CERN Accelerator School":
        self["collections"] = append_collection_hierarchy(
            self["collections"], "Lectures::CERN Accelerator School"
        )

    # Add as keyword
    self["keywords"].append({"name": series})

    # Add as additional description
    description = f"{series},{volume}" if volume else series
    return {"description": description, "type": "SeriesInformation"}


@model.over("affiliation", "^901__")
def affiliation(self, key, value):
    """Translates affiliation."""
    affiliation = value.get("u", "").strip()
    return affiliation


@model.over("restriction", "^5061_")
@for_each_value
def restriction(self, key, value):
    """Translates restriction."""
    access = value.get("a", "").strip()
    restriction_type = value.get("f", "").strip()
    source = value.get("2", "").strip()
    institution = value.get("5", "").strip()

    if access != "Restricted":
        raise UnexpectedValue(field=key, value=access)
    if source and source != "CDS Invenio":
        raise UnexpectedValue(field=key, value=source)
    if institution and institution != "SzGeCERN":
        raise UnexpectedValue(field=key, value=institution)

    # If missing restrictions, return cern-accounts
    restriction_entries = value.get("d", [])
    if not restriction_entries:
        return ["cern-accounts@cern.ch"]

    # Convert restriction entries to emails
    emails = []
    if not isinstance(restriction_entries, (list, tuple)):
        restriction_entries = [restriction_entries]

    if restriction_type == "group":
        for entry in restriction_entries:
            if entry.endswith("[CERN]"):
                # Convert to email format
                group_name = entry.replace("[CERN]", "").strip()
                emails.append(f"{group_name}@cern.ch")
            else:
                raise UnexpectedValue(
                    field=key, message=f"Unknown restriction group format: {entry}"
                )
    elif restriction_type == "email":
        emails = restriction_entries
    else:
        raise UnexpectedValue(
            field=key, value=restriction_type, message="Unknown restriction type!"
        )

    return emails
