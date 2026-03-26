# -*- coding: utf-8 -*-
#
# This file is part of CERN Document Server.
# Copyright (C) 2026 CERN.
#
# Invenio is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the
# License, or (at your option) any later version.


"""CDS-Videos Digitized Video Lecture rules."""

import pycountry
from dojson.errors import IgnoreKey

from cds_migrator_kit.errors import MissingRequiredField, UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.decorators import (
    for_each_value,
    require,
    strip_output,
)
from cds_migrator_kit.transform.xml_processing.quality.parsers import (
    StringValue,
    clean_str,
)
from cds_migrator_kit.videos.weblecture_migration.transform.xml_processing.quality.curation import (
    transform_subfields,
)

from ...models.digitized_video_lecture import model  # noqa: F401
from ..quality.dates import parse_date
from .video_lecture import presented_at, series, url_files


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


def add_contributor(self, name, role):
    """Add a contributor to the record."""
    contributors = self.get("contributors", [])
    contributor_names = [contributor["name"] for contributor in contributors]
    if name and name not in contributor_names:
        contributors.append({"name": name, "role": role})
        self["contributors"] = contributors


def validate_copyright(self, key, holder, year):
    if "copyright" in self:
        copyright = self["copyright"]
        if holder.lower() not in copyright["holder"].lower():
            raise UnexpectedValue(message="Copyright holders not matching!", field=key)
        if year:
            if "year" not in copyright:
                self["copyright"]["year"] = year
            elif year not in copyright["year"]:
                # Curated year is the correct year, use that one
                # There is only one record with different year: https://cds.cern.ch/record/281783/export/xm
                # If different curated years raise an error
                curated_copyright = self.get("curated_copyright", {})
                curated_year = curated_copyright.get("year")
                if curated_year and curated_year != copyright.get("year"):
                    raise UnexpectedValue(
                        message="Copyright years not matching!", field=key
                    )
                # Different year in curated copyright
                if curated_year and key == "5421_":
                    raise UnexpectedValue(
                        message="Copyright years not matching!", field=key
                    )
    else:
        copyright = {"holder": holder}
        if year:
            copyright["year"] = year
        if "cern" in holder.lower():
            copyright["url"] = "http://copyright.web.cern.ch"
        self["copyright"] = copyright
        return copyright


@model.over("related_id", "^962__", override=True)
@for_each_value
def related_id(self, key, value):
    """Translates tag 962."""
    pagination = value.get("k", "").strip()
    # Only 2 records has this field: 300427, 317239
    if pagination and pagination != "no pagination":
        raise UnexpectedValue(field=key, subfield="k", value=pagination)
    new_related_id = presented_at(self, key, value)
    if new_related_id:
        rel_id = new_related_id[0]
        rel_ids = self.get("related_identifiers", [])
        if rel_id not in rel_ids:
            rel_ids.append(rel_id)
            self["related_identifiers"] = rel_ids
    raise IgnoreKey("related_id")


@model.over("descriptions", "^520__", override=True)
@for_each_value
def descriptions(self, key, value):
    """Translates description."""
    description_text = StringValue(value.get("a")).parse()
    provenance = value.get("9", "").strip()
    curation_info = value.get("8", "").strip()

    record_description = self.get("description", "")

    # Decide once whether we need to append legacy data
    should_append_legacy = bool(curation_info or provenance or record_description)

    if not record_description:
        self["description"] = description_text

    if should_append_legacy:
        append_transformed_subfields(self, key, value, "digitized_description")

    IgnoreKey("descriptions")


@model.over("language", "^041__", override=True)
@require(["a"])
@for_each_value
@strip_output
def languages(self, key, value):
    """Translates language field."""
    raw_lang = value.get("a")
    raw_lang = raw_lang if isinstance(raw_lang, (list, tuple)) else [raw_lang]

    try:
        langs = [
            pycountry.languages.lookup(clean_str(r).lower()).alpha_2.lower()
            for r in raw_lang
        ]
    except Exception:
        raise UnexpectedValue(field=key, subfield="a", value=raw_lang)

    if not langs:
        raise MissingRequiredField(field=key, subfield="a", value=raw_lang)
    provenance = value.get("9", "").strip()

    curation_field = value.get("8", "").strip()
    if curation_field or provenance:
        append_transformed_subfields(self, key, value, "digitized_language")

    language = self.get("language", "")
    additional_langs = langs[1:]
    if not language:
        self["language"] = langs[0]
    else:
        additional_langs = langs

    # Extend additional languages if not already present
    record_additional_languages = self.get("additional_languages", [])
    additional_langs = [
        lang
        for lang in additional_langs
        if lang != language and lang not in record_additional_languages
    ]
    record_additional_languages.extend(additional_langs)
    self["additional_languages"] = record_additional_languages

    IgnoreKey("language")


@model.over("keywords", "^653[12_]_", override=True)
@require(["a"])
@for_each_value
def keywords(self, key, value):
    """Translates keywords from tag 6531."""
    keyword = value.get("a", "").strip()
    provenance = value.get("9", "").strip()
    if provenance and provenance not in ["CERN", "review", "review Mar2021", "CERN QA"]:
        # checking if anything else stored in this field
        raise UnexpectedValue(field=key, subfield="9", value=provenance)
    curation_field = value.get("8", "").strip()
    if curation_field and keyword:
        append_transformed_subfields(self, key, value, "digitized_keywords")
    if keyword:
        return {"name": keyword}


@model.over("copyright", "^542__", override=True)
def copyright(self, key, value):
    """Translates copyright."""
    holder = value.get("d", "").strip()
    a_value = value.get("a", "").strip()
    year = value.get("g", "").strip()
    if holder and a_value:
        raise UnexpectedValue(
            message="Holder and a_value present at the same time!", field=key
        )
    if not holder:
        holder = a_value
    if "copyright" in self:
        validate_copyright(self, key, holder, year)
        raise IgnoreKey("copyright")
    statement = value.get("f", "").strip()
    material = value.get("3", "").strip()

    # Drop material
    if material and material not in ["publication", "Report"]:
        raise UnexpectedValue(field=key, subfield="3", value=material)

    full_holder = f"{holder} {statement}".strip()
    if not full_holder:
        raise UnexpectedValue(message="Holder is missing for copyright!")

    copyright = {"holder": full_holder}
    if year:
        copyright["year"] = year
    if "cern" in holder.lower():
        copyright["url"] = "http://copyright.web.cern.ch"

    return copyright


@model.over("publication_date", "^269__", override=True)
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

    provenance = value.get("9", "").strip()
    if provenance:
        append_transformed_subfields(self, key, value, "digitized_imprint_date")
        raise IgnoreKey("publication_date")

    date_field = value.get("c")  # 269 'c' subfield (e.g., '1993-08-09')
    parsed_date = parse_date(date_field)
    if parsed_date:  # If parsing succeeds, return the formatted date
        return parsed_date
    parsed_date = parse_date(place)
    if parsed_date:
        return parsed_date


@model.over("imprint_date", "^260__", override=True)
@for_each_value
def tag260(self, key, value):
    """Check tag 269."""

    def validate_location(val, subfield):
        if val and val.upper() not in {"GENEVA", "CERN"}:
            raise UnexpectedValue(
                field=key, subfield=subfield, value=val, message="Unexpected tag 260"
            )

    name = value.get("b")
    place = value.get("a")
    curation_info = value.get("8", "").strip()
    provenance = value.get("9", "").strip()
    if provenance and provenance not in ["review", "review Mar2021"]:
        raise UnexpectedValue(field=key, subfield="9", value=provenance)
    if curation_info:
        append_transformed_subfields(self, key, value, "digitized_imprint_date")

    validate_location(place, "a")
    validate_location(name, "b")

    date_field = value.get("c")
    parsed_date = parse_date(date_field)
    if parsed_date:  # If parsing succeeds, return the formatted date
        return parsed_date
    raise IgnoreKey("imprint_date")


@model.over("notes", "^500__", override=True)
@for_each_value
@require(["a"])
def notes(self, key, value):
    """Detects notes."""
    curation_field = value.get("8", "").strip()
    if curation_field:
        append_transformed_subfields(self, key, value, "digitized_notes")
        return None
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


@model.over("subject_indicators", "(^690C_)|(^690c_)", override=True)
@for_each_value
def subject_indicators(self, key, value):
    """Translates subject_indicators as keywords from tag 690C."""
    subject = value.get("a", "").strip()
    if subject:
        if subject not in [
            "ACAD",
            "CERN",
            "TALK",
            "movingimages",
            "SSLP",
            "reviewed",
            "quality-controlled",
        ]:
            # checking if anything else stored in this field
            raise UnexpectedValue(field=key, subfield="a", value=subject)
    curated_field = value.get("9", "").strip()
    if curated_field and curated_field not in ["review", "CERN QA"]:
        # checking if anything else stored in this field
        raise UnexpectedValue(field=key, subfield="9", value=curated_field)
    return {"name": subject}


@model.over("subject_categories", "(^65017)|(^65027)", override=True)
@for_each_value
def subject_categories(self, key, value):
    """
    Translates subject_category as keywords from tag 65017,65027.
    Digitization project: EPFL_MC Categories.
    """
    keyword = value.get("a", "").strip()
    source = value.get("2", "").strip()
    if source and source != "SzGeCERN":
        # checking if anything else stored in this field
        raise UnexpectedValue(field=key, subfield="9", value=source)
    provenance = value.get("9", "").strip()
    curation_field = value.get("8", "").strip()
    if curation_field or provenance:
        append_transformed_subfields(self, key, value, "digitized_subject_categories")
        return None
    if keyword:
        return {"name": keyword}


@model.over("additional_descriptions", "(^590__)|(^490__)", override=True)
@for_each_value
def additional_descriptions(self, key, value):
    """Translates additional_descriptions."""
    if key == "490__":
        additional_description = series(self, key, value)
        return additional_description[0]
    description = value.get("a", "").strip()
    provenance = value.get("9", "").strip()
    curation_information = value.get("8", "").strip()
    if provenance or curation_information:
        append_transformed_subfields(self, key, value, "digitized_description")
        return None
    if description:
        return {"description": description, "type": "Other", "lang": "fr"}
    return None


@model.over("lecture_created", "^961__", override=True)
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

    b_value = value.get("b", "").strip().lower()
    if b_value and b_value != "curator":
        raise UnexpectedValue(field=key, subfield="b", value=b_value)
    a_value = value.get("a", "").strip()
    if b_value or a_value:
        append_transformed_subfields(self, key, value, "cds_modification_field")
        raise IgnoreKey("lecture_created")

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


@model.over("action_note", "^5831_")
@for_each_value
def action_note(self, key, value):
    """Translates action note (digitized information)."""
    def format_field(value, subfield):
        val = value.get(subfield)
        if isinstance(val, (list, tuple)):
            if subfield == "f":
                cleaned = [str(v).strip() for v in val if v and str(v).strip()]
                return cleaned if cleaned else None
            raise UnexpectedValue(field=key, subfield=subfield, value=val)
        if subfield == "f" and val:
            return [val.strip()]
        return val if val else None

    preservation_entry = {
        k: v
        for k, v in {
            "source": format_field(value, "2"),
            "format": format_field(value, "3"),
            "institution": format_field(value, "5"),
            "batch": format_field(value, "6"),
            "sequence_identifier": format_field(value, "8"),
            "action": format_field(value, "a"),
            "digitization_setup": format_field(value, "b"),
            "date": format_field(value, "c"),
            "preservation_notes": format_field(value, "f"),
            "workflow": format_field(value, "i"),
            "vendor": format_field(value, "k"),
            "title": format_field(value, "l"),
            "duration_value": format_field(value, "n"),
            "duration_unit": format_field(value, "o"),
            "related_record": format_field(value, "u"),
            "timing_note": format_field(value, "x"),
            "quality_control_note": format_field(value, "z"),
        }.items()
        if v
    }
    preservation_values = self["_curation"].get("preservation_values", [])
    preservation_values.append(preservation_entry)
    self["_curation"]["preservation_values"] = preservation_values
    raise IgnoreKey("action_note")


@model.over("curated_copyright", "^5421_")
@for_each_value
def curation_copyright(self, key, value):
    """Translates curated copyright information."""
    provenance = value.get("9", "").strip()
    if provenance and provenance not in ["review", "review Mar2021"]:
        # checking if anything else stored in this field
        raise UnexpectedValue(field=key, subfield="9", value=provenance)
    year = value.get("g", "").strip()
    holder = value.get("d", "").strip()
    copyright = validate_copyright(self, key, holder, year)
    if copyright:
        self["curated_copyright"] = copyright
    raise IgnoreKey("curated_copyright")


@model.over("597", "^597__")
@for_each_value
def curation_local_note(self, key, value):
    """Translates digitized information."""
    append_transformed_subfields(self, key, value, "digitized_comments")


@model.over("514", "^514__")
@for_each_value
def data_quality_note(self, key, value):
    """Translates digitized information."""
    append_transformed_subfields(self, key, value, "digitized_data_quality_note")


@model.over("594", "^594__")
@for_each_value
def curated_type(self, key, value):
    """Translates digitized information."""
    type = value.get("a", "").strip()
    if type and type not in [
        "Conference Speech",
        "Conférence Speech",
        "Footage",
        "Video Clip",
        "Movie",
    ]:
        raise UnexpectedValue(field=key, subfield="a", value=type)
    provenance = value.get("9", "").strip()
    if provenance and provenance not in ["review", "review Mar2021", "CERN QA"]:
        # checking if anything else stored in this field
        raise UnexpectedValue(field=key, subfield="9", value=provenance)
    # add as digitized keyword to match with multiple video record
    curation_field = value.get("8", "").strip()
    if curation_field:
        append_transformed_subfields(self, key, value, "digitized_keywords")
    # add as keyword if not already present
    else:
        keywords_names = [keyword["name"] for keyword in self.get("keywords", [])]
        if type and type not in keywords_names:
            self["keywords"].append({"name": type})
    raise IgnoreKey("594")


@model.over("performers", "5111_")
@for_each_value
def performer_note(self, key, value):
    """Translates digitized information."""
    contributor_name = value.get("a", "").strip()
    # Sometimes it's contributor
    provenance = value.get("9", "").strip()
    if provenance and provenance not in ["review", "review Mar2021", "CERN QA"]:
        # checking if anything else stored in this field
        raise UnexpectedValue(field=key, subfield="9", value=provenance)

    # Check if any contributor is different
    performer = self.get("performer", "")
    if performer:
        if performer != contributor_name:
            append_transformed_subfields(self, key, value, "digitized_filmed_people")
            raise IgnoreKey("performers")
    else:
        self["performer"] = contributor_name

    add_contributor(self, contributor_name, "Speaker")
    raise IgnoreKey("performers")


@model.over("963", "^963__")
@for_each_value
def digitized_access(self, key, value):
    """Translates digitized information."""
    owner = value.get("a", "").strip()
    # One record's video is restricted. https://cds.cern.ch/record/1566223/
    # If it's restricted digitized link is restricted to cern only
    if owner and owner.lower() not in ["public", "restricted"]:
        raise UnexpectedValue(field=key, subfield="a", value=owner)
    append_transformed_subfields(self, key, value, "digitized_access")
    raise IgnoreKey("963")


@model.over("993", "^993__")
@for_each_value
def digitized_993(self, key, value):
    """Translates digitized information."""
    t_value = value.get("t", "").strip()
    # Only one record have this field: https://cds.cern.ch/record/690303/export/xm
    if t_value and t_value != "Fusion Research":
        raise UnexpectedValue(field=key, subfield="t", value=t_value)
    keywords_names = [keyword["name"] for keyword in self.get("keywords", [])]
    if t_value and t_value not in keywords_names:
        self["keywords"].append({"name": t_value})
    raise IgnoreKey("993")


@model.over("344", "^344__")
@for_each_value
def digitized_344(self, key, value):
    """Translates digitized information."""
    a_value = value.get("a", "").strip()
    # Only one record have this field: https://cds.cern.ch/record/319677/export/xm
    if a_value and a_value.lower() != "video":
        raise UnexpectedValue(field=key, subfield="a", value=a_value)
    raise IgnoreKey("344")


@model.over("directed_by", "(^5081_)|(^508__)")
@for_each_value
def directed_by(self, key, value):
    """Translates directed by information."""
    # All the values are the same: ignored '9' and '8' subfields and transformed as director
    director_name = value.get("a", "").strip()
    provenance = value.get("9", "").strip()
    if provenance and provenance not in ["CERN QA", "review"]:
        # checking if anything else stored in this field
        raise UnexpectedValue(field=key, subfield="9", value=provenance)
    curation_field = value.get("8", "").strip()
    directed_by = self.get("directed_by", [])
    if directed_by:
        if directed_by.lower() != director_name.lower():
            # Checking if all the values are the same
            raise UnexpectedValue(field=key, subfield="a", value=director_name)
    else:
        self["directed_by"] = director_name

    add_contributor(self, director_name, "Director")
    raise IgnoreKey("directed_by")


@model.over("020", "^020__")
@for_each_value
def book_number(self, key, value):
    """Translates digitized information."""
    # Only one record have this field: https://cds.cern.ch/record/334106/export/xm
    append_transformed_subfields(self, key, value, "legacy_marc_fields", "020")


@model.over("856", "^856_2")
@for_each_value
def digitized_856_2(self, key, value):
    """Translates digitized information."""
    # Looks like 8564_ field: https://cds.cern.ch/record/1565555/export/xm
    url_file = url_files(self, key, value)
    if url_file:
        self["url_files"].append(url_file[0])
    raise IgnoreKey("856_2")


@model.over("775", "^775__")
@require(["w"])
@for_each_value
def digitized_775(self, key, value):
    """Translates digitized information."""
    # Only one record: https://cds.cern.ch/record/423086/
    recid = value.get("w", "").strip()
    resource_type = value.get("b", "").strip().lower()
    if resource_type and resource_type != "article":
        raise UnexpectedValue(field=key, subfield="b", value=resource_type)
    related_identifier = {
        "identifier": recid,
        "scheme": "CDS",
        "relation_type": "IsVariantFormOf",
    }
    if resource_type:
        related_identifier["resource_type"] = "Text"
    # Add as related identifier if not present
    rel_ids = self.get("related_identifiers", [])
    if related_identifier not in rel_ids:
        rel_ids.append(related_identifier)
        self["related_identifiers"] = rel_ids
    raise IgnoreKey("775")


@model.over("300", "^300__")
@for_each_value
def pyhsical_description(self, key, value):
    """Translates tag 300."""
    append_transformed_subfields(self, key, value, "digitized_physical_description")
