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
import re
import arrow
from edtf import parse_edtf, EDTFParseException, text_to_edtf
from dateutil.parser import ParserError, parse
from dojson.errors import IgnoreKey
from dojson.utils import force_list
from idutils.normalizers import normalize_isbn, normalize_issn
from isbnlib import NotValidISBNError

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.decorators import (
    filter_list_values,
    for_each_value,
    require,
    strip_output,
)
from cds_migrator_kit.transform.xml_processing.quality.parsers import StringValue
from cds_migrator_kit.transform.xml_processing.rules.base import process_contributors
from ...config import (
    ALLOWED_THESIS_COLLECTIONS,
    IGNORED_THESIS_COLLECTIONS,
    udc_pattern,
    ALLOWED_DOCUMENT_TAGS,
)

from ...models.thesis import thesis_model as model


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
    if collection in IGNORED_THESIS_COLLECTIONS:
        raise IgnoreKey("collection")
    if collection not in ALLOWED_THESIS_COLLECTIONS:
        raise UnexpectedValue(subfield="a", key=key, value=value, field="690C_")
    if collection == "yellow report":
        subjects = self.get("subjects", [])
        subjects.append({
            "subject": collection.upper(),
        })
        self["subjects"] = subjects
    raise IgnoreKey("collection")


@model.over("publication_date", "(^260__)|(^250__)")
def imprint_info(self, key, value):
    """Translates imprint - WARNING - also publisher and publication_date.

    In case of summer student notes this field contains only date
    but it needs to be reimplemented for the base set of rules -
    it will contain also imprint place
    """
    _custom_fields = self.get("custom_fields", {})
    imprint = _custom_fields.get("imprint:imprint", {})

    if key.startswith("250"):
        edition = StringValue(value.get("a")).parse()
        imprint["edition"] = edition
        raise IgnoreKey("publication_date")

    if key == "260__":
        publication_date_str = value.get("c")
        _publisher = value.get("b")
        place = value.get("a")
        if _publisher and not self.get("publisher"):
            self["publisher"] = _publisher
        if place:
            imprint["place"] = place
        self["custom_fields"]["imprint:imprint"] = imprint
        if publication_date_str:
            try:
                date_obj = parse(publication_date_str)
                return date_obj.strftime("%Y-%m-%d")
            except (ParserError, TypeError) as e:
                raise UnexpectedValue(
                    field=key,
                    value=value,
                    message=f"Can't parse provided publication date. Value: {publication_date_str}",
                )
    raise IgnoreKey("publication_date")


@model.over("custom_fields", "(^020__)")
def isbn(self, key, value):
    _custom_fields = self.get("custom_fields", {})
    _isbn = StringValue(value.get("a", "")).parse()
    if _isbn:
        try:
            _isbn = normalize_isbn(_isbn)
        except NotValidISBNError as e:
            raise UnexpectedValue("Not a valid ISBN.", field=key, value=value)
        thesis_fields = _custom_fields.get("imprint:imprint", {})
        thesis_fields["isbn"] = _isbn
        _custom_fields["imprint:imprint"] = thesis_fields

        ids = self.get("identifiers", [])

        new_id = {"identifier": _isbn, "scheme": "isbn"}
        if new_id not in ids:
            ids.append(new_id)
        self["identifiers"] = ids
    return _custom_fields


@model.over("identifiers", "(^022__)")
@for_each_value
def issn(self, key, value):
    _issn = StringValue(value.get("a", "")).parse()
    if _issn:
        try:
            _issn = normalize_issn(_issn)
        except NotValidISBNError as e:
            raise UnexpectedValue("Not a valid ISSN.", field=key, value=value)

        ids = self.get("identifiers", [])

        new_id = {"identifier": _issn, "scheme": "issn"}
        if new_id not in ids:
            return new_id
    raise IgnoreKey("identifiers")


@model.over("subjects", "(^080__)")
def udc(self, key, value):
    """Check 080 field. Drop UDC."""
    val_a = value.get("a")
    if val_a and re.findall(udc_pattern, val_a):
        raise IgnoreKey("identifiers")
    raise UnexpectedValue(
        "UDC format check failed.", field=key, subfield="a", value=value
    )


@model.over("custom_fields", "(^536__)")
def funding(self, key, value):
    _custom_fields = self.get("custom_fields", {})
    programme = value.get("a")
    _access_info = value.get("r", "").strip().lower()
    if _access_info and _access_info not in ["openaccess", "open access"]:
        raise UnexpectedValue(
            "Access information has unexpected value", field=key, value=value
        )
    # https://cerneu.web.cern.ch/fp7-projects
    is_fp7_programme = programme and programme.strip().lower() == "fp7"

    if programme and not is_fp7_programme:
        # if not fp7, then it is cern programme
        programmes = _custom_fields.get("cern:programmes", [])
        programmes.append(programme)
        _custom_fields["cern:programmes"] = programmes
        return _custom_fields
    elif "f" in value or "c" in value:
        awards = self.get("funding", [])
        # this one is reliable, I checked the DB
        try:
            _funding = value.get("f", "").strip().lower()
            _grant_number = value.get("c", "").strip().lower()
        except AttributeError as e:
            raise UnexpectedValue(
                "Multiple grant numbers must be in separate tag", field=key, value=value
            )
        award = {
            "award": {"id": f"00k4n6c32::{_grant_number}"},
            "funder": {"id": "00k4n6c32"},
        }
        if award not in awards:
            awards.append(award)
        self["funding"] = awards
    raise IgnoreKey("custom_fields")


@model.over("custom_fields", "(^269__)")
@for_each_value
def defense_date(self, key, value):
    """Translates defence date."""
    _custom_fields = self.get("custom_fields", {})
    thesis_fields = _custom_fields.get("thesis:thesis", {})
    defense_date = value.get("c", "")
    try:
        defense_date = str(parse_edtf(defense_date))
    except EDTFParseException:
        defense_date = str(text_to_edtf(defense_date))
    if not defense_date:
        raise UnexpectedValue("Not possible to extract defense date.",
                              field=key,
                              value=value)
    thesis_fields["date_defended"] = defense_date
    _custom_fields["thesis:thesis"] = thesis_fields
    self["custom_fields"] = _custom_fields
    raise IgnoreKey("custom_fields")


@model.over("custom_fields", "(^502__)")
def thesis(self, key, value):
    """Translates custom thesis fields."""
    _custom_fields = self.get("custom_fields", {})
    thesis_fields = _custom_fields.get("thesis:thesis", {})
    th_type = StringValue(value.get("a", "")).parse()
    val_b = value.get("b", "")
    if type(val_b) is tuple:
        val_b = ",".join(val_b)
    uni = StringValue(val_b).parse()
    uni = uni.replace("U.", "University")

    thesis_fields["type"] = th_type
    thesis_fields["university"] = uni

    submission_date = value.get("c")
    if submission_date:
        # make it edtf compliant
        is_curator_info = "[" in submission_date
        submission_date = submission_date.replace("-?", "~/").replace("[", "").replace(
            "]", "")
        if is_curator_info:
            dates = self.get("dates", [])
            submission_date = submission_date.replace("?", "")
            date = {
                "description": "Date provided by curator, not found on the resource",
                "date": submission_date, "type": {
                    "id": "submitted"}}
            dates.append(date)
            self["dates"] = dates
        else:
            try:
                submission_date = parse_edtf(submission_date)
                submission_date = str(submission_date)
                thesis_fields["date_submitted"] = submission_date
            except (ParserError, TypeError, EDTFParseException) as e:
                raise UnexpectedValue(
                    field=key,
                    value=value,
                    message=f"Can't parse provided submission date. Value: {submission_date}, {str(e)}",
                )

    _custom_fields["thesis:thesis"] = thesis_fields
    return _custom_fields


@model.over("custom_fields", "(^773__)")
def journal(self, key, value):
    _custom_fields = self.get("custom_fields", {})
    journal_fields = _custom_fields.get("journal:journal", {})
    year = StringValue(value.get("y", "")).parse()
    meeting_fields = ["p", "n", "v", "c"]
    is_journal_year = False
    for field in meeting_fields:
        if field in value:
            is_journal_year = True
            break

    pub_date = self.get("publication_date")
    # if we only have 773 in the record and no other journal fields,
    # it is not journal date
    if not is_journal_year and "y" in value and not pub_date:
        self["publication_date"] = year

    journal_fields["title"] = StringValue(value.get("p", "")).parse()
    journal_fields["issue"] = StringValue(value.get("n", "")).parse()
    journal_fields["volume"] = StringValue(value.get("v", "")).parse()
    journal_fields["pages"] = StringValue(value.get("c", "")).parse()

    _custom_fields["journal:journal"] = journal_fields
    return _custom_fields


@model.over("additional_titles", "(^246__)")
@for_each_value
@require(["a"])
def additional_titles(self, key, value):
    """Translates additional titles."""
    description_text = value.get("a")
    translated_subtitle = value.get("b")
    source = value.get("9")

    if source:
        _additional_title = {
            "title": description_text,
            "type": {
                "id": "alternative-title",
            },
        }
    else:
        _additional_title = {
            "title": description_text,
            "type": {
                "id": "translated-title",
            },
        }
    if _additional_title:
        return _additional_title
    if translated_subtitle:
        _additional_title = {
            "title": translated_subtitle,
            "type": {
                "id": "translated-title",
            },
            "lang": {"id": "eng"}
        }
        return _additional_title
    raise IgnoreKey("additional_titles")


@model.over("dates", "(^500__)")
@for_each_value
def dates(self, key, value):
    text = value.get("a", "")
    source = value.get("9", "")
    # redundant information from arxiv
    if source.lower() == "arxiv":
        raise IgnoreKey("dates")

    cleaned_text = None
    ignored_words = [
        "presented on",
        "Presented on",
        "Presented",
        "presented",
        "presented in",
        "Presented in",
    ]
    for ignored in ignored_words:
        text = text.replace(ignored, "")

    try:
        parsed_date, remaining_text = parse(text, fuzzy_with_tokens=True)
        defense_date = parsed_date.date().isoformat()
        thesis_field = self.get("custom_fields", {}).get("thesis:thesis", {})
        if "date_defended" not in thesis_field:
            # normally it would come from 269 field so we can skip
            thesis_field["date_defended"] = defense_date
            thesis_field["date_defended"] = defense_date
            self["custom_fields"]["thesis:thesis"] = thesis_field
        cleaned_text = " ".join(remaining_text).strip()
    except ParserError as e:
        pass  # string does not contain a date

    if cleaned_text:
        cleaned = re.sub(r"\W+", " ", text)
        cleaned = cleaned.strip()
        if cleaned:
            internal_notes = self.get("internal_notes", [])
            internal_notes.append({"note": cleaned_text})
            if internal_notes:
                self["internal_notes"] = internal_notes

    # warning, values are assigned implicitly to self
    raise IgnoreKey("dates")


@model.over("internal_notes", "^595__")
@for_each_value
def note(self, key, value):
    """Translates notes."""

    def process(_note):
        if _note:
            if _note.strip().lower() in [
                "cern invenio websubmit",
                "cern eds",
                "cds",
                "lanl eds",
                "clas1",
            ]:
                return
            return {"note": _note}

    _note = force_list(value.get("a", ""))
    _note_z = force_list(value.get("z", ""))
    notes_list = _note_z + _note
    _note_b = value.get("b", "")
    _note_c = value.get("c", "")

    is_gensbm_tag = (
        "".join(_note).strip() == "CERN Invenio WebSubmit"
        and _note_b.strip() in ("GENSBM", "GENEU")
        or _note_c.strip() == "1"
    )
    if is_gensbm_tag:
        raise IgnoreKey("internal_notes")
    elif (
        _note_b.strip() not in ("GENSBM", "GENEU")
        and "".join(_note).strip() == "CERN Invenio WebSubmit"
    ):
        raise UnexpectedValue("invalid internal notes", field=key, value=value)

    notes = self.get("internal_notes", [])
    for item in notes_list:
        res = process(item)
        if res:
            notes.append(res)

    self["internal_notes"] = notes
    raise IgnoreKey("internal_notes")


@model.over("affiliations", "^562__")
@for_each_value
def internal_notes(self, key, value):
    """Translate internal notes"""
    note = value.get('c', "")
    return {"note": note}


@model.over("affiliations", "^901__")
@for_each_value
def collection(self, key, value):
    affiliation = value.get("u", "")
    affiliation = affiliation.replace("U.", "University")
    uni = self.get("custom_fields", {}).get("thesis:thesis", {}).get("university")
    if uni != affiliation:
        raise UnexpectedValue(
            f"Record affiliation (901: {affiliation}) not equal with thesis university 502:{uni}"
        )
    raise IgnoreKey("affiliations")


@model.over("related_identifiers", "^962_")
@for_each_value
def related_identifiers(self, key, value):
    """Translates related identifiers."""
    recid = value.get("b")
    material = value.get("n", "").lower().strip()
    rel_ids = self.get("related_identifiers", [])
    res_type = None
    if material and material == "book":
        # if book we know that is published in a book,
        res_type = "publication-book"
    elif material:
        #  otherwise it will be a conference reference
        res_type = "event"
    new_id = {
        "identifier": f"https://cds.cern.ch/records/{recid}",
        "scheme": "url",
        "relation_type": {"id": "references"},
    }

    if res_type:
        new_id.update({"resource_type": {"id": res_type}})

    if new_id not in rel_ids:
        return new_id
    raise IgnoreKey("related_identifiers")


@model.over("collection", "^980__")
@for_each_value
def collection(self, key, value):
    col = value.get("a", "")
    colb = value.get("b", "")
    if type(col) != str or type(colb) != str:
        raise UnexpectedValue("Unexpected collection found", field=key, value=value)
    if col and col.lower() not in ALLOWED_DOCUMENT_TAGS:
        raise UnexpectedValue("Unexpected collection found", field=key, value=value)
    if colb and colb.lower() not in ALLOWED_DOCUMENT_TAGS:
        raise UnexpectedValue("Unexpected collection found", field=key, value=value)
    raise IgnoreKey("collection")
