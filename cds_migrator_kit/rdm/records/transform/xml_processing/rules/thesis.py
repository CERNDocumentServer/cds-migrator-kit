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

from dateutil.parser import ParserError, parse
from dojson.errors import IgnoreKey
from idutils.normalizers import normalize_isbn
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
from ...config import ALLOWED_THESIS_COLLECTIONS, IGNORED_THESIS_COLLECTIONS, \
    udc_pattern

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
    if value.get("a").strip().lower() not in ALLOWED_THESIS_COLLECTIONS:
        raise UnexpectedValue(subfield="a", key=key, value=value, field="690C_")
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

    if key.startswith("260"):
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


@model.over("subjects", "(^080__)")
def udc(self, key, value):
    """Check 080 field. Drop UDC."""
    val_a = value.get("a")
    if val_a and re.findall(udc_pattern, val_a):
        raise IgnoreKey("identifiers")
    raise UnexpectedValue("UDC format check failed.", field=key, subfield="a",
                          value=value)


@model.over("custom_fields", "(^536__)")
def funding(self, key, value):
    _custom_fields = self.get("custom_fields", {})
    programme = value.get("a")
    # https://cerneu.web.cern.ch/fp7-projects
    is_fp7_programme = programme and programme.strip().lower() == "fp7"
    if not is_fp7_programme:
        # if not fp7, then it is cern programme
        programmes = _custom_fields.get("cern:programmes", [])
        programmes.append(programme)
        _custom_fields["cern:programmes"] = programmes
        return _custom_fields
    else:
        awards = self.get("awards", [])
        # this one is reliable, I checked the DB
        try:
            _funding = value.get("f", "").strip().lower()
            _grant_number = value.get("c", "").strip().lower()
        except AttributeError as e:
            raise UnexpectedValue("Multiple grant numbers must be in separate tag",
                                  field=key, value=value)

        # TODO decide about this field
        _access_info = value.get("r", "").strip().lower()
        award = {"id": f"00k4n6c32::{_grant_number}"}
        if award not in awards:
            awards.append(award)
        self["awards"] = awards
    raise IgnoreKey("custom_fields")


@model.over("custom_fields", "(^502__)")
def thesis(self, key, value):
    """Translates custom thesis fields."""
    _custom_fields = self.get("custom_fields", {})
    thesis_fields = _custom_fields.get("thesis:thesis", {})
    dates = self.get("dates", [])

    th_type = StringValue(value.get("a", "")).parse()
    val_b = value.get("b", "")
    if type(val_b) is tuple:
        val_b = ",".join(val_b)
    uni = StringValue(val_b).parse()

    thesis_fields["type"] = th_type
    thesis_fields["university"] = uni

    _custom_fields["thesis:thesis"] = thesis_fields

    submission_date = value.get("c")
    if submission_date:
        try:
            submission_date = parse(submission_date)
            dates.append({
                "date": submission_date.date().isoformat(),
                "type": {
                    "id": "submitted"}
            })
        except (ParserError, TypeError) as e:
            raise UnexpectedValue(
                field=key,
                value=value,
                message=f"Can't parse provided submission date. Value: {submission_date}",
            )

    self["dates"] = dates
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
    raise IgnoreKey("additional_titles")


@model.over("dates", "(^500__)")
@for_each_value
def dates(self, key, value):
    output_dates = self.get("dates", [])
    text = value.get("a", "")
    source = value.get("9", "")
    # redundant information from arxiv
    if source.lower() == "arxiv":
        raise IgnoreKey("dates")

    cleaned_text = None
    ignored_words = ["presented on", "Presented on", 'Presented', 'presented',
                     'presented in', 'Presented in']
    for ignored in ignored_words:
        text = text.replace(ignored, "")

    try:
        parsed_date, remaining_text = parse(text, fuzzy_with_tokens=True)

        output_dates.append({"date": parsed_date.date().isoformat(),
                             "type": {"id": "accepted"},
                             "description": "defense date"})

        self["dates"] = output_dates
        cleaned_text = " ".join(remaining_text).strip()
    except ParserError as e:
        pass  # string does not contain a date

    if cleaned_text:
        cleaned = re.sub(r'\W+', ' ', text)
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
    _note = value.get("a", "").strip()
    if _note:
        if _note.lower() in ["cern invenio webubmit", "cern eds", "cds", "lanl eds",
                             "clas1"]:
            raise IgnoreKey("internal_notes")
        return {"note": note}
