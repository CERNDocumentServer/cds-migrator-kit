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
from dojson.utils import force_list
from edtf import EDTFParseException, parse_edtf, text_to_edtf
from idutils.normalizers import normalize_isbn, normalize_issn
from isbnlib import NotValidISBNError

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.rdm.records.transform.models.thesis import thesis_model as model
from cds_migrator_kit.transform.xml_processing.quality.decorators import (
    filter_list_values,
    for_each_value,
    require,
    strip_output,
)
from cds_migrator_kit.transform.xml_processing.quality.parsers import StringValue
from cds_migrator_kit.transform.xml_processing.rules.base import process_contributors

from ...config import (
    ALLOWED_DOCUMENT_TAGS,
    ALLOWED_THESIS_COLLECTIONS,
    FORMER_COLLECTION_TAGS_TO_KEEP,
    IGNORED_THESIS_COLLECTIONS,
    udc_pattern,
)
from .base import normalize


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
        raise UnexpectedValue(subfield="a", value=value, field=key)
    if collection == "yellow report":
        subjects = self.get("subjects", [])
        subjects.append(
            {
                "subject": collection.upper(),
            }
        )
        self["subjects"] = subjects
    raise IgnoreKey("collection")


@model.over("publication_date", "(^260__)|(^250__)", override=True)
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
                publication_date = normalize(publication_date_str)
                return publication_date
            except (ParserError, TypeError) as e:
                raise UnexpectedValue(
                    field=key,
                    value=value,
                    message=f"Can't parse provided publication date. Value: {publication_date_str}",
                )
    raise IgnoreKey("publication_date")


@model.over("custom_fields", "(^269__)")
@for_each_value
def defense_date(self, key, value):
    """Translates defence date."""
    _custom_fields = self.get("custom_fields", {})
    thesis_fields = _custom_fields.get("thesis:thesis", {})
    defense_date = value.get("c", "")
    try:
        parsed_date = parse(defense_date)
        defense_date = parsed_date.date().isoformat()
        defense_date = str(parse_edtf(defense_date))
    except (EDTFParseException, ParserError) as e:
        defense_date = text_to_edtf(defense_date)
    if not defense_date:
        try:
            parsed_date = parse(value.get("c", ""))
            defense_date = parsed_date.date().isoformat()
        except (EDTFParseException, ParserError) as e:
            defense_date = None
    if not defense_date:
        try:
            parsed_date = parse(value.get("c", ""), dayfirst=True)
            defense_date = parsed_date.date().isoformat()
        except (EDTFParseException, ParserError) as e:
            raise UnexpectedValue(
                "Not possible to extract defense date.", field=key, value=value
            )
    thesis_fields["date_defended"] = defense_date
    _custom_fields["thesis:thesis"] = thesis_fields
    self["custom_fields"] = _custom_fields
    raise IgnoreKey("custom_fields")


@model.over("thesis_custom_fields", "(^502__)")
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
        submission_date = (
            submission_date.replace("-?", "~/").replace("[", "").replace("]", "")
        )
        if is_curator_info:
            dates = self.get("dates", [])
            submission_date = submission_date.replace("?", "")
            date = {
                "description": "Date provided by curator, not found on the resource",
                "date": submission_date,
                "type": {"id": "submitted"},
            }
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
    self["custom_fields"] = _custom_fields
    raise IgnoreKey("thesis_custom_fields")


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
        if "date_defended" not in thesis_field or not thesis_field["date_defended"]:
            # normally it would come from 269 field so we can skip
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


@model.over("funding", "(^536__)", override=True)
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
        _custom_fields["cern:programmes"] = programme
        self["custom_fields"] = _custom_fields
        raise IgnoreKey("funding")
    if programme and "f" in value or "c" in value:
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
    else:
        raise UnexpectedValue("Unexpected grant value", field=key, value=value)
    raise IgnoreKey("funding")


@model.over("affiliations", "^901__")
@for_each_value
def rec_affiliation(self, key, value):
    affiliation = value.get("u", "")
    if type(affiliation) is not str:
        raise UnexpectedValue(f"Record affiliation has a wrong format.")
    affiliation = affiliation.replace("U.", "University")
    uni = self.get("custom_fields", {}).get("thesis:thesis", {}).get("university")
    if uni != affiliation:
        raise UnexpectedValue(
            f"Record affiliation (901: {affiliation}) not equal with thesis university 502:{uni}"
        )
    raise IgnoreKey("affiliations")


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
    if col and col.lower() in FORMER_COLLECTION_TAGS_TO_KEEP:
        subjects = self.get("subjects", [])
        subjects.append({"subject": f"collection:{col.upper()}"})
        self["subjects"] = subjects
    if colb and colb.lower() in FORMER_COLLECTION_TAGS_TO_KEEP:
        subjects = self.get("subjects", [])
        subjects.append({"subject": f"collection:{colb.upper()}"})
        self["subjects"] = subjects
    raise IgnoreKey("collection")


@model.over("related_identifiers", "^962_")
@for_each_value
def related_identifiers(self, key, value):
    """Translates related identifiers."""
    recid = value.get("b")
    try:
        material = value.get("n", "").lower().strip()
    except AttributeError:
        raise UnexpectedValue(
            "related identifiers have unexpected material format",
            field=key,
            value=value,
        )
    rel_ids = self.get("related_identifiers", [])
    res_type = None
    if material and material == "book":
        # if book we know that is published in a book,
        res_type = "publication-book"
    elif material:
        #  otherwise it will be a conference reference
        res_type = "event"
    new_id = {
        "identifier": recid,
        "scheme": "lcds",
        "relation_type": {"id": "references"},
    }

    artid = value.get("k", "")
    if artid:
        artid_from_773 = (
            self.get("custom_fields", {}).get("journal:journal", {}).get("pages")
        )
        if artid_from_773 != artid:
            raise UnexpectedValue(
                message="Ambiguous journal information - not equal with 773",
                field=key,
                value=artid,
                subfield="k",
            )

    if res_type:
        new_id.update({"resource_type": {"id": res_type}})

    if new_id not in rel_ids:
        return new_id
    raise IgnoreKey("related_identifiers")
