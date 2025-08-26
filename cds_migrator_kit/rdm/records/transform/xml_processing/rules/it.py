import math
import re

from dateutil.parser import ParserError, parse
from dojson.errors import IgnoreKey

from cds_migrator_kit.errors import MissingRequiredField, UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.decorators import (
    for_each_value,
    require,
)
from cds_migrator_kit.transform.xml_processing.quality.parsers import StringValue

from ...config import IGNORED_THESIS_COLLECTIONS
from ...models.it import it_model as model
from .base import subjects as base_subjects
from .base import urls
from .publications import journal as base_journal
from .publications import related_identifiers as base_related_identifiers


@model.over("resource_type", "^980__", override=True)
def resource_type(self, key, value):
    """Translates resource_type."""
    value_a = value.get("a")
    value_b = value.get("b")

    priority = {
        v: i
        for i, v in enumerate(
            [
                "intnoteitpubl",
                "intnotetspubl",
                "note",
                "itcerntalk",
                "preprint",
                "bookchapter",
                "conferencepaper",
                "article",
            ]
        )
    }

    current = self.get("resource_type")

    # Normalize both values (lowercase if not None)
    candidates = []
    if value_a:
        candidates.append(value_a.lower())
    if value_b:
        candidates.append(value_b.lower())

    if not candidates:
        raise IgnoreKey("resource_type")  # nothing to decide on

    # Select the candidate with the highest priority (lowest rank)
    best_value = min(candidates, key=lambda v: priority.get(v, float("inf")))
    rank = priority.get(best_value, float("inf"))

    mapping = {
        "preprint": {"id": "publication-preprint"},
        "conferencepaper": {"id": "publication-conferencepaper"},
        "article": {"id": "publication"},
        "itcerntalk": {"id": "presentation"},
        "intnoteitpubl": {"id": "publication-technicalnote"},
        "bookchapter": {"id": "publication-section"},
        "cnlissue": {"id": "publication-article"},
        "report": {"id": "publication-report"},
        "poster": {"id": "poster"},
    }

    if current:
        current_key = next((k for k, v in mapping.items() if v == current), None)
        current_rank = priority.get(current_key, float("inf"))

        if rank < current_rank:
            try:
                return mapping[best_value]
            except KeyError:
                raise UnexpectedValue(
                    "Unknown resource type (IT)", value=best_value, field=key
                )
        else:
            raise IgnoreKey("resource_type")
    else:
        try:
            return mapping[best_value]
        except KeyError:
            raise UnexpectedValue(
                "Unknown resource type (IT)", value=best_value, field=key
            )


@model.over("custom_fields", "^111__")
def meeting(self, key, value):
    _custom_fields = self.get("custom_fields", {})
    meeting_fields = _custom_fields.get("meeting:meeting", {})
    meeting_fields["title"] = StringValue(value.get("a", "")).parse()
    meeting_fields["place"] = StringValue(value.get("c", "")).parse()
    meeting_fields["dates"] = StringValue(value.get("d", "")).parse()
    if value.get("u", ""):
        identifier = {
            "identifier": StringValue(value.get("u", "")).parse(),
            "scheme": "url",
        }
        meeting_fields.setdefault("identifiers", []).append(identifier)

    _custom_fields["meeting:meeting"] = meeting_fields
    return _custom_fields


@model.over("additional_descriptions", "(^500__)")
@for_each_value
@require(["a"])
def additional_descriptions(self, key, value):
    """Translates additional description."""
    description_text = StringValue(value.get("a")).parse()
    if description_text:
        _additional_description = {
            "description": description_text,
            "type": {
                "id": "other",
            },
        }
        return _additional_description
    raise IgnoreKey("additional_descriptions")


@model.over("collection", "^690C_")
@for_each_value
def collection(self, key, value):
    """Translates collection field."""
    collection = value.get("a").strip().lower()
    if collection in ["article", "cern"]:
        raise IgnoreKey("collection")
    if collection == "yellow report":
        subjects = self.get("subjects", [])
        subjects.append(
            {
                "subject": collection.upper(),
            }
        )
        self["subjects"] = subjects
    raise IgnoreKey("collection")


@model.over("related_works", "^7870_", override=True)
@for_each_value
def related_works(self, key, value):
    """Handles related identifiers from 7870_ with deduplication."""

    related_identifiers = self.get("related_identifiers", [])
    description = value.get("i")

    def add_identifier(identifier_value, scheme):
        if not identifier_value:
            return
        new_entry = {
            "identifier": identifier_value,
            "scheme": scheme,
            "relation_type": {"id": "references"},
        }
        if new_entry not in related_identifiers:
            related_identifiers.append(new_entry)

    # Add 'w' field as LCDS
    add_identifier(value.get("w"), "lcds")

    # Add 'r' field as CDS reference
    add_identifier(value.get("r"), "cds_ref")

    self["related_identifiers"] = related_identifiers
    raise IgnoreKey("related_works")


@model.over("contributors", "^110__")
@for_each_value
def corporate_author(self, key, value):
    """Translates corporate author."""
    if "a" in value:
        name = value.get("a")
        if name.strip() == "CERN. Geneva":
            name = "CERN"
        contributor = {
            "person_or_org": {
                "type": "organizational",
                "name": StringValue(name).parse(),
                "family_name": StringValue(name).parse(),
            },
            "role": {"id": "hostinginstitution"},
        }
        return contributor
    raise IgnoreKey("contributors")


@model.over("meeting", "(^773__)", override=True)
@for_each_value
def meeting(self, key, value):
    """Translates additional description."""
    _custom_fields = self.get("custom_fields", {})
    meeting_fields = _custom_fields.get("meeting:meeting", {})
    if value.get("t"):
        meeting_fields["place"] = StringValue(value.get("t", "")).parse()
        _custom_fields["meeting:meeting"] = meeting_fields
    if value.get("w"):
        cnum = value.get("w", "")
        cnum_regexp = re.compile(r"(?:\d+$|[A-Z]\d{2}-\d{2}-\d{2}\.\d+)", flags=re.I)
        if cnum_regexp.match(cnum):
            identifier = {
                "identifier": StringValue(value.get("w", "")).parse(),
                "scheme": "url",
            }
            meeting_fields.setdefault("identifiers", []).append(identifier)

    journal_info = (base_journal(self, key, value)).get("journal:journal", {})
    existing_journal = _custom_fields.get("journal:journal", {})
    existing_journal.update(journal_info)
    _custom_fields["journal:journal"] = existing_journal

    raise IgnoreKey("meeting")


@model.over("custom_fields", "(^250__)")
@for_each_value
@require(["a"])
def imprint(self, key, value):
    """Translates additional description."""
    _custom_fields = self.setdefault("custom_fields", {})
    imprint = _custom_fields.setdefault("imprint:imprint", {})
    imprint["edition"] = StringValue(value.get("a")).parse()
    raise IgnoreKey("custom_fields")


@model.over("notes", "^8564_", override=True)
@for_each_value
def notes(self, key, value):
    """Translate internal notes"""
    url = value.get("u", "")
    note = StringValue(value.get("z", "")).parse()
    if url:
        identifiers = self.get("identifiers", [])
        url_entries = urls(self, key, value)
        for entry in url_entries:
            if entry not in identifiers:
                identifiers.append(entry)
        self["identifiers"] = identifiers

    elif note:
        _internal_notes = self.get("internal_notes", [])
        _internal_notes.append(note)
        self["internal_notes"] = _internal_notes
    raise IgnoreKey("notes")


@model.over(
    "subjects", "(^6931_)|(^650[12_][7_])|(^653[12_]_)|(^695__)|(^694__)", override=True
)
@require(["a"])
@for_each_value
def subjects(self, key, value):
    """Translates subjects fields."""
    val_a = value.get("a", "")
    scheme = value.get("2", "")
    _subjects = self.get("subjects", [])

    if val_a == "Talk" or val_a == "Lecture":
        subject = {"subject": val_a}
        _subjects.append(subject)
        self["subjects"] = _subjects
        raise IgnoreKey("subjects")

    if (key.startswith(("694_", "695_"))) and value.get("9") == "DESY":
        raise IgnoreKey("subjects")

    if val_a == "XX":
        raise IgnoreKey("subjects")
    else:
        base_subjects(self, key, value)
    raise IgnoreKey("subjects")


@model.over("related_identifiers_and_imprint", "^962_", override=True)
@for_each_value
def related_identifiers_and_imprint(self, key, value):
    """Translates related identifiers and safely updates imprint pages."""
    # Imprint pages
    _custom_fields = self.setdefault("custom_fields", {})
    imprint = _custom_fields.setdefault("imprint:imprint", {})
    k_value = value.get("k")
    if k_value:
        parsed_pages = StringValue(k_value).parse()
        imprint["pages"] = parsed_pages
    _custom_fields["imprint:imprint"] = imprint

    # Related identifiers
    rel_ids = self.setdefault("related_identifiers", [])
    for new_id in base_related_identifiers(self, key, value):
        if new_id not in rel_ids:
            rel_ids.append(new_id)
    self["related_identifiers"] = rel_ids

    raise IgnoreKey("related_identifiers_and_imprint")


@model.over("contributors", "^906__")
@for_each_value
def supervisor(self, key, value):
    """Translates supervisor."""
    supervisor = StringValue(value.get("p")).parse()
    if not supervisor:
        raise MissingRequiredField(field=key, subfield="p", priority="warning")
    contributor = {
        "person_or_org": {
            "type": "personal",
            "name": supervisor,
            "family_name": supervisor,
        },
        "role": {"id": "supervisor"},
    }

    return contributor


@model.over("dates", "(^269__)", override=True)
@for_each_value
def imprint_dates(self, key, value):
    """Translates imprint - WARNING - also publisher and publication_date."""

    def format_date(date_str, date_obj):
        parts = date_str.strip().split()
        if len(parts) == 1:  # Only year
            return date_obj.strftime("%Y")
        elif len(parts) == 2:  # Month + year
            return date_obj.strftime("%Y-%m")
        else:  # Full date
            return date_obj.strftime("%Y-%m-%d")

    _custom_fields = self.setdefault("custom_fields", {})
    imprint = _custom_fields.setdefault("imprint:imprint", {})

    publication_date_str = value.get("c")
    _publisher = value.get("b")
    place = value.get("a")

    if _publisher and not self.get("publisher"):
        self["publisher"] = _publisher
    if place:
        imprint["place"] = place

    _custom_fields["imprint:imprint"] = imprint
    self["custom_fields"] = _custom_fields

    if publication_date_str:
        try:
            if "?" in publication_date_str:
                # Strip '?' and record as indeterminate
                cleaned_date = publication_date_str.replace("?", "").strip()
                date_obj = parse(cleaned_date)
                date = {
                    "description": "The publication date is indeterminate.",
                    "date": format_date(cleaned_date, date_obj),
                    "type": {"id": "other"},
                }
                self.setdefault("dates", []).append(date)
                self["publication_date"] = str(date_obj.year)
            else:
                date_obj = parse(publication_date_str)
                self["publication_date"] = date_obj.strftime("%Y-%m-%d")
        except (ParserError, TypeError) as e:
            raise UnexpectedValue(
                field=key,
                value=value,
                message=f"Can't parse provided publication date. Value: {publication_date_str}",
            )

    raise IgnoreKey("dates")
