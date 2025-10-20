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
from .base import normalize
from .base import note as base_internal_notes
from .base import subjects as base_subjects
from .base import urls
from .base import yellow_reports as base_yellow_reports
from .publications import journal as base_journal
from .publications import related_identifiers as base_publication_identifiers


@model.over("resource_type", "^980__", override=True)
def resource_type(self, key, value):
    """Translates resource_type."""
    value_a = value.get("a", "")
    if value_a and value_a.lower() in ["publarda"]:
        raise IgnoreKey("resource_type")

    value_b = value.get("b", "")

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
        "note": {"id": "publication-technicalnote"},
        "brochure": {"id": "other"},
        "itcerntalk": {"id": "presentation"},
        "peri": {"id": "publication-periodicalissue"},
        "intnoteitpubl": {"id": "publication-technicalnote"},
        "intnotetspubl": {"id": "publication-technicalnote"},
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
    if collection in ("yellow report", "yellowrepcontrib"):
        subjects = self.get("subjects", [])
        subjects.append(
            {
                "subject": collection.upper(),
            }
        )
        self["subjects"] = subjects
    if collection == "publarda":
        _custom_fields = self.get("custom_fields", {})
        projects = _custom_fields.get("cern:projects", [])
        projects.append("ARDA")
        _custom_fields["cern:projects"] = list(set(projects))
        self["custom_fields"] = _custom_fields
    raise IgnoreKey("collection")


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
    _custom_fields = self.setdefault("custom_fields", {})
    meeting_fields = _custom_fields.get("meeting:meeting", {})
    if value.get("t"):
        meeting_fields["place"] = StringValue(value.get("t", "")).parse()
        _custom_fields["meeting:meeting"] = meeting_fields
    _custom_fields["meeting:meeting"] = meeting_fields

    journal_info = (base_journal(self, key, value)).get("journal:journal", {})
    existing_journal = _custom_fields.get("journal:journal", {})
    existing_journal.update(journal_info)
    _custom_fields["journal:journal"] = existing_journal
    self["custom_fields"] = _custom_fields

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
        related_identifiers = self.get("related_identifiers", [])
        url_entries = urls(self, key, value)
        for entry in url_entries:
            if entry not in related_identifiers:
                related_identifiers.append(entry)
        self["related_identifiers"] = related_identifiers

    elif note:
        _internal_notes = self.get("internal_notes", [])
        _internal_notes.append(note)
        self["internal_notes"] = _internal_notes
    raise IgnoreKey("notes")


@model.over(
    "subjects",
    "(^6931_)|(^650[12_][7_])|(^653[12_]_)|(^695__)|(^694__)|(^69531_)",
    override=True,
)
@require(["a"])
@for_each_value
def subjects(self, key, value):
    """Translates subjects fields."""
    val_a = value.get("a", "")
    val_9 = value.get("9", "")
    scheme = value.get("2", "")
    _subjects = self.get("subjects", [])

    if val_a == "Talk" or val_a == "Lecture":
        subject = {"subject": val_a}
        _subjects.append(subject)
        self["subjects"] = _subjects
        raise IgnoreKey("subjects")

    if (key.startswith(("694_", "695_"))) and value.get("9") == "DESY":
        raise IgnoreKey("subjects")

    if key.startswith("695_") and val_9 == "JACoW":
        self["subjects"].extend([{"subject": val_a}, {"subject": val_9}])
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
    for new_id in base_publication_identifiers(self, key, value):
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

    # --- imprint metadata ---
    _cf = self.setdefault("custom_fields", {})
    imprint = _cf.setdefault("imprint:imprint", {})

    if value.get("b") and not self.get("publisher"):
        self["publisher"] = value["b"]
    if value.get("a"):
        imprint["place"] = value["a"]
    _cf["imprint:imprint"] = imprint

    pub = value.get("c")
    if not pub:
        raise IgnoreKey("dates")

    try:
        if "?" in pub:
            pub = pub.replace("?", "").rstrip("-").strip()
            self.setdefault("dates", []).append(
                {
                    "description": "The publication date is indeterminate.",
                    "date": normalize(pub),
                    "type": {"id": "created"},
                }
            )
        self["publication_date"] = normalize(pub)
    except (ParserError, TypeError):
        raise UnexpectedValue(
            field=key,
            value=value,
            message=f"Can't parse provided publication date. Value: {pub}",
        )

    raise IgnoreKey("dates")


@model.over("conference_title_and_note", "^595__", override=True)
@for_each_value
def conference_title(self, key, value):
    """Translates notes and conference meeting."""

    # --- MEETING FIELD ---
    conference_title = StringValue(value.get("d")).parse()
    if conference_title:
        _custom_fields = self.get("custom_fields", {})
        meeting = _custom_fields.get("meeting:meeting", {})
        meeting["title"] = conference_title
        _custom_fields["meeting:meeting"] = meeting

        self["custom_fields"] = _custom_fields

    # --- NOTES FIELD ---
    base_internal_notes(self, key, value)
    raise IgnoreKey("conference_title_and_note")


@model.over("additional_descriptions", "(^590__)")
@for_each_value
def translated_description(self, key, value):
    description_text = value.get("a", "")
    description_text_b = value.get("b", "")
    description_text = description_text.replace("<!--HTML-->", "").strip()
    description_text_b = description_text_b.replace("<!--HTML-->", "").strip()

    if len(description_text) > 3 or len(description_text_b) > 3:
        description_text = f"<h2>{description_text}</h2><p>{description_text_b}</p>"
    if description_text:
        _additional_description = {
            "description": description_text,
            "type": {
                "id": "other",
            },
            "lang": {"id": "fra"},
        }
        return _additional_description
    raise IgnoreKey("additional_descriptions")
