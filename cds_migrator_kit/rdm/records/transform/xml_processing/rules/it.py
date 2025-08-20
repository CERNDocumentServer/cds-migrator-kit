from dateutil.parser import ParserError, parse
from dojson.errors import IgnoreKey
import math
import re
from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.decorators import for_each_value, require
from cds_migrator_kit.transform.xml_processing.quality.parsers import StringValue
from .publications import journal, related_identifiers
from .base import urls, subjects
from ...config import IGNORED_THESIS_COLLECTIONS
from ...models.it import it_model as model


@model.over("resource_type", "^980__", override=True)
def resource_type(self, key, value):
    """Translates resource_type."""
    value_a = value.get("a")
    value_b = value.get("b")

    priority = {
        v: i for i, v in enumerate([
            "intnoteitpubl", "intnotetspubl", "note", "itcerntalk",
            "preprint", "bookchapter", "conferencepaper", "article"
        ])
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
        "article": {"id": "publication-article"},
        "itcerntalk": {"id": "presentation"},
        "note": {"id": "publication-technicalnote"},
        "intnotebepubl": {"id": "publication-technicalnote"},
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
                raise UnexpectedValue("Unknown resource type (IT)", value=best_value, field=key)
        else:
            raise IgnoreKey("resource_type")
    else:
        try:
            return mapping[best_value]
        except KeyError:
            raise UnexpectedValue("Unknown resource type (IT)", value=best_value, field=key)


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
    description_text = value.get("a")
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
    """Handles related identifiers from 7870_."""

    related_works = self.get("related_identifiers", [])
    description = value.get("i")

    if value.get("w"):
        related_works.append({
            "identifier": value["w"],
            "scheme": "lcds",
            "relation_type": {"id": "references"},
        })

    if value.get("r"):
        related_works.append({
            "identifier": value["r"],
            "scheme": "cds_ref",
            "relation_type": {"id": "references"},
        })

    self["related_identifiers"] = related_works

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
        cnum_regexp = re.compile(
            r"(?:\d+$|[A-Z]\d{2}-\d{2}-\d{2}\.\d+)", flags=re.I
        ) 
        if cnum_regexp.match(cnum):
            identifier = { 
                "identifier": StringValue(value.get("w", "")).parse(),
                "scheme": "url",
            }
            meeting_fields.setdefault("identifiers", []).append(identifier)
    else:
        journal(self, key, value)
    raise IgnoreKey("meeting")

@model.over("custom_fields", "(^250__)")
@for_each_value
@require(["a"])
def imprint(self, key, value):
    """Translates additional description."""
    _custom_fields = self.get("custom_fields", {})
    imprint = _custom_fields.get("imprint:imprint", {})
    imprint["edition"] = StringValue(value.get("a")).parse()
    raise IgnoreKey("custom_fields")

@model.over("notes", "^8564_", override=True)
@for_each_value
def notes(self, key, value):
    """Translate internal notes"""
    url = value.get("u", "")
    note = StringValue(value.get("z", "")).parse()
    if url:
        urls(self, key, value)
    elif note:
        _internal_notes = self.get("internal_notes", [])
        _internal_notes.append(note)  
        self["internal_notes"] = _internal_notes
    raise IgnoreKey("notes")

@model.over("it_subjects", "(^6931_)|(^650[12_][7_])|(^653[12_]_)|(^695__)|(^694__)", override=True)
@require(["a"])
@for_each_value
def it_subjects(self, key, value):
    """Translates subjects fields."""
    val_a = value.get("a", "")
    scheme = value.get("2", "")

    if val_a == "Talk":
        subject = {"subject": val_a}
        _subjects = self.get("subjects", [])
        _subjects.append(subject)
        self["subjects"] = _subjects
        raise IgnoreKey("it_subjects")
    
    if (key.startswith(("694_", "695_"))) and value.get("9") == "DESY":
        raise IgnoreKey("it_subjects")

    if val_a == "XX":
        raise IgnoreKey("it_subjects")  
    else:
        subjects(self, key, value)
    raise IgnoreKey("it_subjects")  
    

@model.over("related_identifiers", "^962_", override=True)
@for_each_value
def related_identifiers(self, key, value):
    """Translates related identifiers."""
    if value.get("k"):
        _custom_fields = self.get("custom_fields", {})
        meeting_fields = _custom_fields.get("imprint:imprint", {})
        meeting_fields["pages"] = StringValue(value.get("k", "")).parse()
        _custom_fields["imprint:imprint"] = meeting_fields
    else: 
        related_identifiers(self, key, value)
    raise IgnoreKey("related_identifiers")