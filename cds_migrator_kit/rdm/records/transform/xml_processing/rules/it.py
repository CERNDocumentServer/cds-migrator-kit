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

from ...models.it import it_model as model
from .base import additional_titles as base_additional_titles
from .base import custom_fields_693 as base_custom_fields_693
from .base import normalize
from .base import note as base_internal_notes
from .base import subjects as base_subjects
from .base import urls
from .publications import imprint_info as base_publication_imprint_info
from .publications import journal as base_journal
from .publications import related_identifiers as base_publications_related_identifiers


@model.over("access_grants", "^506[1_]_")
@for_each_value
def access_grants(self, key, value):
    """Translates access permissions (by user email or group name)."""
    raw_identifier = value.get("d") or value.get("m") or value.get("a")
    subject_identifier = StringValue(raw_identifier).parse()
    if not subject_identifier:
        raise IgnoreKey("access_grants")

    permission_type = "view"
    return {str(subject_identifier): permission_type}


@model.over("resource_type", "(^980__)|(^697C_)", override=True)
def resource_type(self, key, value):
    """Translates resource_type."""
    value_a = value.get("a", "")
    if value_a and value_a.lower() in ["publarda"]:
        raise IgnoreKey("resource_type")

    value_b = value.get("b", "")


    # first has highest priority
    priority = {
        v: i
        for i, v in enumerate(
            [
                "note",
                "intnotetspubl",
                "intnoteitpubl",
                "preprint",
                "article",
                "slides",
                "itcerntalk",
                "bookchapter",
                "conferencepaper",
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
        "article": {"id": "publication-article"},
        "note": {"id": "publication-technicalnote"},
        "brochure": {"id": "publication-brochure"},
        "itcerntalk": {"id": "presentation"},
        "slides": {"id": "presentation"},
        "peri": {"id": "publication-periodical"},
        "intnoteitpubl": {"id": "publication-technicalnote"},
        "intnotetspubl": {"id": "publication-technicalnote"},
        "bookchapter": {"id": "publication-section"},
        "cnlissue": {"id": "publication-periodicalissue"},
        "cnlarticle": {"id": "publication-periodicalarticle"},
        "report": {"id": "publication-report"},
        "progress report": {"id": "publication-report"},
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


@model.over("meeting_info", "^111__")
def meeting(self, key, value):
    _custom_fields = self.setdefault("custom_fields", {})
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
    raise IgnoreKey("meeting_info")


@model.over("additional_descriptions", "(^500__)|(^935__)|(^210__)")
@for_each_value
@require(["a"])
def additional_descriptions(self, key, value):
    """Translates additional description."""
    if key.startswith("210"):
        base_additional_titles(self, key, value)
    else:
        description_text = StringValue(value.get("a")).parse()
        description_type = "other"
        if key.startswith("935"):
            description_type = "technical-info"
        if description_text:
            _additional_description = {
                "description": description_text,
                "type": {
                    "id": description_type,
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
                "subject": f"collection:{collection.upper()}",
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


@model.over("creators", "^110__")
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
        }
        return contributor
    raise IgnoreKey("contributors")


@model.over("meeting", "(^773__)", override=True)
@for_each_value
def meeting(self, key, value):
    """Translates additional description."""
    published_in = value.get("e", "").strip().lower()

    if published_in:
        _related_identifiers = self.setdefault("related_identifiers", [])
        _related_identifiers.append({"identifier": published_in, "scheme": "cds",
                                     "relation_type": {"id": "ispublishedin"},
                                     "resource_type": {
                                         "id": "publication-periodicalissue"}})
        self["related_identifiers"] = _related_identifiers

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


@model.over("imprint_info", "(^250__)")
@for_each_value
@require(["a"])
def imprint(self, key, value):
    """Translates additional description."""
    _custom_fields = self.setdefault("custom_fields", {})
    imprint = _custom_fields.setdefault("imprint:imprint", {})
    imprint["edition"] = StringValue(value.get("a")).parse()
    raise IgnoreKey("imprint_info")


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
    for new_id in base_publications_related_identifiers(self, key, value):
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


@model.over("custom_fields", "(^269__)|(^933__)|(^693__)", override=True)
@for_each_value
def imprint_dates(self, key, value):
    """Translates imprint - WARNING - also publisher and publication_date."""
    if key.startswith("693"):
        base_custom_fields_693(self, key, value)

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
        raise IgnoreKey("custom_fields")

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

    raise IgnoreKey("custom_fields")


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


@model.over("publication_date", "(^362__)|(^260__)", override=True)
def imprint_info(self, key, value):
    """Translates publication_date field."""
    if key.startswith("260"):
        base_publication_imprint_info(self, key, value)
    else:
        publication_date_str = value.get("a")
        if publication_date_str:
            try:
                pub_date = re.search(
                    r"\b(19|20)\d{2}(?:[-/]\d{1,2})?(?:[-/]\d{1,2})?\b",
                    publication_date_str,
                )
                if pub_date:
                    publication_date = normalize(pub_date.group(0))
                    return publication_date
            except (ParserError, TypeError) as e:
                raise UnexpectedValue(
                    field=key,
                    value=value,
                    message=f"Can't parse provided publication date. Value: {publication_date_str}",
                )
        raise IgnoreKey("publication_date")


@model.over("related_identifiers", "^785__")
@for_each_value
def related_works(self, key, value):
    """Translates related identifiers."""
    description = StringValue(value.get("i")).parse().lower()
    recid = value.get("w")
    rel_ids = self.get("related_identifiers", [])
    if "continue" in description:
        relation_type = "iscontinuedby"
    else:
        relation_type = "references"
    new_id = {
        "identifier": recid,
        "scheme": "cds",
        "relation_type": {"id": relation_type},
        "resource_type": {"id": "other"},
    }
    if new_id not in rel_ids:
        return new_id

    raise IgnoreKey("related_identifiers")


@model.over("additional_descriptions_it", "^8564[1_]", override=True)
@for_each_value
def series(self, key, value):
    """Translates additional descriptions and url."""
    content_type = value.get("x", "")
    if content_type == "icon":
        # ignore icon urls (conditionally ignoring by accessing the value)
        url_q = value.get("q", "")
        url_u = value.get("u", "")
        raise IgnoreKey("url_identifiers")

    description = StringValue(value.get("3")).parse()
    url = value.get("u", "")
    if url:
        related_identifiers = self.get("related_identifiers", [])
        url_entries = urls(self, key, value)
        for entry in url_entries:
            if entry not in related_identifiers:
                related_identifiers.append(entry)
        self["related_identifiers"] = related_identifiers
    if description:
        _additional_descriptions = self.setdefault("additional_descriptions", [])
        _additional_descriptions.append({"description": description, "type": {"id": "series-information"}})
        self["additional_descriptions"] = _additional_descriptions
    raise IgnoreKey("additional_descriptions_it")


@model.over("additional_titles", "^246_[3]")
@for_each_value
@require(["a"])
def additional_titles(self, key, value):
    """Translates additional titles."""
    description_text = value.get("a")
    if description_text:
        _additional_title = {
            "title": description_text,
            "type": {
                "id": "alternative-title",
            },
        }
        return _additional_title
    raise IgnoreKey("additional_titles")
