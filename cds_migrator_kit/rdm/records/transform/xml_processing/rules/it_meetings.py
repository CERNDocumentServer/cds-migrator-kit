import os
import pickle
from datetime import datetime
from urllib.parse import ParseResult, parse_qs, urlparse, urlunparse

from dateutil.parser import ParserError, parse
from dojson.errors import IgnoreKey

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.decorators import (
    for_each_value,
    require,
)
from cds_migrator_kit.transform.xml_processing.quality.parsers import (
    StringValue,
    clean_val,
)
from cds_migrator_kit.videos.weblecture_migration.transform.xml_processing.quality.identifiers import (
    get_new_indico_id,
)

from ...models.it_meetings import it_meetings_model as model


@model.over("resource_type", "^980__", override=True)
def resource_type(self, key, value):
    """Translates resource_type."""
    value = value.get("a")
    if not value:
        raise IgnoreKey("resource_type")
    value = value.lower()

    map = {
        "contributionsfromindico": {"id": "presentation"},
        "eventsfromindico": {"id": "event"},
    }
    try:
        return map[value]
    except KeyError:
        raise UnexpectedValue(
            "Unknown resource type (IT Meetings)", field=key, value=value
        )


@model.over("collection", "^690C_")
@for_each_value
def collection(self, key, value):
    """Translates collection field."""
    collection = value.get("a").strip().lower()
    if collection not in ["conference", "announcement"]:
        raise UnexpectedValue(subfield="a", field=key, value=value)
    subjects = self.get("subjects", [])
    subjects.append(
        {
            "subject": collection.upper(),
        }
    )
    self["subjects"] = subjects
    raise IgnoreKey("collection")


@model.over("related_identifiers", "^962__", override=True)
@for_each_value
def related_identifiers(self, key, value):
    """Translate 962 fields into related identifiers (LCDS/Indico)."""
    recid = value.get("b")

    self.setdefault("related_identifiers", []).append(
        {
            "identifier": recid,
            "scheme": "lcds",
            "relation_type": {"id": "ispartof"},
        }
    )

    raise IgnoreKey("related_identifiers")


@model.over("conference_dates", "^925__")
@for_each_value
def conference_date(self, key, value):
    """Translates dates."""
    _custom_fields = self.get("custom_fields", {})
    meeting_fields = _custom_fields.get("meeting:meeting", {})

    a_str = StringValue(value.get("a", "")).parse()
    b_str = StringValue(value.get("b", "")).parse()
    start_date = datetime.fromisoformat(a_str)
    end_date = datetime.fromisoformat(b_str)

    if start_date.date() == end_date.date():
        # same day, just store first date
        meeting_fields["dates"] = start_date.date().isoformat()
    else:
        # different days, interval with only dates
        meeting_fields["dates"] = (
            f"{start_date.date().isoformat()} - {end_date.date().isoformat()}"
        )

    _custom_fields["meeting:meeting"] = meeting_fields
    self["custom_fields"] = _custom_fields
    raise IgnoreKey("conference_dates")


@model.over("title", "(^245__)|(^65024)", override=True)
def title(self, key, value):
    """Translates title."""
    # main title or subtitle depending on key
    text = StringValue(value.get("a", "")).parse()

    if key == "245__":
        self["title"] = text
        full_title = text
    else:
        full_title = (
            f"{self.get('title', '')} - {text}" if text else self.get("title", "")
        )

    # handle very short titles
    if len(full_title) < 4:
        prefix = (
            "Meeting"
            if self["related_identifiers"][0]["resource_type"]["id"] == "event"
            else "Meeting Contribution"
        )
        full_title = f"{prefix}: {full_title}"

    return full_title


@model.over("subjects", "(^65027)", override=True)
@for_each_value
def subjects(self, key, value):
    """Translates description."""
    raw_value = StringValue(value.get("2")).parse()

    category_name = StringValue(value.get("a")).parse()
    # Remove prefix and get the category id and name
    prefix = "INDICO.CERN.CH_CATEGORY_"
    if prefix in raw_value:
        parts = raw_value.split(prefix)[1].split("$$")
        category_id = parts[0]
        description_text = f"Indico Category ({category_id}): {category_name}"
    else:
        description_text = category_name  # fallback if prefix not found

    if description_text:
        additional_desc = {
            "description": description_text,
            "type": {
                "id": "technical-info",
            },
        }
        self.setdefault("additional_descriptions", []).append(additional_desc)

    raise IgnoreKey("subjects")


@model.over("indico_information", "^111__", override=True)
def indico_information(self, key, value):
    """Translates indico_informations."""
    title = StringValue(value.get("a")).parse()
    event_id = StringValue(value.get("g")).parse()
    location = StringValue(value.get("c")).parse()
    date = StringValue(value.get("9")).parse()
    year = StringValue(value.get("f")).parse()
    if date:
        date_obj = parse(date)
    meeting_date = date_obj.strftime("%Y-%m-%d") if date else year

    if len(title) < 4:
        prefix = (
            "Meeting"
            if self["related_identifiers"][0]["resource_type"]["id"] == "event"
            else "Meeting Contribution"
        )
        title = f"{prefix}: {title}"
    self["title"] = title
    self["publication_date"] = meeting_date

    _custom_fields = self.get("custom_fields", {})
    meeting_fields = _custom_fields.get("meeting:meeting", {})
    meeting_fields["title"] = title
    meeting_fields["place"] = location
    meeting_fields["dates"] = meeting_date
    indico_prefix = "INDICO.CERN.CH."
    indico_id = event_id.removeprefix(indico_prefix)
    if not indico_id.isdigit():
        indico_id = get_new_indico_id(
            indico_id
        )  # for legacy indico ids starting with 'a'

    if value.get("u", ""):
        identifier = {
            "identifier": f"https://indico.cern.ch/event/{indico_id}",
            "scheme": "indico",
        }
        meeting_fields.setdefault("identifiers", []).append(identifier)

    _custom_fields["meeting:meeting"] = meeting_fields
    self["custom_fields"] = _custom_fields

    raise IgnoreKey("indico_information")


@model.over("access_grants", "^270__", override=True)
@for_each_value
@require(["p"])
def contact_person(self, key, value):
    """Translates contact person."""
    contributors = self.get("contributors", [])
    name = StringValue(value.get("p")).parse()
    if not name:
        name = "Corinne Pralavorio"  # for missing name in a few records as all correspond to this person
    contributor = {
        "person_or_org": {
            "type": "personal",
            "name": name,
            "family_name": name,
        },
        "role": {"id": "contactperson"},
    }
    contributors.append(contributor)
    self["contributors"] = contributors
    raise IgnoreKey("access_grants")


@model.over("related_workds", "^035__", override=True)
@for_each_value
def identifiers(self, key, value):
    """Translates identifiers into related_identifiers with Indico support."""
    id_value = StringValue(value.get("a", "")).parse()
    scheme = StringValue(value.get("9", "")).parse()

    if scheme != "INDICO.CERN.CH":
        raise UnexpectedValue(
            "Unknown identifier type (IT Meetings)", field=key, value=value
        )

    is_event_id = "." not in id_value

    # normalize: strip any contribution suffix after a dot
    base_id = id_value.split(".")[0]
    relation_type = "isversionof" if is_event_id else "ispartof"

    related_works = self.get("related_identifiers", [])

    if not base_id.isdigit():  # for legacy indico ids starting with 'a'
        base_id = get_new_indico_id(base_id)

    related_works.append(
        {
            "identifier": str(base_id),
            "scheme": "indico",
            "relation_type": {"id": relation_type},
            "resource_type": {"id": "event"},
        }
    )
    self["related_identifiers"] = related_works

    raise IgnoreKey("related_workds")


# Load once so we don’t reload pickle each call
with open(
    "cds_migrator_kit/rdm/data/indico/indico-legacy-ids-contribs.pickle", "rb"
) as f:
    LEGACY_CONTRIB_MAP = pickle.load(f)


@model.over("related_indico_identifiers", "^8564_", override=True)
@for_each_value
def urls(self, key, value):
    """Translates urls field into normalized related_identifiers."""
    sub_u = clean_val("u", value, str, req=True)
    if not sub_u:
        raise UnexpectedValue(
            "Unrecognised string format or link missing.",
            field=key,
            subfield="u",
            value=value,
        )

    if all(x in sub_u for x in ["cds", ".cern.ch/record/", "/files"]):
        raise IgnoreKey("related_indico_identifiers")

    parsed = urlparse(sub_u)
    # Handle old Indico Display links
    if parsed.path.endswith("Display.py"):
        qs = parse_qs(parsed.query)
        conf_id, contrib_id = (
            qs.get("confId", [None])[0],
            qs.get("contribId", [None])[0],
        )

        if conf_id and not conf_id.isdigit():  # remap legacy event id
            conf_id = get_new_indico_id(conf_id)
            if contrib_id:
                contrib_id = LEGACY_CONTRIB_MAP.get((conf_id, contrib_id))

        sub_u = (
            f"https://indico.cern.ch/event/{conf_id}/contributions/{contrib_id}"
            if conf_id and contrib_id
            else f"https://indico.cern.ch/event/{conf_id}"
        )

    netloc = parsed.netloc or parsed.path
    path = parsed.path if parsed.netloc else ""
    if not netloc.startswith("www."):
        netloc = "www." + netloc
    normalized = urlunparse(
        ("http", netloc, path, parsed.params, parsed.query, parsed.fragment)
    )

    related_works = self.get("related_identifiers", [])
    related_works.append(
        {"identifier": sub_u, "scheme": "url", "relation_type": {"id": "isversionof"}}
    )
    self["related_identifiers"] = related_works

    raise IgnoreKey("related_indico_identifiers")
