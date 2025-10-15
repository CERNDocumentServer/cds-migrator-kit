import re

from dateutil.parser import ParserError, parse
from dojson.errors import IgnoreKey

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.decorators import (
    for_each_value,
    require,
)
from cds_migrator_kit.transform.xml_processing.quality.parsers import StringValue

from ...models.hr import hr_model as model
from .base import aleph_number, corporate_author, report_number, subjects, urls


@model.over("subjects", "(^6931_)|(^650[12_][7_])|(^653[12_]_)|(^695__)|(^694__)")
@require(["a"])
@for_each_value
def hr_subjects(self, key, value):
    if key == "6531_":
        keyword = value.get("a")
        try:
            resource_type_map = {
                "Presentation": {"id": "presentation"},
            }
            self["resource_type"] = resource_type_map.get(keyword)
            raise IgnoreKey("subjects")
        except KeyError:
            pass
    subjects(self, key, value)


@model.over("collection", "^690C_", override=True)
@for_each_value
def collection(self, key, value):
    """Translates collection field."""
    collection = value.get("a").strip().lower()
    if collection in ["chis bulletin"]:
        subjects = self.get("subjects", [])
        subjects.append({"subject": "collection:{}".format(collection)})
        self["subjects"] = subjects
        raise IgnoreKey("collection")
    if collection not in [
        "cern admin e-guide",
        "staff rules and regulations",
        "cern",
        "annual personnel statistics",
        "administrative circular",
        "cern annual personnel statistics",
        "intnote",
        "operational circular",
        "publhr",
    ]:
        raise UnexpectedValue(subfield="a", value=value, field=key)
    raise IgnoreKey("collection")


@model.over("creators", "(^773__)|(^110__)")
@for_each_value
def corpo_author(self, key, value):
    if key == "773__":
        author = value.get("t", "").strip()
        conference_cnum = value.get("w", "")
        if conference_cnum:
            _custom_fields = self.get("custom_fields", {})
            custom_meeting_fields = _custom_fields.get("meeting:meeting", {})
            identifiers = custom_meeting_fields.get("identifiers", [])
            identifiers.append({"scheme": "inspire", "identifier": conference_cnum})
            # TODO for record 2713447 - belongs to two conferences, what do we do?
            custom_meeting_fields["identifiers"] = identifiers
            _custom_fields["meeting:meeting"] = custom_meeting_fields
    else:
        author = value.get("a", "").strip()
    if not author:
        raise UnexpectedValue(subfield="a", value=value, field=key)
    author = {"person_or_org": {"type": "organizational", "name": author}}
    if author not in self.get("creators", []):
        return author
    raise IgnoreKey("creators")


@model.over("resource_type", "^980__", override=True)
def resource_type(self, key, value):
    """Translates resource_type."""
    value = value.get("a")
    if value:
        value = value.strip().lower()

    map = {
        "annualstats": {"id": "publication-report"},
        "cern-admin-e-guide": {"id": "publication-article"},
        "administrativenote": {"id": "publication-technicalnote"},
        "intnotehrpubl": {"id": "publication-technicalnote"},
        "chisbulletin": {"id": "publication-periodicalissue"},
        "bulletin": {"id": "publication-periodicalissue"},
        "admincircular": {"id": "administrative-circular"},
        "opercircular": {"id": "administrative-operationalcircular"},
        "staffrules": {"id": "administrative-regulation"},
        "staffrulesvd": {"id": "administrative-regulation"},
        "hr-smc": {"id": "administrative-regulation"},
        "ccp": {"id": "other"},
    }
    try:
        return map[value]
    except KeyError:
        raise UnexpectedValue("Unknown resource type (HR)", field=key, value=value)


@model.over("internal_notes", "^562__")
@for_each_value
def note(self, key, value):
    """Translates notes."""
    return {"note": StringValue(value.get("c")).parse()}


@model.over("record_restriction", "^591__")
@for_each_value
def record_restriction(self, key, value):
    """Translates notes."""
    access = value.get("a")
    if access and access.lower() in ("cern internal", "restricted"):
        return "restricted"
    elif access and access.lower() != "public":
        raise UnexpectedValue("Access field other than public", field=key, value=value)
    raise IgnoreKey("access")


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
                "id": "other",  # what's with the lang
            },
        }
        return _additional_description
    raise IgnoreKey("additional_descriptions")


@model.over("dates", "^925__")
@for_each_value
def date(self, key, value):
    """Translates dates."""
    dates = self.get("dates", [])
    valid = value.get("a")
    date = {
        "date": valid,
        "type": {"id": "valid"},
    }
    dates.append(date)
    withdrawn = value.get("b")
    if "9999" not in withdrawn:
        date = {
            "date": valid,
            "type": {"id": "withdrawn"},
        }
        dates.append(date)
    self["dates"] = dates
    raise IgnoreKey("dates")


@model.over("administrative_unit", "^710__", override=True)
@for_each_value
def custom_fields(self, key, value):
    """Translates administrative_unit."""
    unit = value.get("b")
    if unit:
        _custom_fields = self.get("custom_fields", {})
        _custom_fields["cern:administrative_unit"] = unit
        self["custom_fields"] = _custom_fields
    else:
        contributors = self.get("contributors", [])
        try:
            author = corporate_author(self, key, value)
        except IgnoreKey:
            author = None
        if author:
            contributors.append(author[0])
            self["contributors"] = contributors

    raise IgnoreKey("administrative_unit")


@model.over("description", "^520__", override=True)
def description(self, key, value):
    """Translates description."""
    description_text = StringValue(value.get("a")).parse()
    if len(description_text) >= 3:
        return description_text
    raise IgnoreKey("description")


@model.over("additional_descriptions", "(^590__)")
@for_each_value
def translated_description(self, key, value):
    description_text = value.get("a", "")
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


@model.over("identifiers", "(^037__)|(^088__)|(^8564_)|(^970__)", override=True)
@for_each_value
def title(self, key, value):
    """Translates title and identifiers."""
    # ----Title-----#
    title = StringValue(value.get("a")).parse()
    if title.startswith("CERN-STAFF-RULES-"):
        match = re.match(r"^CERN-STAFF-RULES-([A-Z0-9]+)(?:-.+)?$", title)
        if match:
            suffix = match.group(1)
            self["title"] = f"Staff Rules and Regulations No.{suffix}"

    # ------Identifiers-----#
    new_id = None
    if key in ("037__", "088__"):
        new_id = report_number(self, key, value)
    elif key == "8564_":
        new_id = urls(self, key, value)
    elif key == "970__":
        new_id = aleph_number(self, key, value)
    if new_id:
        return new_id[0]
    raise IgnoreKey("identifiers")
