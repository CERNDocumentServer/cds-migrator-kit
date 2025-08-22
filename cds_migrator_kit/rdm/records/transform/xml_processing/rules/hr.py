from dateutil.parser import ParserError, parse
from dojson.errors import IgnoreKey

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.decorators import (
    for_each_value,
    require,
)
from cds_migrator_kit.transform.xml_processing.quality.parsers import StringValue
from .base import subjects

from ...models.hr import hr_model as model


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

    map = {
        "preprint": {"id": "publication-preprint"},
        "conferencepaper": {"id": "publication-conferencepaper"},
        "intnotebepubl": {"id": "publication-technicalnote"},
        "article": {"id": "publication"},
        "itcerntalk": {"id": "presentation"},
        "intnoteitpubl": {"id": "publication-technicalnote"},
        "bookchapter": {"id": "publication-bookchapter"},
        # todo newsletter
    }
    try:
        return {"id": "other"}
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
    if access and access.lower() == "cern internal":
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
