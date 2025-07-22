from dateutil.parser import ParserError, parse
from dojson.errors import IgnoreKey

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.decorators import (
    for_each_value,
    require,
)
from cds_migrator_kit.transform.xml_processing.quality.parsers import StringValue

from ...config import IGNORED_THESIS_COLLECTIONS
from ...models.hr import hr_model as model


@model.over("resource_type", "^980__", override=True)
def resource_type(self, key, value):
    """Translates resource_type."""
    value = value.get("a")
    if value:
        value = value.lower()
        if value in ["internaldocument", "slides"]:
            raise IgnoreKey("resource_type")

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
        raise UnexpectedValue("Unknown resource type", key=key, value=value)


@model.over("internal_notes", "^562__")
@for_each_value
def note(self, key, value):
    """Translates notes."""
    return {"note": StringValue(value.get("c")).parse()}


@model.over("access", "^591__")
@for_each_value
def access(self, key, value):
    """Translates notes."""
    access = value.get("a")
    if access and access.lower() != "public":
        raise UnexpectedValue("Access field other than public", key=key, value=value)
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
