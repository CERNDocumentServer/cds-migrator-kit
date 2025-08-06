from dateutil.parser import ParserError, parse
from dojson.errors import IgnoreKey

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.decorators import for_each_value

from ......transform.xml_processing.quality.decorators import require
from ......transform.xml_processing.rules.base import process_contributors
from ...config import IGNORED_THESIS_COLLECTIONS
from ...models.sy import sy_model as model


@model.over("collection", "^690C_")
@for_each_value
def collection(self, key, value):
    """Translates collection field."""
    collection = value.get("a").strip().lower()
    if collection not in ["publsy", "intnote", "cern", "preprint"]:
        raise UnexpectedValue(subfield="a", key=key, value=value, field="690C_")
    raise IgnoreKey("collection")


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
        "intnotesypubl": {"id": "publication-technicalnote"},
        "bookchapter": {"id": "publication-bookchapter"},
        # todo newsletter
    }
    try:
        return map[value]
    except KeyError:
        raise UnexpectedValue("Unknown resource type", key=key, value=value)


@model.over("creators", "^100__", override=True)
@for_each_value
@require(["a"])
def creators(self, key, value):
    """Translates the creators field."""
    return process_contributors(key, value, orcid_subfield="j")
