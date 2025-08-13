import math

from dateutil.parser import ParserError, parse
from dojson.errors import IgnoreKey

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.decorators import for_each_value
from cds_migrator_kit.transform.xml_processing.quality.parsers import StringValue

from ...config import IGNORED_THESIS_COLLECTIONS
from ...models.en import en_model as model


@model.over("collection", "^690C_", override=True)
@for_each_value
def collection(self, key, value):
    """Translates collection field."""
    collection = value.get("a").strip().lower()
    if collection in ["yellowrepcontrib"]:
        subjects = self.get("subjects", [])
        subjects.append({"subject": "collection:YELLOWREPCONTRIB"})
        self["subjects"] = subjects
        raise IgnoreKey("collection")
    if collection not in ["cern", "preprint", "article", "intnote", "publen",
                          "en department head documents",
                          "eucard",
                          "eucardpub",
                          "publts", # to be checked
                          "publats", # to be checked, via claiming
                          "lhcf_papers",
                          "lhcf_proc"

]:
        raise UnexpectedValue(subfield="a", value=value, field=key)
    raise IgnoreKey("collection")


@model.over("resource_type", "^980__", override=True)
def resource_type(self, key, value):
    """Translates resource_type."""
    value = value.get("a")
    priority = {v: i for i, v in
                enumerate(["intnoteenpubl", "intnotetspubl", "note", "preprint", "bookchapter",
                           "conferencepaper", "article"])}
    current = self.get("resource_type")
    if value:
        value = value.lower()

    rank = priority.get(value, math.inf)
    map = {
        "preprint": {"id": "publication-preprint"},
        "conferencepaper": {"id": "publication-conferencepaper"},
        "article": {"id": "publication-article"},
        "note": {"id": "publication-technicalnote"},
        "intnoteenpubl": {"id": "publication-technicalnote"},
        "intnotetspubl": {"id": "publication-technicalnote"},
        "bookchapter": {"id": "publication-section"},
        "enlib": {"id": "other"},  # TODO
    }

    if current:
        current_key = next((k for k, v in map.items() if v == current), None)
        current_rank = priority.get(current_key, math.inf)
        if rank < current_rank:
            try:
                return map[value]
            except KeyError:
                raise UnexpectedValue("Unknown resource type (EN)", value=value, field=key)
        else:
            raise IgnoreKey("resource_type")
    else:
        try:
            return map[value]
        except KeyError:
            raise UnexpectedValue("Unknown resource type (EN)", value=value, field=key)
