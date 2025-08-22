from dateutil.parser import ParserError, parse
from dojson.errors import IgnoreKey

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.decorators import for_each_value

from ...config import IGNORED_THESIS_COLLECTIONS
from ...models.beams import beams_model as model


@model.over("collection", "^690C_", override=True)
@for_each_value
def collection(self, key, value):
    """Translates collection field."""
    collection = value.get("a").strip().lower()
    if collection in IGNORED_THESIS_COLLECTIONS:
        raise IgnoreKey("collection")
    # if collection not in ALLOWED_THESIS_COLLECTIONS:
    #     raise UnexpectedValue(subfield="a", key=key, value=value, field="690C_")
    # if collection == "yellow report":
    subjects = self.get("subjects", [])
    subjects.append(
        {
            "subject": collection.upper(),
        }
    )
    self["subjects"] = subjects
    raise IgnoreKey("collection")


@model.over("resource_type", "^980__", override=True)
def resource_type(self, key, value):
    """Translates resource_type."""
    value = value.get("a")
    if value:
        value = value.lower()
        if value in ["internaldocument"]:
            raise IgnoreKey("resource_type")

    map = {
        "preprint": {"id": "publication-preprint"},
        "conferencepaper": {"id": "publication-conferencepaper"},
        "belib": {"id": "other"},
        "intnotebepubl": {"id": "publication-technicalnote"},
        "article": {"id": "publication"},
        # todo newsletter
    }
    try:
        return map[value]
    except KeyError:
        raise UnexpectedValue("Unknown resource type (BE)", field=key, value=value)


@model.over("internal_notes", "(^500__)")
def internal_notes(self, key, value):
    # TODO change to normal notes
    """Translates internal_notes field."""
    internal_notes = self.get("internal_notes", [])
    internal_notes.append({"note": value.get("a")})
    return internal_notes
