from dateutil.parser import ParserError, parse
from dojson.errors import IgnoreKey

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.decorators import for_each_value

from ...config import IGNORED_THESIS_COLLECTIONS
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
    }
    try:
        return map[value]
    except KeyError:
        raise UnexpectedValue("Unknown resource type (IT)", field=key, value=value)



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