from dateutil.parser import ParserError, parse
from dojson.errors import IgnoreKey

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.decorators import for_each_value

from ...config import IGNORED_THESIS_COLLECTIONS
from ...models.en import en_model as model


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
        "article": {"id": "publication"},
        "intnoteenpubl": {"id": "publication-technicalnote"},
        "enlib": {"id": "other"},
        # todo newsletter
    }
    try:
        return map[value]
    except KeyError:
        raise UnexpectedValue("Unknown resource type", key=key, value=value)
