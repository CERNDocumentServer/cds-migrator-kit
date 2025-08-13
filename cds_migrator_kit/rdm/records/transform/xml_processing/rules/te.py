import math

from dateutil.parser import ParserError, parse
from dojson.errors import IgnoreKey

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.decorators import for_each_value

from ...config import IGNORED_THESIS_COLLECTIONS
from ...models.te import te_model as model


@model.over("resource_type", "^980__", override=True)
def resource_type(self, key, value):
    """Translates resource_type."""
    value = value.get("a")
    priority = {v: i for i, v in
                enumerate(["intnotetepubl", "intnotetspubl", "clinot", "note", "preprint", "bookchapter",
                           "conferencepaper", "article"])}
    current = self.get("resource_type")
    if value:
        value = value.lower()

    rank = priority.get(value, math.inf)
    map = {
        "preprint": {"id": "publication-preprint"},
        "conferencepaper": {"id": "publication-conferencepaper"},
        "article": {"id": "publication-article"},
        "intnotetepubl": {"id": "publication-technicalnote"},
        "intnoteatspubl": {"id": "publication-technicalnote"},
        "clinot": {"id": "publication-technicalnote"},
        "note": {"id": "publication-technicalnote"},
        "bookchapter": {"id": "publication-section"},
    }

    if current:
        current_key = next((k for k, v in map.items() if v == current), None)
        current_rank = priority.get(current_key, math.inf)
        if rank < current_rank:
            try:
                return map[value]
            except KeyError:
                raise UnexpectedValue("Unknown resource type (TE)", value=value, field=key)
        else:
            raise IgnoreKey("resource_type")
    else:
        try:
            return map[value]
        except KeyError:
            raise UnexpectedValue("Unknown resource type (TE)", value=value, field=key)
