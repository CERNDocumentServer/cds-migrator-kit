import math

from dateutil.parser import ParserError, parse
from dojson.errors import IgnoreKey
from dojson.utils import  force_list
from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.decorators import for_each_value
from cds_migrator_kit.transform.xml_processing.quality.parsers import StringValue

from ...config import IGNORED_THESIS_COLLECTIONS
from ...models.technical_support import technical_support_model as model
from .base import corporate_author as base_corporate_author
from .base import normalize

@model.over("resource_type", "^980__", override=True)
def resource_type(self, key, value):
    """Translates resource_type."""
    value = value.get("a", "").lower()
    if value in ["conferencepaper", "note","bookchapter", "lhcb_misc"]:
        raise IgnoreKey("resource_type")

    map = {
        "preprint": {"id": "publication-preprint"},
        "article": {"id": "publication"},
        "itcerntalk": {"id": "presentation"},
        "intnotetspubl": {"id": "publication-technicalnote"},
        "demsuppliers": {"id": "physicalobject"},

    }
    try:
        if value:
            return map[value]
        else:
            raise IgnoreKey("resource_type")
    except KeyError:
        breakpoint()
        raise UnexpectedValue("Unknown resource type (TS)", field=key, value=value)

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
        base_corporate_author(self, key, value)

    raise IgnoreKey("administrative_unit")


# @model.over("additional_descriptions", "(^500__)")
# @for_each_value
# def additional_descriptions(self, key, value):
#     description = value.get("a", "").strip()
#     if description:
#         return {"description": description, "type": {"id": "technical-info"}}
#     raise IgnoreKey("additional_descriptions")


@model.over("publication_date", "(^961__)", override=True)
def imprint_info(self, key, value):
    """Translates publication_date field."""
    publication_date_str = value.get("x")
    if publication_date_str:
        try:
            publication_date = normalize(publication_date_str)

            return publication_date
        except (ParserError, TypeError) as e:
            raise UnexpectedValue(
                field=key,
                value=value,
                message=f"Can't parse provided publication date. Value: {publication_date_str}",
            )
    raise IgnoreKey("publication_date")