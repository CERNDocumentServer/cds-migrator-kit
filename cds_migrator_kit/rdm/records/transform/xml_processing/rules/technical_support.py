import math

from dateutil.parser import ParserError, parse
from dojson.errors import IgnoreKey
from dojson.utils import force_list
from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.decorators import for_each_value, \
    require
from cds_migrator_kit.transform.xml_processing.quality.parsers import StringValue
from .base import subjects as base_subjects
from ...config import IGNORED_THESIS_COLLECTIONS
from ...models.technical_support import technical_support_model as model
from .base import corporate_author as base_corporate_author
from .base import normalize


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
