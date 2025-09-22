import re

from dateutil.parser import ParserError, parse
from dojson.errors import IgnoreKey
from dojson.utils import flatten
from idutils.normalizers import normalize_isbn
from isbnlib import NotValidISBNError

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.decorators import for_each_value
from cds_migrator_kit.transform.xml_processing.quality.parsers import (
    StringValue,
    clean_val,
)

from ...models.books import book_model as model


@model.over("creators", "(^110)")
@for_each_value
def corpo_creator(self, key, value):
    """Translates creators field."""

    creator = value.get("a", "").strip()

    if not creator:
        raise UnexpectedValue(subfield="a", value=value, field=key)
    author = {"person_or_org": {"type": "organizational", "name": creator}}
    if author not in self.get("creators", []):
        return author
    raise IgnoreKey("creators")


@model.over("collection", "^690C_")
@for_each_value
def collection(self, key, value):
    """Translates collection field."""
    collection = value.get("a", "")
    if type(collection) is tuple:
        raise UnexpectedValue(subfield="a", field=key, value=value)
    collection = collection.strip().lower()
    if collection not in ["cern", "book"]:
        raise UnexpectedValue(subfield="a", field=key, value=value)
    raise IgnoreKey("collection")


@model.over("keywords", "^697C_")
@for_each_value
def keywords(self, key, value):
    val = value.get("a")
    keyword_map = {"LEGSERLIB": "Legal service", "BOOKSHOP": "BOOKSHOP"}

    if val:
        subjects = self.get("subjects", [])
        subjects.append({"subject": keyword_map[val]})
        self["subjects"] = subjects
    raise IgnoreKey("keywords")


@model.over("table_of_content", "(^505__)|(^5050_)")
@flatten
@for_each_value
def table_of_content(self, key, value):
    """Translates table of content field."""
    text = value.get("a", "").strip().lower()
    if not text:
        text = value.get("t", "").strip().lower()
    if text:
        chapters = re.split(r"; | -- |--", text)
        chapters = "<br>".join(chapters)
        return chapters
    else:
        raise UnexpectedValue(subfield="a or t", field=key, value=value)
