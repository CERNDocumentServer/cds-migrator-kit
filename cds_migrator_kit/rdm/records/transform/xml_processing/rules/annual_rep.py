from dateutil.parser import ParserError, parse
from dojson.errors import IgnoreKey
from idutils.normalizers import normalize_isbn
from isbnlib import NotValidISBNError

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.decorators import for_each_value
from cds_migrator_kit.transform.xml_processing.quality.parsers import StringValue

from ...models.annual_report import annual_rep_model as model


@model.over("collection", "^690C_")
@for_each_value
def collection(self, key, value):
    """Translates collection field."""
    collection = value.get("a").strip().lower()
    if collection not in ["cern"]:
        raise UnexpectedValue(subfield="a", key=key, value=value, field="690C_")
    raise IgnoreKey("collection")


@model.over("subjects", "^65017", override=True)
def subjects(self, key, value):
    raise IgnoreKey("subjects")


@model.over("isbn", "(^020__)", override=True)
def isbn(self, key, value):
    _isbn = StringValue(value.get("a", "")).parse()
    _isbn_material = StringValue(value.get("u", "")).parse()

    if _isbn:
        try:
            _isbn = normalize_isbn(_isbn)

        except NotValidISBNError as e:
            raise UnexpectedValue("Not a valid ISBN.", field=key, value=value)

        is_cern_isbn = _isbn.startswith("978-92-9083")
        is_print_version = "print" in _isbn_material.lower()

        if is_cern_isbn and not is_print_version:
            destination = "identifiers"
            new_id = {"identifier": _isbn, "scheme": "isbn"}
        else:
            destination = "related_identifiers"
            new_id = {
                "identifier": _isbn,
                "scheme": "isbn",
                "relation_type": {"id": "isversionof"},
            }
        ids = self.get(destination, [])
        if not ids:
            import ipdb;ipdb.set_trace()
            ids = []
        if new_id not in ids:
            ids.append(new_id)
        self[destination] = ids
    raise IgnoreKey("isbn")
