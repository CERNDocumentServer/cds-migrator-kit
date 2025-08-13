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


@model.over("related_identifiers_custom_fields", "^962_", override=True)
@for_each_value
def related_identifiers_custom_fields(self, key, value):
    """Handles both custom fields and related identifiers from 962_."""

    # ------------------------------
    # Related Identifiers
    # ------------------------------
    recid = value.get("b")
    year = "".join(filter(str.isdigit, value.get("n", "")))[:4]
    related_works = self.get("related_identifiers", [])

    new_id = {
        "identifier": recid,
        "scheme": "lcds",
        "relation_type": {"id": "ispartof"},
        "resource_type": {"id": "publication-report"},
    }

    if new_id not in related_works:
        related_works.append(new_id)
        self["related_identifiers"] = related_works

    # ------------------------------
    # Journal & Imprint
    # ------------------------------

    _custom_fields = self.get("custom_fields", {})
    journal_fields = _custom_fields.get("journal:journal", {})
    imprint_fields = _custom_fields.get("imprint:imprint", {})
    journal_fields["title"] = f"CERN Annual Report {year}"
    journal_fields["volume"] = value.get("s", "")
    breakpoint()
    if self.get("title") in value.get("v", ""):  # It is a volume
        journal_fields["pages"] = StringValue(value.get("k", "")).parse()
    else:  # It is an article of the volume
        imprint_fields["title"] = StringValue(value.get("v", "")).parse()
        imprint_fields["pages"] = StringValue(value.get("k", "")).parse()

    _custom_fields["journal:journal"] = journal_fields
    _custom_fields["imprint:imprint"] = imprint_fields
    self["custom_fields"] = _custom_fields

    # Return nothing, updated both fields in-place
    raise IgnoreKey("related_identifiers_custom_fields")


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
            ids = []
        if new_id not in ids:
            ids.append(new_id)
        self[destination] = ids
    raise IgnoreKey("isbn")
