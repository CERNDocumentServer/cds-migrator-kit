import re

from dateutil.parser import ParserError, parse
from dojson.errors import IgnoreKey
from dojson.utils import force_list
from edtf import EDTFParseException, parse_edtf, text_to_edtf
from idutils.normalizers import normalize_isbn, normalize_issn
from isbnlib import NotValidISBNError

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.decorators import (
    filter_list_values,
    for_each_value,
    require,
    strip_output,
)
from cds_migrator_kit.transform.xml_processing.quality.parsers import StringValue
from cds_migrator_kit.transform.xml_processing.rules.base import process_contributors

from ...config import (
    ALLOWED_DOCUMENT_TAGS,
    ALLOWED_THESIS_COLLECTIONS,
    FORMER_COLLECTION_TAGS_TO_KEEP,
    IGNORED_THESIS_COLLECTIONS,
    udc_pattern,
)
from ...models.base_publication_record import rdm_base_publication_model as model


@model.over("custom_fields", "(^020__)")
def isbn(self, key, value):
    _custom_fields = self.get("custom_fields", {})
    _isbn = StringValue(value.get("a", "")).parse()

    if _isbn:
        try:
            _isbn = normalize_isbn(_isbn)

        except NotValidISBNError as e:
            raise UnexpectedValue("Not a valid ISBN.", field=key, value=value)
        is_cern_isbn = _isbn.startswith("978-92-9083")
        thesis_fields = _custom_fields.get("imprint:imprint", {})
        thesis_fields["isbn"] = _isbn
        _custom_fields["imprint:imprint"] = thesis_fields

        if is_cern_isbn:
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

        if new_id not in ids:
            ids.append(new_id)
        self[destination] = ids
    return _custom_fields


@model.over("related_identifiers", "(^022__)")
@for_each_value
def issn(self, key, value):
    _issn = StringValue(value.get("a", "")).parse()
    if _issn:
        try:
            _issn = normalize_issn(_issn)
        except NotValidISBNError as e:
            raise UnexpectedValue("Not a valid ISSN.", field=key, value=value)

        ids = self.get("identifiers", [])

        new_id = {
            "identifier": _issn,
            "scheme": "issn",
            "relation_type": {"id": "ispublishedin"},
        }
        if new_id not in ids:
            return new_id
    raise IgnoreKey("identifiers")


@model.over("subjects", "(^080__)")
@for_each_value
def udc(self, key, value):
    """Check 080 field. Drop UDC."""
    val_a = value.get("a")
    if val_a and re.findall(udc_pattern, val_a):
        raise IgnoreKey("identifiers")
    raise UnexpectedValue(
        "UDC format check failed.", field=key, subfield="a", value=value
    )


@model.over("custom_fields", "(^536__)")
def funding(self, key, value):
    _custom_fields = self.get("custom_fields", {})
    programme = value.get("a")
    _access_info = value.get("r", "").strip().lower()
    if _access_info and _access_info not in ["openaccess", "open access"]:
        raise UnexpectedValue(
            "Access information has unexpected value", field=key, value=value
        )
    # https://cerneu.web.cern.ch/fp7-projects
    is_fp7_programme = programme and programme.strip().lower() == "fp7"

    if programme and not is_fp7_programme:
        # if not fp7, then it is cern programme
        _custom_fields["cern:programmes"] = programme
        return _custom_fields
    elif "f" in value or "c" in value:
        awards = self.get("funding", [])
        # this one is reliable, I checked the DB
        try:
            _funding = value.get("f", "").strip().lower()
            _grant_number = value.get("c", "").strip().lower()
        except AttributeError as e:
            raise UnexpectedValue(
                "Multiple grant numbers must be in separate tag", field=key, value=value
            )
        award = {
            "award": {"id": f"00k4n6c32::{_grant_number}"},
            "funder": {"id": "00k4n6c32"},
        }
        if award not in awards:
            awards.append(award)
        self["funding"] = awards
    raise IgnoreKey("custom_fields")


@model.over("custom_fields", "(^773__)")
def journal(self, key, value):
    _custom_fields = self.get("custom_fields", {})
    journal_fields = _custom_fields.get("journal:journal", {})
    year = StringValue(value.get("y", "")).parse()
    meeting_fields = ["p", "n", "v", "c"]
    is_journal_year = False
    for field in meeting_fields:
        if field in value:
            is_journal_year = True
            break

    pub_date = self.get("publication_date")
    # if we only have 773 in the record and no other journal fields,
    # it is not journal date
    if not is_journal_year and "y" in value and not pub_date:
        self["publication_date"] = year

    journal_fields["title"] = StringValue(value.get("p", "")).parse()
    journal_fields["issue"] = StringValue(value.get("n", "")).parse()
    journal_fields["volume"] = StringValue(value.get("v", "")).parse()
    journal_fields["pages"] = StringValue(value.get("c", "")).parse()

    _custom_fields["journal:journal"] = journal_fields
    return _custom_fields


@model.over("internal_notes", "^562__")
@for_each_value
def internal_notes(self, key, value):
    """Translate internal notes"""
    note = value.get("c", "")
    return {"note": note}


@model.over("contributors", "^901__")
@for_each_value
def organisation(self, key, value):
    contributor = value.get("u")
    return {"person_or_org": {"type": "organizational", "name": contributor}}


@model.over("related_identifiers", "^962_")
@for_each_value
def related_identifiers(self, key, value):
    """Translates related identifiers."""
    recid = value.get("b")
    try:
        material = value.get("n", "").lower().strip()
    except AttributeError:
        raise UnexpectedValue(
            "related identifiers have unexpected material format",
            field=key,
            value=value,
        )
    rel_ids = self.get("related_identifiers", [])
    res_type = None
    if material and material == "book":
        # if book we know that is published in a book,
        res_type = "publication-book"
    elif material:
        #  otherwise it will be a conference reference
        res_type = "event"
    new_id = {
        "identifier": f"https://cds.cern.ch/record/{recid}",
        "scheme": "url",
        "relation_type": {"id": "references"},
    }

    if res_type:
        new_id.update({"resource_type": {"id": res_type}})

    if new_id not in rel_ids:
        return new_id
    raise IgnoreKey("related_identifiers")
