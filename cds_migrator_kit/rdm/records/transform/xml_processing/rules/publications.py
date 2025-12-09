import re

from dateutil.parser import ParserError, parse
from dojson.errors import IgnoreKey
from dojson.utils import force_list
from edtf import EDTFParseException, parse_edtf, text_to_edtf
from idutils.normalizers import normalize_isbn, normalize_issn
from isbnlib import NotValidISBNError

from cds_migrator_kit.errors import ManualImportRequired, UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.decorators import (
    filter_list_values,
    for_each_value,
    require,
    strip_output,
)
from cds_migrator_kit.transform.xml_processing.quality.parsers import StringValue

from ...config import (
    udc_pattern,
)
from ...models.base_publication_record import rdm_base_publication_model as model
from .base import normalize


@model.over("isbns", "^020__")
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
            # TODO, should we have ISBN as internal?
            destination = "related_identifiers"
            new_id = {
                "identifier": _isbn,
                "scheme": "isbn",
                "relation_type": {"id": "isvariantformof"},
                "resource_type": {"id": "publication-book"},
            }
        else:
            destination = "related_identifiers"
            new_id = {
                "identifier": _isbn,
                "scheme": "isbn",
                "relation_type": {"id": "isvariantformof"},
                "resource_type": {"id": "publication-book"},
            }
        ids = self.get(destination, [])

        if new_id not in ids:
            ids.append(new_id)
        self[destination] = ids
    self["custom_fields"] = _custom_fields
    raise IgnoreKey("custom_fields")


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
    raise IgnoreKey("related_identifiers")


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


@model.over("publication_date", "(^260__)", override=True)
def imprint_info(self, key, value):
    """Translates imprint - WARNING - also publisher and publication_date.

    In case of summer student notes this field contains only date
    but it needs to be reimplemented for the base set of rules -
    it will contain also imprint place
    """
    _custom_fields = self.get("custom_fields", {})
    imprint = _custom_fields.get("imprint:imprint", {})

    publication_date_str = value.get("c")
    _publisher = value.get("b")
    place = value.get("a")
    if _publisher and not self.get("publisher"):
        self["publisher"] = _publisher
    if place:
        imprint["place"] = place
    self["custom_fields"]["imprint:imprint"] = imprint
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


@model.over("internal_notes", "(^500__)")
def internal_notes(self, key, value):
    # TODO change to normal notes
    """Translates internal_notes field."""
    internal_notes = self.get("internal_notes", [])
    note = value.get("a")
    if note:
        internal_notes.append({"note": note})
    return internal_notes


@model.over("funding", "(^536__)")
def funding(self, key, value):
    _custom_fields = self.get("custom_fields", {})
    programme = value.get("a")
    _access_info = value.get("r", "").strip().lower()
    if _access_info:
        raise ManualImportRequired(
            "Open access field detected", field=key, value=value, priority="critical"
        )
    if _access_info and _access_info not in ["openaccess", "open access"]:
        raise UnexpectedValue(
            "Access information has unexpected value", field=key, value=value
        )
    # https://cerneu.web.cern.ch/fp7-projects
    is_fp7_programme = programme and programme.strip().lower() == "fp7"

    # TODO check if this applies to other publications not only thesis
    # if programme and not is_fp7_programme:
    #     # if not fp7, then it is cern programme
    #     _custom_fields["cern:programmes"] = programme
    #     self["custom_fields"] = _custom_fields
    if programme and "f" in value or "c" in value:
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
    else:
        raise UnexpectedValue("Unexpected grant value", field=key, value=value)
    raise IgnoreKey("funding")


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

    conference_cnum = value.get("w", "")
    if conference_cnum:
        custom_meeting_fields = _custom_fields.get("meeting:meeting", {})
        identifiers = custom_meeting_fields.get("identifiers", [])
        identifiers.append({"scheme": "inspire", "identifier": conference_cnum})

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
    contributor = value.get("u", "")
    return {
        "person_or_org": {
            "type": "organizational",
            "name": contributor,
        },
        "role": {"id": "hostinginstitution"},
    }


@model.over("related_identifiers", "^962__")
@for_each_value
def related_identifiers(self, key, value):
    """Translates related identifiers."""
    recid = value.get("b")
    artid = value.get("k", "")
    try:
        conference = value.get("n", "").lower().strip()
        meeting_fields = self.get("custom_fields", {}).get("meeting:meeting", {})
        if not meeting_fields.get("title"):
            meeting_fields["title"] = conference
        self["custom_fields"]["meeting:meeting"] = meeting_fields
    except AttributeError:
        raise UnexpectedValue(
            "related identifiers have unexpected material format",
            field=key,
            value=value,
        )
    rel_ids = self.get("related_identifiers", [])

    new_id = {
        "identifier": recid,
        "scheme": "cds",
        "relation_type": {"id": "references"},
        "resource_type": {"id": "event"},
    }

    if artid:
        artid_from_773 = (
            self.get("custom_fields", {}).get("journal:journal", {}).get("pages")
        )
        if artid_from_773 != artid:
            res_type = "publication-other"
            new_id.update({"resource_type": {"id": res_type}})

    if recid and new_id not in rel_ids:
        return new_id
    raise IgnoreKey("related_identifiers")
