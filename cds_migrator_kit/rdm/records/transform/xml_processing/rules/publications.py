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
from .base import licenses as _base_licenses
from .base import normalize

# Unwrapped base functions (strip @for_each_value to avoid double-wrapping).
# licenses also has @filter_values beneath @for_each_value, so two levels deep.
_raw_licenses = (
    _base_licenses.__wrapped__
)  # filter_values(raw) — handles None filtering

_FUNDING_MODEL_MAP = {
    "scoap3": "scoap3",
    "collective": "collective",
    "cern-rp": "cern-rp",
    "cern-apc": "cern-apc",
    "other": "other",
}


def _sub(v, code):
    """Return first string value of a MARC subfield, handling dojson tuple packing."""
    val = force_list(v.get(code))
    return val[0] if val else ""


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


@model.over("creators", "(^110__)")
@for_each_value
def corpo_author(self, key, value):
    author = value.get("a", "").strip()
    if not author:
        raise UnexpectedValue(subfield="a", value=value, field=key)
    author = {"person_or_org": {"type": "organizational", "name": author}}
    if author not in self.get("creators", []):
        return author
    raise IgnoreKey("creators")


@model.over("imprint_info", "(^250__)")
@for_each_value
@require(["a"])
def imprint(self, key, value):
    """Translates additional description."""
    _custom_fields = self.setdefault("custom_fields", {})
    imprint = _custom_fields.setdefault("imprint:imprint", {})
    imprint["edition"] = StringValue(value.get("a")).parse()
    raise IgnoreKey("imprint_info")


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
        place = place.rstrip(".")
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


@model.over("_approval", "(^591__)")
def status(self, key, value):
    _status = value.get("b", "").lower().strip()
    if _status == "approved":
        raise IgnoreKey("_approval")
    raise UnexpectedValue("Unexpected status value", field=key, value=value)


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
    conference_acronym = value.get("q", "")
    custom_meeting_fields = _custom_fields.get("meeting:meeting", {})
    if conference_cnum:
        identifiers = custom_meeting_fields.get("identifiers", [])
        identifiers.append({"scheme": "inspire", "identifier": conference_cnum})
    if conference_acronym:
        custom_meeting_fields["acronym"] = conference_acronym

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


@model.over("_oa_license", "^540__", override=True)
def oa_level_from_license(self, key, value):
    """Detect funding model; also runs base license logic for rights.

    540__f: 'SCOAP3'|'Collective'|'CERN-RP'|'CERN-APC'|'Other' → funding model
    """
    _custom_fields = self.get("custom_fields", {})
    rights = self.get("rights", [])

    for v in force_list(value):
        qualifier = _sub(v, "f").strip()
        funding_model_id = _FUNDING_MODEL_MAP.get(qualifier.lower())

        if funding_model_id and not _custom_fields.get("cern:oa_funding_model"):
            _custom_fields["cern:oa_funding_model"] = {"id": funding_model_id}

        # Base license logic: expand repeated 'a' subfields into individual calls
        # because clean_val raises UnexpectedValue for tuple values by default.
        a_vals = force_list(v.get("a")) or ()
        for a_val in a_vals:
            if not a_val:
                continue
            license_result = _raw_licenses(self, key, dict(v, a=a_val))
            if license_result and license_result not in rights:
                rights.append(license_result)

    self["custom_fields"] = _custom_fields
    if rights:
        self["rights"] = rights
    raise IgnoreKey("_oa_license")


@model.over("access_grants", "^506[1_]_")
@for_each_value
def access_grants(self, key, value):
    """Translates access permissions (by user email or group name)."""
    raw_identifier = value.get("d") or value.get("m") or value.get("a")
    subject_identifier = StringValue(raw_identifier).parse()
    if not subject_identifier:
        raise IgnoreKey("access_grants")

    permission_type = "view"
    return {str(subject_identifier): permission_type}


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


# @model.over("_approvals", "^903__")
# @for_each_value
# def organisation(self, key, value):
#     contributor = value.get("u", "")
#     return {
#         "person_or_org": {
#             "type": "organizational",
#             "name": contributor,
#         },
#         "role": {"id": "hostinginstitution"},
#     }


@model.over("dates", "^925__")
@for_each_value
def date(self, key, value):
    """Translates dates."""
    dates = self.get("dates", [])
    valid = value.get("a")
    if valid:
        date = {
            "date": valid,
            "type": {"id": "submitted"},
        }
        dates.append(date)
    withdrawn = value.get("b", "")
    if withdrawn and "9999" not in withdrawn:
        date = {"date": withdrawn, "type": {"id": "other"}, "description": "completed"}
        dates.append(date)
    self["dates"] = dates
    raise IgnoreKey("dates")


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
        if artid_from_773 and artid_from_773 != artid:
            res_type = "publication-other"
            new_id.update({"resource_type": {"id": res_type}})

    if recid and new_id not in rel_ids:
        return new_id
    raise IgnoreKey("related_identifiers")


@model.over("resource_type", "(^980__)|(^697C_)", override=True)
def resource_type(self, key, value):
    """Translates resource_type."""
    value_a = value.get("a", "")
    value_b = value.get("b", "")

    ignore_res_types = [
        "publarda",
        "aleph_misc",
        "opal_misc",
        "l3_misc",
        "delphi_misc",
        "l3_papers",
        "delphi_papers",
        "opal_papers",
        "aleph_papers",
        "ps212_papers",
    ]

    committees = {
        "scicommpubldrdc": "DRDC",
        "scicommpubleec": "EEC",
        "scicommpublemc": "EmC",
        "scicommpublisc": "ISC",
        "scicommpublisrc": "ISRC",
        "scicommpublistc": "ISTC",
        "scicommpubllepc": "LEPC",
        "scicommpublnprc": "NPRC",
        "scicommpublnsc": "NSC",
        "scicommpublphi": "PH-I",
        "scicommpublphiii": "PH-III",
        "scicommpublpsc": "PSC",
        "scicommpublpscc": "PSCC",
        "scicommpublscc": "SCC",
        "sc_and_ps_advisory_committee": "SC and PS Advisory Committee",
        "scicommpublspsc": "SPSC",
        "scicommpublspslc": "SPSLC",
        "scicommpubltcc": "TCC",
    }

    if (value_a and value_a.lower() in committees.keys()) or (
        value_b and value_b in committees
    ):

        custom_fields = self.get("custom_fields", {})
        comm_cf = custom_fields.get("cern:committees", [])
        if value_a:
            comm_cf.append({"id": committees[value_a.lower()]})
        if value_b:
            comm_cf.append({"id": committees[value_b.lower()]})
        self["custom_fields"]["cern:committees"] = comm_cf
        raise IgnoreKey("resource_type")
    if (value_a and value_a.lower() in ignore_res_types) or (
        value_b and value_b in ignore_res_types
    ):
        raise IgnoreKey("resource_type")

    # first has highest priority
    priority = {
        v: i
        for i, v in enumerate(
            [
                "conferencepaper",
                "bookchapter",
                "itcerntalk",
                "antarescerntalk",
                "slides",
                "article",
                "preprint",
                "intnotetspubl",
                "intnoteitpubl",
                "intnotealephpriv",
                "intnoteeppubl",
                "intnotehsepubl",
                "note",
                "lcd-notes",
                "software",
            ]
        )
    }
    current = self.get("resource_type")

    # Normalize both values (lowercase if not None)
    candidates = []
    if value_a:
        candidates.append(value_a.lower())
    if value_b:
        candidates.append(value_b.lower())

    if not candidates:
        raise IgnoreKey("resource_type")  # nothing to decide on

    # Select the candidate with the highest priority (lowest rank)
    best_value = min(candidates, key=lambda v: priority.get(v, float("inf")))
    rank = priority.get(best_value, float("inf"))

    mapping = {
        "preprint": {"id": "publication-preprint"},
        "conferencepaper": {"id": "publication-conferencepaper"},
        "article": {"id": "publication-article"},
        "note": {"id": "publication-technicalnote"},
        "lcd-notes": {"id": "publication-technicalnote"},
        "brochure": {"id": "publication-brochure"},
        "itcerntalk": {"id": "presentation"},
        "antarescerntalk": {"id": "presentation"},
        "slides": {"id": "presentation"},
        "peri": {"id": "publication-periodical"},
        "intnoteitpubl": {"id": "publication-technicalnote"},
        "intnotealephpriv": {"id": "publication-technicalnote"},
        "intnotetspubl": {"id": "publication-technicalnote"},
        "intnoteeppubl": {"id": "publication-technicalnote"},
        "intnotehsepubl": {"id": "publication-technicalnote"},
        "bookchapter": {"id": "publication-section"},
        "cnlissue": {"id": "publication-periodicalissue"},
        "cnlarticle": {"id": "publication-periodicalarticle"},
        "report": {"id": "publication-report"},
        "book": {"id": "publication-book"},
        "progress report": {"id": "publication-report"},
        "poster": {"id": "poster"},
        "software": {"id": "software"},
    }

    try:

        mapping[best_value]
    except KeyError:
        if key == "697C_" and "lexi" in value_b.lower() or "lexi" in value_a.lower():
            subjects = self.get("subjects")
            subjects.append({"subject": value_a if value_a else value_b})
            self["subjects"] = subjects
            raise IgnoreKey("resource_type")
        raise UnexpectedValue(
            "Unknown resource type (Publications)", value=best_value, field=key
        )

    if current:
        current_key = next((k for k, v in mapping.items() if v == current), None)
        current_rank = priority.get(current_key, float("inf"))

        if rank < current_rank:
            return mapping[best_value]
        else:
            raise IgnoreKey("resource_type")
    else:
        return mapping[best_value]
