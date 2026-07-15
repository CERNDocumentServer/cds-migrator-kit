import re
from datetime import datetime

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


@model.over("additional_descriptions", "^691__")
@for_each_value
@require(["a"])
def abbreviation(self, key, value):
    """Translates 691__a abbreviation into an additional description."""
    description_text = value.get("a")
    if not description_text:
        raise IgnoreKey("additional_descriptions")
    return {
        "description": f"Abbreviation: {description_text}",
        "type": {
            "id": "other",
        },
    }


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


@model.over("_approval", "(^591__)", override=True)
def status(self, key, value):

    val_a = value.get("a", "").lower().strip()
    val_b = value.get("b", "").lower().strip()

    self.setdefault("request_data", {})

    values = {val_a, val_b}
    known = {"draft", "approved", "not approved", ""}

    if values & known:
        self["request_data"]["status"] = (
            "rejected" if "not approved" in values else "accepted"
        )
    else:
        raise UnexpectedValue("Unexpected status value", field=key, value=value)

    raise IgnoreKey("_approval")


@model.over("custom_fields", "(^773__)")
def journal(self, key, value):
    _custom_fields = self.get("custom_fields", {})
    journal_fields = _custom_fields.get("journal:journal", {})
    year = StringValue(value.get("y", "")).parse()

    # p/n/v are journal-specific; c alone with w is a conference proceedings artid
    is_journal = any(f in value for f in ["p", "n", "v"])
    is_journal_year = any(f in value for f in ["p", "n", "v", "c"])

    conference_cnum = value.get("w", "")
    conference_acronym = value.get("q", "")
    meetings = _custom_fields.get("meeting:meeting", [])
    if conference_cnum or conference_acronym:
        session = StringValue(value.get("c", "")).parse()
        new_meeting = {}
        if conference_cnum:
            new_meeting["identifiers"] = [
                {"scheme": "inspire", "identifier": conference_cnum}
            ]
        if conference_acronym:
            new_meeting["acronym"] = conference_acronym
        if session:
            new_meeting["session"] = session
        meetings.append(new_meeting)
        _custom_fields["meeting:meeting"] = meetings

    pub_date = self.get("publication_date")
    # if we only have 773 in the record and no other journal fields,
    # it is not journal date
    if not is_journal_year and "y" in value and not pub_date:
        self["publication_date"] = year

    # Only populate journal fields from a journal 773 (has p/n/v).
    # A 773 with only 'c'+'w' is a conference proceedings reference and must
    # not overwrite journal data extracted from the sibling journal 773.
    if is_journal:
        journal_fields["title"] = StringValue(value.get("p", "")).parse()
        journal_fields["issue"] = StringValue(value.get("n", "")).parse()
        journal_fields["volume"] = StringValue(value.get("v", "")).parse()
        journal_fields["pages"] = StringValue(value.get("c", "")).parse()
    _custom_fields["journal:journal"] = journal_fields
    return _custom_fields


@model.over("custom_fields", "(^111__)|(^711__)", override_tag=True)
def meeting(self, key, value):
    """Translates meeting name entries (111__, 711__) into meeting:meeting.

    111__a -> title, 111__c -> place, 111__g -> acronym. 111__d -> dates; if
    missing, fall back to 111__9 (parsed as YYYYMMDD), then to 111__f (a bare
    year). 711__a -> title, but only if no meeting entry with that title
    already exists (711 is an added entry, often duplicating the 111
    conference name).
    """
    _custom_fields = self.get("custom_fields", {})
    meetings = _custom_fields.get("meeting:meeting", [])

    if key.startswith("111"):
        title = StringValue(value.get("a", "")).parse()
        place = StringValue(value.get("c", "")).parse()
        date_recon = StringValue(value.get("9", "")).parse()
        if date_recon:
            try:
                date_recon = datetime.strptime(date_recon, "%Y%m%d").strftime(
                    "%Y-%m-%d"
                )
            except ValueError:
                raise UnexpectedValue(
                    "Can't parse meeting date (111__9)",
                    field=key,
                    subfield="9",
                    value=value,
                )
        date_year = StringValue(value.get("f", "")).parse()
        dates = StringValue(value.get("d", "")).parse() or date_recon or date_year
        acronym = StringValue(value.get("g", "")).parse()

        new_meeting = {}
        if title:
            new_meeting["title"] = title
        if place:
            new_meeting["place"] = place
        if dates:
            new_meeting["dates"] = dates
        if acronym:
            new_meeting["acronym"] = acronym
        if new_meeting:
            meetings.append(new_meeting)
    else:
        title = StringValue(value.get("a", "")).parse()
        if title and not any(m.get("title") == title for m in meetings):
            meetings.append({"title": title})

    if meetings:
        _custom_fields["meeting:meeting"] = meetings
    self["custom_fields"] = _custom_fields
    raise IgnoreKey("custom_fields")


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


@model.over("request_reviewers", "^906__", override=True)
@for_each_value
def request_reviewers(self, key, value):
    name = StringValue(value.get("p", "")).parse().strip()

    if "," in name:
        last, first = (part.strip() for part in name.split(",", 1))
    else:
        last, first = name, ""

    reviewer = " ".join(part for part in (first, last) if part)

    if reviewer:
        request_data = self.setdefault("request_data", {})
        reviewers = request_data.setdefault("reviewers", [])

        if reviewer not in reviewers:
            reviewers.append(reviewer)

    raise IgnoreKey("request_reviewers")


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


@model.over("dates", "^583__")
@for_each_value
def deadline_date(self, key, value):
    """Translates deadline date."""
    dates = self.get("dates", [])
    action_note = value.get("z", "")
    if action_note and action_note.strip().upper() != "UNKNOWN":
        raise UnexpectedValue("Unexpected value in 583__z", field=key, value=value)
    deadline = value.get("c", "")
    if deadline and deadline.strip().upper() != "UNKNOWN":
        try:
            deadline = normalize(deadline)
        except (ParserError, TypeError) as e:
            raise UnexpectedValue(
                field=key,
                value=value,
                message=f"Can't parse provided deadline date. Value: {deadline}",
            )
        dates.append(
            {
                "date": deadline,
                "type": {"id": "other"},
                "description": "Deadline date",
            }
        )
    self["dates"] = dates
    raise IgnoreKey("dates")


@model.over("related_identifiers", "(^962__)|(^518__)")
@for_each_value
def related_identifiers(self, key, value):
    """Translates related identifiers and meeting date (518__r/__d)."""
    if key.startswith("518"):
        meeting_date_d = value.get("d", "").strip()
        meeting_date_r = value.get("r", "").strip()
        meeting_date = meeting_date_d or meeting_date_r
        if not meeting_date:
            raise IgnoreKey("related_identifiers")

        meetings = self.get("custom_fields", {}).get("meeting:meeting", [])
        if len(meetings) > 1:
            raise UnexpectedValue(
                "Can't determine which meeting the 518__r/d date belongs to, "
                "more than one meeting present",
                field=key,
                value=value,
            )
        try:
            meeting_date = normalize(meeting_date)
        except (ParserError, TypeError):
            if meeting_date.upper() == "CERN":
                # not a parseable date (e.g. "CERN") - ignore silently
                raise IgnoreKey("related_identifiers")
            raise UnexpectedValue("Can't parse meeting date (518__")
        # 518 is processed before 962, so the meeting entry doesn't exist yet -
        # create the first (only) meeting entry for 962 to fill in later.
        if not meetings:
            meetings.append({})
        meetings[0]["dates"] = meeting_date
        self["custom_fields"]["meeting:meeting"] = meetings
        raise IgnoreKey("related_identifiers")

    recid = value.get("b")
    artid = value.get("k", "")
    try:
        conference = value.get("n", "").lower().strip()
        meetings = self.get("custom_fields", {}).get("meeting:meeting", [])
        if conference:
            matching_meeting = next(
                (m for m in meetings if artid and m.get("session") == artid),
                None,
            )
            if matching_meeting is not None:
                matching_meeting["title"] = conference
            elif len(meetings) == 1 and "title" not in meetings[0]:
                # first meeting entry was created by 518 (dates only, no
                # session/title yet) - fill it in instead of appending a
                # duplicate entry
                meetings[0]["title"] = conference
            else:
                meetings.append({"title": conference})
        self["custom_fields"]["meeting:meeting"] = meetings
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
        "resource_type": {"id": "publication-conferenceproceeding"},
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
        "slintnote",
        "indico",
        "re29_papers",
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
                "proceedings",
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
        "alephdraft": {"id": "publication-other"},
        "preprint": {"id": "publication-preprint"},
        "conferencepaper": {"id": "publication-conferencepaper"},
        "proceedings": {"id": "publication-conferenceproceeding"},
        "article": {"id": "publication-article"},
        "note": {"id": "publication-technicalnote"},
        "lcd-notes": {"id": "publication-technicalnote"},
        "brochure": {"id": "publication-brochure"},
        "itcerntalk": {"id": "presentation"},
        "talk": {"id": "presentation"},
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
