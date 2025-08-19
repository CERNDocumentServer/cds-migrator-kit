# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM migration rules module."""

import datetime
import logging
import re
from urllib.parse import ParseResult, urlparse

from dateutil.parser import ParserError, parse
from dojson.errors import IgnoreKey
from dojson.utils import filter_values, flatten, force_list
from idutils.validators import is_doi, is_handle, is_urn
from invenio_accounts.models import User

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.rdm.records.transform.config import (
    CONTROLLED_SUBJECTS_SCHEMES,
    KEYWORD_SCHEMES_TO_DROP,
    PID_SCHEMES_TO_STORE_IN_IDENTIFIERS,
    RECOGNISED_KEYWORD_SCHEMES,
    udc_pattern,
)
from cds_migrator_kit.rdm.records.transform.models.base_record import (
    rdm_base_record_model as model,
)
from cds_migrator_kit.transform.xml_processing.quality.dates import get_week_start
from cds_migrator_kit.transform.xml_processing.quality.decorators import (
    filter_list_values,
    for_each_value,
    require,
    strip_output,
)
from cds_migrator_kit.transform.xml_processing.quality.parsers import (
    StringValue,
    clean_str,
    clean_val,
)

cli_logger = logging.getLogger("migrator")


@model.over("legacy_recid", "^001", override=True)
def recid(self, key, value):
    """Record Identifier."""
    identifiers = self.get("identifiers", [])
    new_id = {"identifier": value, "scheme": "lcds"}
    if new_id not in identifiers:
        identifiers.append(new_id)
        self["identifiers"] = identifiers
    self["recid"] = value
    return int(value)


@model.over("status_week_date", "(^916__)")
@require(["w"])
def created(self, key, value):
    """Translates created information to fields."""
    if "s" in value:
        source = clean_val("s", value, str)
        # h = human catalogued
        # n = script catalogued or via submission
        if source not in ["n", "h", "m", "r"]:
            raise UnexpectedValue(subfield="s", key=key, value=value)
    date_values = value.get("w")
    if not date_values or not date_values[0]:
        return datetime.date.today().isoformat()
    if isinstance(date_values, list):
        date = min(date_values)
    if isinstance(date_values, tuple):
        date = int(date_values[0])
    else:
        date = int(date_values)
    try:
        if date:
            if not (100000 < int(date) < 999999):
                raise UnexpectedValue("Wrong date format", field=key, subfield="w")
            year, week = str(date)[:4], str(date)[4:]
            date = get_week_start(int(year), int(week))
            if date < datetime.date.today():
                return date.isoformat()
            else:
                return datetime.date.today().isoformat()
    except ValueError:
        return datetime.date.today().isoformat()


@model.over("subjects", "(^6931_)|(^650[12_][7_])|(^653[12_]_)|(^695__)|(^694__)")
@require(["a"])
@for_each_value
def subjects(self, key, value):
    """Translates subjects fields."""

    def validate_subject_scheme(subject_scheme, subfield, key):
        if not subject_scheme:
            return True

        is_cern_scheme = subject_scheme.lower() in CONTROLLED_SUBJECTS_SCHEMES

        is_recognised = subject_scheme.lower() in RECOGNISED_KEYWORD_SCHEMES
        is_freetext = key.startswith("653")
        is_euproject_info = subject_scheme.lower() in [
            "aida",
            "eucard",
            "eucard2",
            "aida-2020",
        ]

        if is_euproject_info:
            return "eu"
        if is_freetext:
            return True
        if not (is_cern_scheme or is_recognised):
            raise UnexpectedValue(field=key, subfield=subfield, value=subject_scheme)

    # subject
    val_a = value.get("a", "")

    subfield = "2" if "2" in value else "9"
    scheme = value.get("2", "")
    if not scheme:
        scheme = value.get("9", "")

    if type(scheme) is not str:
        raise UnexpectedValue(field=key, subfield=subfield, value=value)

    if not scheme and key == "65017":
        # assume scheme
        scheme = "szgecern"
        # raise UnexpectedValue(field=key, subfield=subfield, value=scheme)

    scheme = scheme.lower()
    if scheme in KEYWORD_SCHEMES_TO_DROP:
        raise IgnoreKey("subjects")

    is_keyword_field = any(
        [key.startswith(x) for x in ["653", "693", "695", "694", "65027"]]
    )
    is_keyword = is_keyword_field or (
        key == "65017" and scheme in RECOGNISED_KEYWORD_SCHEMES
    )

    is_controlled_subject = key == "65017" and (scheme in CONTROLLED_SUBJECTS_SCHEMES)

    if type(val_a) is tuple:
        # sometimes keywords are stick in one tag, so they come out as tuple
        s_values = val_a
        _subjects = []
        for _value in s_values:
            try:
                subj = subjects(self, key, {"a": _value, subfield: scheme})
                _subjects += subj
            except IgnoreKey as e:
                # ignore the exceptions to pass to next keyword
                pass
        subjects_list = self.get("subjects", [])
        subjects_list += _subjects
        self["subjects"] = subjects_list
        raise IgnoreKey("subjects")
    else:
        subject_value = val_a.strip()
        _subjects = self.get("subjects", [])
        # invalid schema = euproject info    scheme = scheme
        if validate_subject_scheme(scheme, subfield, key) == "eu":
            descriptions = self.get("additional_descriptions", [])
            b_sub = value.get("b")
            desc = {
                "description": f"{subject_value} ({b_sub})",
                "type": {"id": "technical-info"},
            }
            if desc not in descriptions:
                descriptions.append(desc)
                self["additional_descriptions"] = descriptions
            raise IgnoreKey("subjects")

        if is_controlled_subject:
            if subject_value:
                subject_value = (
                    subject_value.title()
                    .replace(" And ", " and ")
                    .replace(" In ", " in ")
                    .replace(" Of ", " of ")
                )
                subject = {
                    "id": subject_value,
                    "subject": subject_value,
                    # "scheme": "CERN", # scheme not accepted when ID is supplied
                }
                _subjects.append(subject)
                self["subjects"] = _subjects
            raise IgnoreKey("subjects")

        elif is_keyword:
            if subject_value:
                subject = {
                    "subject": subject_value,
                }
                _subjects.append(subject)
                self["subjects"] = _subjects
            raise IgnoreKey("subjects")
        else:
            raise UnexpectedValue(
                "unrecognised Subject value and scheme.", field=key, value=value
            )


@model.over("custom_fields", "(^693__)")
def custom_fields(self, key, value):
    """Translates custom fields."""
    _custom_fields = self.get("custom_fields", {})
    experiments, accelerators, projects, facilities, studies, beams = (
        [],
        [],
        [],
        [],
        [],
        [],
    )
    if key == "693__":
        if "e" in value and value.get("e"):
            experiments += [StringValue(v).parse() for v in force_list(value.get("e"))]
        if "a" in value and value.get("a"):
            accelerators += [StringValue(v).parse() for v in force_list(value.get("a"))]
        if "p" in value and value.get("p"):
            projects += [StringValue(v).parse() for v in force_list(value.get("p"))]
        if "f" in value and value.get("f"):
            facilities += [StringValue(v).parse() for v in force_list(value.get("f"))]
        if "s" in value and value.get("s"):
            studies += [StringValue(v).parse() for v in force_list(value.get("s"))]
        if "b" in value and value.get("b"):
            beams += [StringValue(v).parse() for v in force_list(value.get("b"))]

        _custom_fields["cern:experiments"] = experiments
        _custom_fields["cern:accelerators"] = accelerators
        _custom_fields["cern:projects"] = projects
        _custom_fields["cern:facilities"] = facilities
        _custom_fields["cern:studies"] = studies
        _custom_fields["cern:beams"] = beams
    return _custom_fields


@model.over("record_restriction", "^963__")
def record_restriction(self, key, value):
    """Translate record restriction field."""
    restr = value.get("a", "")
    parsed = StringValue(restr).parse()
    if parsed == "PUBLIC":
        return "public"
    else:
        raise UnexpectedValue(
            field="963", subfield="a", message="Record restricted", priority="critical"
        )


@model.over("identifiers", "(^037__)|(^088__)")
@for_each_value
def report_number(self, key, value):
    """Translates report_number fields."""
    identifier = value.get("a", "")
    identifier = StringValue(identifier).parse()
    existing_ids = self.get("identifiers", [])
    scheme = value.get("2")
    provenance = value.get("9", "")
    if not scheme:
        scheme = provenance
        is_hidden_report_number = scheme.upper().startswith("CERN-")
        if is_hidden_report_number:
            scheme = None
            identifier = value.get("9")
    if key == "037__" and scheme:
        if scheme.lower() == "urn":
            if not is_urn(identifier) and is_handle(identifier):
                scheme = "handle"
        if scheme.lower() == "hdl":
            scheme = "handle"
        if scheme == "arXiv:reportnumber":
            scheme = "cds_ref"
        if scheme.upper() in PID_SCHEMES_TO_STORE_IN_IDENTIFIERS:
            scheme = scheme.lower()
    if key == "037__" and "n" in value:
        # this means we have URN/HAL schema (only one record in thesis)
        if value.get("n", "") != "URN/HAL":
            raise UnexpectedValue(field=key, value=value, subfield="n")
        scheme = "handle"
    if (key == "037__" and not scheme) or (identifier and key == "088__"):
        # if there is no scheme, it means report number
        scheme = "cds_ref"

    # if there is no identifier it means something else was stored in __9
    if not identifier:
        if re.findall(udc_pattern, scheme):
            raise IgnoreKey("identifiers")
        elif scheme.startswith("CM-"):
            # barcode, to drop
            raise IgnoreKey("identifiers")
        elif scheme.upper().startswith("P00"):
            # barcode, to drop
            raise IgnoreKey("identifiers")
        elif scheme.upper() == "CERN LIBRARY":
            raise IgnoreKey("identifiers")
        else:
            raise UnexpectedValue("Missing ID value", field=key, value=value)
    new_id = {"scheme": scheme, "identifier": identifier}
    if new_id in existing_ids:
        raise IgnoreKey("identifiers")
    return new_id


@model.over("identifiers", "^970__")
@for_each_value
def aleph_number(self, key, value):
    """Translates identifiers: ALEPH.

    Attention:  035 might contain aleph number
    https://github.com/CERNDocumentServer/cds-migrator-kit/issues/21
    """
    aleph = StringValue(value.get("a")).parse()
    identifiers = self.get("identifiers")
    new_id = {"scheme": "aleph", "identifier": aleph}
    if aleph and new_id not in identifiers:
        return {"scheme": "aleph", "identifier": aleph}
    else:
        raise IgnoreKey("identifiers")


@model.over("identifiers", "^035__")
@for_each_value
def identifiers(self, key, value):
    """Translates identifiers.

    Attention: might contain aleph number
    https://github.com/CERNDocumentServer/cds-migrator-kit/issues/21
    """
    id_value = StringValue(value.get("a", "")).parse()
    scheme = StringValue(value.get("9", "")).parse()

    # drop oai harvest info
    if id_value.startswith("oai:inspirehep.net"):
        raise IgnoreKey("identifiers")
    if scheme.lower() == "cern annual report":
        additional_descriptions = self.get("additional_descriptions", [])
        new_desc = {
            "description": f"{scheme} {id_value}",
            "type": {"id": "series-information"},
        }
        additional_descriptions.append(new_desc)
        self["additional_descriptions"] = additional_descriptions
        related_works = self.get("related_identifiers", [])
        new_id = {
            "identifier": f"cern-annual-report:{id_value}",
            "scheme": "other",
            "relation_type": {"id": "ispartof"},
            "resource_type": {"id": "publication-report"},
        }
        related_works.append(new_id)
        self["related_identifiers"] = related_works
        raise IgnoreKey("identifiers")
    is_aleph_number = scheme.lower() == "cercer" or not scheme and "CERCER" in id_value
    if is_aleph_number:
        scheme = "aleph"
    if id_value:
        return {"scheme": scheme.lower(), "identifier": id_value}


@model.over("_pids", "^0247_", override=True)
@for_each_value
def _pids(self, key, value):
    """Translates external_system_identifiers fields."""
    pid_dict = self.get("_pids", {})
    scheme = value.get("2", "").lower()
    qualifier = value.get("q", "").lower().strip()
    identifier = value.get("a")

    if not scheme:
        scheme = value.get("9", "").lower()
    if not scheme:
        raise UnexpectedValue(
            "Missing identifier scheme", field=key, subfield="2", stage="transform"
        )

    is_doi_id = is_doi(identifier)
    is_handle_id = not is_doi_id and is_handle(identifier)
    if not is_doi_id and is_handle_id and (scheme == "doi" or scheme == "urn/hdl"):
        scheme = "handle"

    if qualifier == "publication" or qualifier == "thesis":
        # if we have a qualifier for one of these two, we know it references
        # an external resource (checked in DB and individually on records)
        # if qualifier == ebook, it references itself, so qualifier is not needed
        if qualifier == "thesis":
            qualifier = "publication-thesis"
        related_works = self.get("related_identifiers", [])
        new_id = {
            "identifier": identifier,
            "scheme": scheme,
            "relation_type": {"id": "isversionof"},
            "resource_type": {"id": qualifier},
        }
        if new_id not in related_works:
            related_works.append(new_id)
        self["related_identifiers"] = related_works
        raise IgnoreKey("_pids")

    if scheme.upper() in PID_SCHEMES_TO_STORE_IN_IDENTIFIERS:
        if scheme == "hdl":
            scheme = "handle"
        ids = self.get("identifiers", [])
        ids.append({"scheme": scheme, "identifier": identifier})
        self["identifiers"] = ids
        raise IgnoreKey("_pids")
    else:
        if scheme:
            pid_dict[scheme] = {"identifier": identifier}
        elif is_doi_id and not scheme:
            pid_dict["doi"] = {"identifier": identifier}
        else:
            raise UnexpectedValue(
                "Missing identifier scheme", field=key, subfield="2", stage="transform"
            )
        self["_pids"] = pid_dict
        raise IgnoreKey("_pids")


@model.over("contributors", "^710__")
@for_each_value
def corporate_author(self, key, value):
    """Translates corporate author."""
    if "g" in value or "a" in value:
        name = value.get("g") if "g" in value else value.get("a")

        if name.strip() == "CERN. Geneva":
            name = "CERN"
        contributor = {
            "person_or_org": {
                "type": "organizational",
                "name": StringValue(name).parse(),
                "family_name": StringValue(name).parse(),
            },
            "role": {"id": "hostinginstitution"},
        }
        return contributor
    if "5" in value:
        department = StringValue(value.get("5")).parse()
        departments = self.get("custom_fields", {}).get("cern:departments", [])
        if department and department not in departments:
            departments.append(department)
        self["custom_fields"]["cern:departments"] = departments
        raise IgnoreKey("contributors")
    raise IgnoreKey("contributors")


@model.over("additional_titles", "(^242__)")
@for_each_value
def additional_titles(self, key, value):
    """Translates title translations."""
    _additional_titles = self.get("additional_titles", [])
    if "a" in value:
        _additional_titles.append(
            {
                "title": clean_val("a", value, str, req=True),
                "type": {"id": "translated-title"},
                "lang": {"id": "eng"},
            }
        )
    if "b" in value:
        _additional_titles.append(
            {
                "title": clean_val("b", value, str, req=True),
                # should be translated subtitle, but we don't have it
                "type": {"id": "subtitle"},
                "lang": {"id": "eng"},
            }
        )
    if _additional_titles:
        self["additional_titles"] = _additional_titles
    raise IgnoreKey("additional_titles")


@model.over("additional_descriptions", "^210__")
@for_each_value
def additional_titles(self, key, value):
    """Translates title translations."""
    _additional_descriptions = self.get("additional_descriptions", [])
    abbreviation = clean_val("a", value, str, req=True)
    _additional_descriptions.append(
        {
            "title": abbreviation,
            "type": {"id": "other"},
        }
    )
    if _additional_descriptions:
        self["additional_titles"] = _additional_descriptions
    raise IgnoreKey("additional_titles")


@model.over("title", "^245__", override=True)
def title(self, key, value):
    """Translates title."""
    title = StringValue(value.get("a"))
    subtitle = StringValue(value.get("b", "")).parse()
    title.required()
    if subtitle:
        alt_titles = self.get("additional_titles", [])
        alt_titles.append(
            {
                "title": subtitle,
                "type": {"id": "subtitle"},
            }
        )
        self["additional_titles"] = alt_titles
    return title.parse()


@model.over("rights", "^540__")
@for_each_value
@filter_values
def licenses(self, key, value):
    """Translates license fields."""
    ARXIV_LICENSE = "arxiv.org/licenses/nonexclusive-distrib/1.0/"
    _license = dict()
    license_url = clean_val("u", value, str)
    license_id = clean_val("a", value, str)
    if "b" in value:
        imposing = value.get("b")
        self["copyright"] = f"© {imposing}.".strip()
    # 2897660, 2694245, 684383
    if license_id in [
        "CC BY-NC-ND 3.0 US",
        "CC-BY-NC-ND-3.0-DE",
        "CC-BY-3.0-DE",
        "CC-BY",
    ]:
        return {
            "title": {"en": license_id},
            "link": license_url,
            # "description": description,
        }
    if not license_id:
        raise UnexpectedValue(
            "License title missing", field=key, subfield="a", value=value
        )
    license_id.lower()
    is_standard_license = True
    is_arxiv = "arxiv" in license_id

    if not license_id.startswith("CC"):
        is_standard_license = False

    if is_standard_license:
        license_id = license_id.replace(" ", "-").lower()
        _license = {"id": license_id}
    else:
        if is_arxiv:
            license_url = ARXIV_LICENSE
        description = clean_val("g", value, str)
        _license = {
            "title": {"en": license_id},
            "link": license_url,
            "description": description,
        }
    return _license


@model.over("copyright", "^542__")
def copyrights(self, key, value):
    """Translate copyright."""
    holder = value.get("d", "")
    statement = value.get("f", "")
    year = value.get("g", "")
    url = value.get("u", "")

    return f"{year} © {holder}. {statement} {url}".strip()


@model.over("identifiers", "^8564_")
@for_each_value
def urls(self, key, value):
    """Translates urls field."""
    # Contains description and restriction of the url
    # sub_y = clean_val("y", value, str, default="")
    # Value of the url
    sub_u = clean_val("u", value, str, req=True)
    if not sub_u:
        raise UnexpectedValue(
            "Unrecognised string format or link missing.",
            field=key,
            subfield="u",
            value=value,
        )
    is_cds_file = False
    if all(x in sub_u for x in ["cds", ".cern.ch/record/", "/files"]):
        is_cds_file = True
    if is_cds_file:
        raise IgnoreKey("identifiers")
    else:
        p = urlparse(sub_u, "http")
        netloc = p.netloc or p.path
        path = p.path if p.netloc else ""
        if not netloc.startswith("www."):
            netloc = "www." + netloc

        p = ParseResult("http", netloc, path, *p[3:])
        return {"identifier": p.geturl(), "scheme": "url"}


@model.over("additional_descriptions", "^490__")
@for_each_value
def series_information(self, key, value):
    """Translate series information."""
    sub_a = value.get("a", "").strip()
    sub_v = value.get("v", "").strip()

    series = f"{sub_a}"
    if sub_v:
        series = f"{series} ({sub_v})"
    related_works = self.get("related_identifiers", [])
    ids = []
    if sub_a.lower() == "springer theses":
        new_id = {
            "identifier": "2190-5053",
            "scheme": "issn",
            "relation_type": {"id": "ispartof"},
            "resource_type": {"id": "publication-other"},
        }
        new_e_id = {
            "identifier": "2190-5053",
            "scheme": "issn",
            "relation_type": {"id": "ispartof"},
            "resource_type": {"id": "publication-other"},
        }
        ids.append(new_id)
        ids.append(new_e_id)

    if sub_a.lower() == "Springer tracts in modern physics":
        new_id = {
            "identifier": "0081-3869",
            "scheme": "issn",
            "relation_type": {"id": "ispartof"},
            "resource_type": {"id": "publication-other"},
        }
        new_e_id = {
            "identifier": "1615-0430",
            "scheme": "issn",
            "relation_type": {"id": "ispartof"},
            "resource_type": {"id": "publication-other"},
        }
        ids.append(new_id)
        ids.append(new_e_id)

    for rel_id in ids:
        if rel_id not in related_works:
            related_works.append(rel_id)

    self["related_identifiers"] = related_works
    return {"description": series, "type": {"id": "series-information"}}


@model.over("related_identifiers", "^084__")
@for_each_value
def yellow_reports(self, key, value):
    """Translate related records - yellow reports."""
    scheme = value.get("2", "").strip()
    provenance = value.get("9", "").strip()
    identifier = value.get("a", "").strip()

    if scheme.upper() == "CERN LIBRARY":
        raise IgnoreKey("related_identifiers")
    if provenance.lower() == "pacs":
        raise IgnoreKey("related_identifiers")

    if scheme and scheme.lower() == "cern yellow report":
        new_id = {
            "identifier": identifier,
            "scheme": "cds_ref",
            "relation_type": {"id": "ispublishedin"},
            "resource_type": {"id": "publication-report"},
        }
        return new_id
    if scheme.lower() == "pacs":
        raise IgnoreKey("related_identifiers")
    if not scheme and identifier.startswith("CERN-"):
        # report number
        new_id = {
            "identifier": identifier,
            "scheme": "cds_ref",
        }
        identifiers = self.get("identifiers", [])
        if new_id not in identifiers:
            identifiers.append(new_id)
            self["identifiers"] = identifiers
        raise IgnoreKey("related_identifiers")

    raise UnexpectedValue("Unknown value found.", field=key, value=value)


@model.over("related_identifiers", "^7870_")
@for_each_value
def related_identifiers(self, key, value):
    """Translates related identifiers."""
    description = value.get("i")
    recid = value.get("w")
    rel_ids = self.get("related_identifiers", [])
    new_id = {
        "identifier": f"https://cds.cern.ch/record/{recid}",
        "scheme": "url",
        "relation_type": {"id": "references"},
    }
    if new_id not in rel_ids:
        return new_id
    raise IgnoreKey("related_identifiers")


@model.over("related_identifiers", "^775_")
@for_each_value
def related_identifiers(self, key, value):
    """Translates related identifiers."""
    description = value.get("b")
    year = value.get("c")
    recid = value.get("w")
    rel_ids = self.get("related_identifiers", [])
    new_id = {
        "identifier": f"https://cds.cern.ch/record/{recid}",
        "scheme": "url",
        "relation_type": {"id": "references"},
    }
    if new_id not in rel_ids:
        return new_id
    raise IgnoreKey("related_identifiers")


@model.over("_clc_sync", "^599__")
def sync(self, key, value):
    """Translates related identifiers."""
    sync = value.get("a")
    if sync in ["ILSSYNC", "ILSLINK"]:
        return True
    return False


@model.over("publication_date", "(^260__)", override=True)
def imprint_info(self, key, value):
    """Translates publication_date field."""

    publication_date_str = value.get("c")
    if publication_date_str:
        try:
            date_obj = parse(publication_date_str)
            return date_obj.strftime("%Y-%m-%d")
        except (ParserError, TypeError) as e:
            raise UnexpectedValue(
                field=key,
                value=value,
                message=f"Can't parse provided publication date. Value: {publication_date_str}",
            )
    raise IgnoreKey("publication_date")


@model.over("custom_fields", "(^269__)")
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
            date_obj = parse(publication_date_str)
            self["publication_date"] = date_obj.strftime("%Y-%m-%d")
        except (ParserError, TypeError) as e:
            raise UnexpectedValue(
                field=key,
                value=value,
                message=f"Can't parse provided publication date. Value: {publication_date_str}",
            )
    raise IgnoreKey("custom_fields")


@model.over("internal_notes", "^595__")
@for_each_value
def note(self, key, value):
    """Translates notes."""

    def process(_note):
        if _note:
            if _note.strip().lower() in [
                "cern invenio websubmit",
                "cern eds",
                "cds",
                "lanl eds",
                "clas1",
            ]:
                return
            return {"note": _note}

    _note = force_list(value.get("a", ""))
    _note_z = force_list(value.get("z", ""))
    notes_list = _note_z + _note
    _note_b = value.get("b", "")
    _note_c = value.get("c", "")

    is_gensbm_tag = (
        "".join(_note).strip() == "CERN Invenio WebSubmit"
        and _note_b.strip() in ("GENSBM", "GENEU")
        or _note_c.strip() == "1"
    )
    if is_gensbm_tag:
        raise IgnoreKey("internal_notes")
    elif (
        _note_b.strip() not in ("GENSBM", "GENEU")
        and "".join(_note).strip() == "CERN Invenio WebSubmit"
    ):
        raise UnexpectedValue("invalid internal notes", field=key, value=value)

    notes = self.get("internal_notes", [])
    for item in notes_list:
        res = process(item)
        if res:
            notes.append(res)

    self["internal_notes"] = notes
    raise IgnoreKey("internal_notes")


@model.over("additional_titles", "(^246__)")
@for_each_value
@require(["a"])
def additional_titles(self, key, value):
    """Translates additional titles."""
    description_text = value.get("a")
    translated_subtitle = value.get("b")
    source = value.get("9")
    is_abbreviation = value.get("i") == "Abbreviation"

    if is_abbreviation:
        additional_descriptions = self.get("additional_descriptions", [])

        additional_descriptions.append(
            {
                "description": f"Abbreviation: {description_text}",
                "type": {
                    "id": "other",  # what's with the lang
                },
            }
        )
        self["additional_descriptions"] = additional_descriptions
        raise IgnoreKey("additional_titles")
    if source:
        _additional_title = {
            "title": description_text,
            "type": {
                "id": "alternative-title",
            },
        }
    else:
        _additional_title = {
            "title": description_text,
            "type": {
                "id": "translated-title",
            },
        }
    if description_text and _additional_title:
        return _additional_title
    if translated_subtitle:
        _additional_title = {
            "title": translated_subtitle,
            "type": {
                "id": "translated-title",
            },
            "lang": {"id": "eng"},
        }
        return _additional_title
    raise IgnoreKey("additional_titles")


@model.over("access_grants", "(^270__)|(^506[1]_)")
@for_each_value
def access_grants(self, key, value):
    """Translates access permissions (by user email or group name)."""
    subject_identifier = ""
    email_regex = re.compile(r"[^@]+@[^@]+\.[^@]+")

    if value.get("d"):  # Always expected to be a user email address
        subject_identifier = StringValue(value.get("d")).parse()
        if not (
            email_regex.fullmatch(subject_identifier)
            and User.query.filter_by(email=subject_identifier).one_or_none()
        ):
            raise UnexpectedValue(
                f"Expected a valid user email, got :'{subject_identifier}'",
                field=key,
                value=value,
                priority="critical",
            )
    else:  # Can be a user email address or a group id
        subject_identifier = StringValue(value.get("m")).parse()

    if subject_identifier:
        permission_type = "manage" if key == "270__" else "view"
        return {str(subject_identifier): permission_type}
    raise IgnoreKey("access_grants")
