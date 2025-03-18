# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM migration rules module."""

import datetime

from dojson.errors import IgnoreKey
from dojson.utils import filter_values, flatten, force_list

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.rdm.records.transform.config import CONTROLLED_SUBJECTS_SCHEMES, \
    RECOGNISED_KEYWORD_SCHEMES, PID_SCHEMES_TO_STORE_IN_IDENTIFIERS
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


@model.over("_created", "(^916__)")
@require(["w"])
def created(self, key, value):
    """Translates created information to fields."""
    if "s" in value:
        source = clean_val("s", value, str)
        # h = human catalogued
        # n = script catalogued or via submission
        if source not in ["n", "h"]:
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


@model.over("subjects", "(^6931_)|(^650[1_][7_])|(^653[1_]_)")
@require(["a"])
@filter_list_values
def subjects(self, key, value):
    """Translates subjects fields."""

    def validate_subject_scheme(value, subfield):
        subject_scheme = value.get(subfield)

        if not subject_scheme:
            return True

        if type(subject_scheme) is not str:
            raise UnexpectedValue(field=key, subfield=subfield, value=subject_scheme)

        is_cern_scheme = (
            subject_scheme.lower() in CONTROLLED_SUBJECTS_SCHEMES
        )

        is_recognised = subject_scheme.lower() in RECOGNISED_KEYWORD_SCHEMES

        if not (is_cern_scheme or is_recognised):
            raise UnexpectedValue(field=key, subfield=subfield, value=subject_scheme)

    _subjects = self.get("subjects", [])
    subject_value = StringValue(value.get("a", "")).parse()

    validate_subject_scheme(value, "2")
    validate_subject_scheme(value, "9")

    if key == "65017":
        if subject_value:
            subject = {
                "id": subject_value,
                "subject": subject_value,
                # "scheme": "CERN", # scheme not accepted when ID is supplied
            }
            _subjects.append(subject)
    if key.startswith("653"):
        if subject_value:
            subject = {
                "subject": subject_value,
            }
            _subjects.append(subject)
    return _subjects


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
    scheme = value.get("9")
    if key == "037__" and scheme:
        if scheme.upper() in PID_SCHEMES_TO_STORE_IN_IDENTIFIERS:
            scheme = scheme.lower()
        else:
            raise UnexpectedValue("Unknown ID scheme", field=key, subfield="9",
                                  value=value)
    if (key == "037__" and not scheme) or key == "088__":
        # if there is no scheme, it means report number
        scheme = "cds_ref"
    if not identifier:
        raise UnexpectedValue("Missing ID value", field=key, value=value)
    return {"scheme": scheme, "identifier": identifier}


@model.over("identifiers", "^970__")
@for_each_value
def aleph_number(self, key, value):
    """Translates identifiers: ALEPH.

    Attention:  035 might contain aleph number
    https://github.com/CERNDocumentServer/cds-migrator-kit/issues/21
    """
    aleph = StringValue(value.get("a")).parse()
    if aleph:
        return {"scheme": "aleph", "identifier": aleph}


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

    if id_value:
        return {"scheme": scheme.lower(), "identifier": id_value}


@model.over("_pids", "^0247_", override=True)
def _pids(self, key, value):
    """Translates external_system_identifiers fields."""
    pid_dict = self.get("_pids", {})
    scheme = StringValue(value.get("2", "")).parse().lower()
    if not scheme:
        scheme = StringValue(value.get("9", "")).parse().lower()
    if not scheme:
        raise UnexpectedValue("Missing identifier scheme", field=key,
                              subfield="2",
                              stage="transform")
    identifier = StringValue(value.get("a")).parse()
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
        else:
            pid_dict["other"] = {"identifier": identifier}
        return pid_dict


@model.over("contributors", "^710__")
@for_each_value
def corporate_author(self, key, value):
    """Translates corporate author."""
    if "g" in value:
        contributor = {
            "person_or_org": {
                "type": "organizational",
                "name": StringValue(value.get("g")).parse(),
                "family_name": StringValue(value.get("g")).parse(),
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


@model.over("alternative_titles", "(^242__)|(^210__)")
@filter_list_values
def alternative_titles(self, key, value):
    """Translates title translations."""
    _alternative_titles = self.get("alternative_titles", [])
    if key == "210__":
        abbreviation = clean_val("a", value, str, req=True)
        _alternative_titles.append(
            {
                "title": abbreviation,
                "type": {"id:": "other"},
            }
        )
    elif "a" in value:
        _alternative_titles.append(
            {
                "title": clean_val("a", value, str, req=True),
                "type": {"id:": "translated-title"},
                "lang": {"id": "eng"},
            }
        )
    if "b" in value:
        _alternative_titles.append(
            {
                "title": clean_val("b", value, str, req=True),
                # should be translated subtitle, but we don't have it
                "type": {"id": "subtitle"},
                "lang": {"id": "eng"},
            }
        )
    return _alternative_titles


@model.over("title", "^245__", override=True)
def title(self, key, value):
    """Translates title."""
    title = StringValue(value.get("a"))
    subtitle = StringValue(value.get("b", "")).parse()
    title.required()
    if subtitle:
        alt_titles = self.get("alternative_titles", [])
        alt_titles.append({"title": subtitle,
                           "type": {"id": "subtitle"},
                           })
        self["alternative_titles"] = alt_titles
    return title.parse()


@model.over("licenses", "^540__")
@for_each_value
@filter_values
def licenses(self, key, value):
    """Translates license fields."""
    ARXIV_LICENSE = "arxiv.org/licenses/nonexclusive-distrib/1.0/"
    _license = dict()
    license_url = clean_val("u", value, str)
    license_id = clean_val("a", value, str)

    if not license_id:
        raise UnexpectedValue("License title missing",
                              field=key,
                              subfield="a",
                              value=value)
    license_id.lower()
    is_standard_license = True
    is_arxiv = "arxiv" in license_id
    if not license_id.startswith("CC"):
        is_standard_license = False

    if is_standard_license:
        license_id = license_id.replace(" ", "-")
        _license = {"id": license_id}
    else:
        if is_arxiv:
            license_url = ARXIV_LICENSE
        description = clean_val("g", value, str)
        _license = {"title": license_id,
                    "link": license_url,
                    "description": description}
    return _license


@model.over("identifiers", "^8564_")
@for_each_value
def urls(self, key, value):
    """Translates urls field."""
    # Contains description and restriction of the url
    # sub_y = clean_val("y", value, str, default="")
    # Value of the url
    sub_u = clean_val("u", value, str, req=True)
    if not sub_u:
        raise UnexpectedValue("Unrecognised string format or link missing.", field=key,
                              subfield="u", value=value)
    is_cds_file = False
    if all(x in sub_u for x in ["cds", ".cern.ch/record/", "/files"]):
        is_cds_file = True
    if is_cds_file:
        raise IgnoreKey("identifiers")
    else:
        return {"identifier": sub_u, "scheme": "url"}
