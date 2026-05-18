# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM FASER publication rules."""

from dojson.errors import IgnoreKey
from dojson.utils import for_each_value

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.decorators import require
from cds_migrator_kit.transform.xml_processing.quality.parsers import StringValue
from cds_migrator_kit.transform.xml_processing.rules.base import process_contributors

from ...models.faser_publication import faser_publication_model as model


@model.over("collection", "^690C_", override=True)
@for_each_value
def collection(self, key, value):
    """Translates collection field and ignores."""
    collection = value.get("a").strip()
    if collection.lower() not in [
        "cern",
        "faser",
        "article",
        "preprint",
        "scicom",
        "publlhcc",
    ]:
        raise UnexpectedValue(subfield="a", field=key, value=value)
    raise IgnoreKey("collection")


@model.over("access_grants", "^506[1_]_", override=True)
@for_each_value
def access_grants(self, key, value):
    """Translates access to comments field and ignores."""
    restriction_access_note = StringValue(value.get("a", "")).parse().strip().lower()
    if restriction_access_note and restriction_access_note not in [
        "faser-confnote",
        "faser-confpaper",
        "faser-preprint",
        "faser-slide",
    ]:
        raise UnexpectedValue(subfield="a", field=key, value=value)
    raise IgnoreKey("access_grants")


@model.over("faser_contributors", "^700__", override=True)
@for_each_value
@require(["a"])
def faser_contributors(self, key, value):
    """Translates contributors."""
    orcid_subfield = "j"
    if not value.get(orcid_subfield):
        orcid_subfield = "k"
    contributors = self.get("contributors", [])
    contributors.append(process_contributors(key, value, orcid_subfield=orcid_subfield))
    self["contributors"] = contributors
    raise IgnoreKey("faser_contributors")


@model.over("request_reviewers", "(^905__|^906__)", override=True)
@for_each_value
def spokesperson(self, key, value):
    """Translates spokesperson field into contributors."""
    email = value.get("m", "").strip().lower()
    # Tag 905__ is contact person, 906__ is project manager
    role = "contactperson" if key == "^905__" else "projectmanager"
    address = StringValue(value.get("a", "")).parse()
    person = StringValue(value.get("p", "")).parse()

    if not person:
        raise UnexpectedValue(subfield="p", field=key, value=value)

    person_or_org = {"type": "personal"}
    if person:
        names = person.split(",", 1)
        if len(names) == 2:
            person_or_org["family_name"] = names[0].strip()
            person_or_org["given_name"] = names[1].strip()
        else:
            person_or_org["family_name"] = person
        person_or_org["name"] = person

    contributor = {
        "person_or_org": person_or_org,
        "role": {"id": role},
    }
    if address:
        contributor["affiliations"] = [address]

    contributors = self.get("contributors", [])
    existing_names = {c.get("person_or_org", {}).get("name") for c in contributors}
    if person_or_org["name"] not in existing_names:
        contributors.append(contributor)
        self["contributors"] = contributors

    raise IgnoreKey("request_reviewers")
