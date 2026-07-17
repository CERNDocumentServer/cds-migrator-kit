# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM request reviewer accounts migration rules.

Mirrors the 859__f "submitter" rule (see
cds_migrator_kit/transform/xml_processing/rules/base.py) which is used to
find/recreate the record owner's account: this extracts the reviewers
listed in 906__m/906__p, later used to find or recreate their accounts.
"""

from dojson.errors import IgnoreKey

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.transform.xml_processing.models.base import model


@model.over("reviewers", "^906__")
def record_reviewer(self, key, value):
    """Translate request reviewers.

    906__m holds the reviewer's email directly, same as 859__f does for
    the submitter. 906__p instead holds one or more "Family name, Given
    name" strings - multiple reviewers can be packed into a single
    occurrence, joined by a literal "\\n" - used as a fallback to later
    match or recreate the reviewer's account when no email is available.
    """
    reviewers = self.get("reviewers", [])

    email = value.get("m")
    if type(email) is tuple:
        raise UnexpectedValue(field=key, subfield="m", value=email)
    if email:
        email = email.strip().lower()
        if email not in reviewers:
            reviewers.append(email)

    name = value.get("p")
    if type(name) is tuple:
        raise UnexpectedValue(field=key, subfield="p", value=name)
    if name:
        for single_name in name.split("\\n"):
            single_name = single_name.strip()
            if single_name and single_name not in reviewers:
                reviewers.append(single_name)

    self["reviewers"] = reviewers
    raise IgnoreKey("reviewers")
