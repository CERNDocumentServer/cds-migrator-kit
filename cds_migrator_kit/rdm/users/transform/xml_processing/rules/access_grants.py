# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM access grant accounts migration rules.

Mirrors the 859__f "submitter" / 906__m "reviewer" rules (see
cds_migrator_kit/transform/xml_processing/rules/base.py and
cds_migrator_kit/rdm/users/transform/xml_processing/rules/reviewers.py):
506 access restriction fields (see the `access_grants` rule in
cds_migrator_kit/rdm/records/transform/xml_processing/rules/research.py)
can name a person directly by email, in subfields d/m/a, instead of an
e-group/role name. Those emails need an account pre-created too, so the
record's actual access grant can later be resolved to a `User`
(cds_migrator_kit/rdm/records/transform/entities/parent.py, `resolve_grants`).

Registered directly on `submitter_model`, not on the shared `base_model`:
research.py/hr.py/it.py/faser_publication.py already register their own,
unrelated "^506[1_]_" rule on their own separate model instances, so this
rule must stay isolated to `submitter_model` to avoid clashing with those.
This is why it is imported at the bottom of
cds_migrator_kit/rdm/users/transform/xml_processing/models/submitter.py,
after `submitter_model` has been constructed.
"""

import re

from dojson.errors import IgnoreKey

from cds_migrator_kit.rdm.users.transform.xml_processing.models.submitter import (
    submitter_model,
)

EMAIL_PATTERN = re.compile(r"[^@]+@[^@]+\.[^@]+")


@submitter_model.over("access_grant_emails", "^506[1_]_")
def record_access_grant_emails(self, key, value):
    """Translate 506 access grant emails, ignoring e-group/role names."""
    emails = self.get("access_grant_emails", [])
    for subfield in ("d", "m", "a"):
        raw = value.get(subfield)
        if isinstance(raw, tuple):
            raw = raw[0]
        if not raw:
            continue
        candidate = raw.strip().lower()
        if EMAIL_PATTERN.match(candidate) and candidate not in emails:
            emails.append(candidate)
    self["access_grant_emails"] = emails
    raise IgnoreKey("access_grant_emails")
