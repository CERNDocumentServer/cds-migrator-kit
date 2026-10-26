# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM record submitter account migration rule.

Registered directly on `submitter_model` (after it has been constructed, see
models/submitter.py) so the submitters migration does not need the unrelated
record rules of the shared `base_model`.
"""

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.rdm.users.transform.xml_processing.models.submitter import (
    submitter_model,
)


@submitter_model.over("submitter", "(^859__)")
def record_submitter(self, key, value):
    """Translate record submitter."""
    submitter = value.get("f")
    if type(submitter) is tuple:
        raise UnexpectedValue(field=key, subfield="f", value=submitter)
    if submitter:
        submitter = submitter.lower()
    return submitter
