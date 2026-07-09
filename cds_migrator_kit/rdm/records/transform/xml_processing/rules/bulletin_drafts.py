# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM Bulletin Drafts rules."""

from cds_migrator_kit.errors import UnexpectedValue

from ...models.bulletin_drafts import bulletin_drafts_model as model


@model.over("resource_type", "^980__", override=True)
def resource_type(self, key, value):
    """Translates resource_type for bulletin drafts."""
    value = value.get("a", "").lower()
    if value in [
        "bulletinstaffdraft",
        "bulletinnewsdraft",
        "bulletinofficialdraft",
        "bulletintrainingdraft",
        "bulletinannouncedraft",
        "bulletineventsdraft",
    ]:
        return {"id": "publication-periodicalarticle"}
    raise UnexpectedValue(
        "Unknown resource type (BULLETIN DRAFTS)", field=key, value=value
    )
