# -*- coding: utf-8 -*-
#
# Copyright (C) 2022-2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Top-level ``record_json_output`` field mappers."""
from cds_migrator_kit.rdm.records.transform.mappers.base import FieldMapper


class AccessGrantsMapper(FieldMapper):
    """Maps access_grants, appending any configured collection-wide view grants."""

    id = "access_grants"

    def map_value(self, ctx):
        """Return access_grants extended with configured view grants."""
        access_grants = ctx.dojson_entry.pop("access_grants", [])
        if ctx.access_grants_view:
            for grant in ctx.access_grants_view:
                access_grants.append({str(grant): "view"})
        return access_grants
