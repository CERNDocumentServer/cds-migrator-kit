# -*- coding: utf-8 -*-
#
# Copyright (C) 2022-2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Top-level ``record_json_output`` field mappers."""

from cds_migrator_kit.rdm.records.transform.entities.restrictions import (
    RecordRestrictions,
)
from cds_migrator_kit.rdm.records.transform.mappers.base import (
    FieldMapper,
    RecordTransformContext,
)


class AccessGrantsMapper(FieldMapper):
    """Maps the access grants from the dump's `restrictions` object for the record itself only."""

    id = "access_grants"

    def map_value(self, ctx: RecordTransformContext):
        """Return dumped view access grants, extended with configured view grants for the stream."""
        restrictions: RecordRestrictions = ctx.raw_dump_entry["restrictions"]
        access_grants = []

        for field in restrictions["record"]["viewers"]:
            access_grants.extend(
                {grant: "view"} for grant in restrictions["record"]["viewers"][field]
            )

        if ctx.access_grants_view:
            access_grants.extend(
                {str(grant): "view"} for grant in ctx.access_grants_view
            )

        return access_grants
