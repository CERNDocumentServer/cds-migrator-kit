# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM CMS note model."""
from cds_migrator_kit.rdm.records.transform.models.base_record import (
    rdm_base_record_model,
)
from cds_migrator_kit.transform.overdo import CdsOverdo


class MOUSModel(CdsOverdo):
    """Translation model for MoUs."""

    __query__ = "980__:MOUS"

    __ignore_keys__ = {}

    _default_fields = {
        "resource_type": {"id": "publication-other"},
        "custom_fields": {},
    }


mous_model = MOUSModel(
    bases=(rdm_base_record_model,),
    entry_point_group="cds_migrator_kit.migrator.rules.mous",
)
