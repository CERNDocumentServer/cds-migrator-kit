# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM Radio Protection model."""
from cds_migrator_kit.rdm.records.transform.models.hse import HseModel, hse_model


class RadioProtectionModel(HseModel):
    """Translation model for Radio Protection records."""

    __query__ = """980__:RP_RESTRICTED"""

    _default_fields = {
        "resource_type": {"id": "publication-preprint"}, # Default (only 2 articles which the rules will update)
        "custom_fields": {},
    }


radio_protection_model = RadioProtectionModel(
    bases=(hse_model,),  # inherits the HSE rules
    entry_point_group="cds_migrator_kit.migrator.rules.radio_protection",
)
