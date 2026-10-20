# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM HSE model."""
from cds_migrator_kit.rdm.records.transform.models._config import IGNORE_SYSTEM_KEYS
from cds_migrator_kit.rdm.records.transform.models.base_publication_record import (
    rdm_base_publication_model,
)
from cds_migrator_kit.transform.overdo import CdsOverdo


class HseModel(CdsOverdo):
    """Translation model for HSE records."""

    __query__ = """595__:CERN-HSE"""

    __ignore_keys__ = IGNORE_SYSTEM_KEYS | {
        "100__j",
        "300__c",  # value only '9 p': 2712787, 2789695
        "506__a",  # Access note restrictions, only 2 records have "Free" but they are restricted
        "595__a",  # Unneeded: CERN-HSE or "To be check by ..."
        "690C_a",  # Duplicate of 980__a resource type
        # "6531_9",  # keyword scheme
        "700__j",
        "720__a",  # Author's duplicate
        "7870_r",  # detailed description of record relation (2862345)
        # Drop citation field https://cds.cern.ch/record/202927/
        "913__t",  # Citation field
        "913__y",  # Citation field
        "913__v",  # Citation field
        "913__c",  # Citation field
    }

    _default_fields = {
        "custom_fields": {},
        "record_restriction": "public",  # All HSE papers are public, but some of them are missing 963:PUBLIC
    }


hse_model = HseModel(
    bases=(rdm_base_publication_model,),
    entry_point_group="cds_migrator_kit.migrator.rules.hse",
)
