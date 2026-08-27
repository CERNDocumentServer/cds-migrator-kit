# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM North Area models (NA61-64)."""

from cds_migrator_kit.rdm.records.transform.models._config import IGNORE_SYSTEM_KEYS
from cds_migrator_kit.rdm.records.transform.models.base_publication_record import (
    rdm_base_publication_model,
)
from cds_migrator_kit.transform.overdo import CdsOverdo


class NorthAreaModel(CdsOverdo):
    """Translation model for North Area experiments."""

    __query__ = """693__.e:"NA61" OR 693__.e:"SHINE NA61" OR 693__.e:"NA62" OR 693__.e:"NA63" OR 693__.e:"NA64"
    -980__:THESIS -980__:DELETED -980__:HIDDEN -980__:MIGRATED -980__:DUMMY"""

    __ignore_keys__ = IGNORE_SYSTEM_KEYS | {
        "270__m",  # Email of contact person
        "500__9",  # Provenance of the note
        "903__s",  # 'public'
        "905__m",  # Submitter email address
        "995__a",  # "Inspire"
    }

    _default_fields = {
        "custom_fields": {},
    }


north_area_model = NorthAreaModel(
    bases=(rdm_base_publication_model,),
    entry_point_group="cds_migrator_kit.migrator.rules.north_area",
)
