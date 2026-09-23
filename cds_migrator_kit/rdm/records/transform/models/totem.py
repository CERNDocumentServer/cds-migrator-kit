# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM TOTEM model."""

from cds_migrator_kit.rdm.records.transform.models._config import IGNORE_SYSTEM_KEYS
from cds_migrator_kit.rdm.records.transform.models.base_publication_record import (
    rdm_base_publication_model,
)
from cds_migrator_kit.transform.overdo import CdsOverdo


class TOTEMModel(CdsOverdo):
    """Translation model for the TOTEM experiment."""

    __query__ = "693__.e:TOTEM -980__:THESIS -980__:DELETED -980__:HIDDEN -980__:DUMMY"

    __ignore_keys__ = IGNORE_SYSTEM_KEYS

    _default_fields = {
        "custom_fields": {},
    }


totem_model = TOTEMModel(
    bases=(rdm_base_publication_model,),
    entry_point_group="cds_migrator_kit.migrator.rules.totem",
)
