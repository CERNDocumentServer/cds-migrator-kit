# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM ISOLDE model."""

from cds_migrator_kit.rdm.records.transform.models._config import IGNORE_SYSTEM_KEYS
from cds_migrator_kit.rdm.records.transform.models.base_publication_record import (
    rdm_base_publication_model,
)
from cds_migrator_kit.rdm.records.transform.models.research import ResearchModel
from cds_migrator_kit.transform.overdo import CdsOverdo


class ISOLDEModel(CdsOverdo):
    """Translation model for ISOLDE."""

    __query__ = '693__.a:"CERN ISOLDE" AND (980__:ARTICLE OR 980__:PREPRINT OR 980__:conferencepaper OR 980__:NOTE OR 980__:REPORT) -980__:DELETED -980__:DUMMY'

    __ignore_keys__ = IGNORE_SYSTEM_KEYS | ResearchModel.__ignore_keys__

    _default_fields = {
        "custom_fields": {},
    }


isolde_model = ISOLDEModel(
    bases=(rdm_base_publication_model,),
    entry_point_group="cds_migrator_kit.migrator.rules.isolde",
)
