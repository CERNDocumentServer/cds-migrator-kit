# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM HIE-ISOLDE model."""

from cds_migrator_kit.rdm.records.transform.models.base_publication_record import (
    rdm_base_publication_model,
)
from cds_migrator_kit.rdm.records.transform.models.isolde import ISOLDEModel


class HIEISOLDEModel(ISOLDEModel):
    """Translation model for HIE-ISOLDE project notes and reports."""

    __query__ = "980__:HIE-ISOLDE-Project-Notes -980__:DELETED -980__:DUMMY"


hie_isolde_model = HIEISOLDEModel(
    bases=(rdm_base_publication_model,),
    entry_point_group="cds_migrator_kit.migrator.rules.hie_isolde",
)
