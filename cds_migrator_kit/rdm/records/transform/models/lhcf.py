# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM LHCf model."""
from cds_migrator_kit.rdm.records.transform.models.base_publication_record import (
    rdm_base_publication_model,
)
from cds_migrator_kit.rdm.records.transform.models.research import ResearchModel


class LHCfModel(ResearchModel):
    """Translation model for LHCf."""

    __query__ = "980__:LHCf_Papers OR 980__:LHCf_Proc OR 980__:LHCf_Reports -980__:THESIS -980:SCICOMMPUBLLHCC -980__:DELETED -980__.a:DUMMY -690C_.a:SCICOM"


lhcf_model = LHCfModel(
    bases=(rdm_base_publication_model,),
    entry_point_group="cds_migrator_kit.migrator.rdm.rules.research",
)
