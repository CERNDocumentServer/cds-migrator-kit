# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM SHIP research model."""

from cds_migrator_kit.rdm.records.transform.models.research import (
    ResearchModel,
    research_model,
)


class SHIPResearchModel(ResearchModel):
    """Translation model for SHIP records."""

    __query__ = (
        "((980__.a:NOTE OR 980__.a:Note OR 980__.a:ConferencePaper) AND 690C_.a:SHiP) OR "
        "980__.a:SHiPPUBDRAFTFINAL OR 980__.a:SHiP_Papers OR "
        "(980__.a:INTNOTE AND 693__.e:SHiP) "
        "-980__:DELETED -980__.a:DUMMY"
    )

    # the keys of the research model (the source of truth, see research.py) plus the
    # fields ignored only here
    __ignore_keys__ = ResearchModel.__ignore_keys__ | {
        "100__v",  # complete affiliation
        "110__u",  # Confirmed with SIS
        "270__m",  # document contact email
        "903__s",  # public
    }


ship_research_model = SHIPResearchModel(
    bases=(research_model,),
    entry_point_group="cds_migrator_kit.migrator.rdm.rules.ship",
)
