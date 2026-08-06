# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM SHIP research model."""

from cds_migrator_kit.rdm.records.transform.models._config import IGNORE_SYSTEM_KEYS
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

    __ignore_keys__ = IGNORE_SYSTEM_KEYS | {
        "100__v",  # complete affiliation
        "0247_9",  # provenance of the DOI
        "035__h",  # oai identifiers
        "035__d",  # oai identifiers
        "035__t",  # oai identifiers
        "035__u",  # oai identifiers
        "035__m",  # oai identifiers
        "110__u",  # Confirmed with SIS
        "270__m",  # document contact email
        "540__3",  # material of license
        "542__3",  # copyright material
        "700__v",  # complete affiliation
        "773__o",  # Duplicate meeting title
        "8564_z",  # automatic process with EP value:Stamped by WebSubmit
        "903__s",  # public
    }


ship_research_model = SHIPResearchModel(
    bases=(research_model,),
    entry_point_group="cds_migrator_kit.migrator.rdm.rules.ship",
)
