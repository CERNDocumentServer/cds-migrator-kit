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
        "(980__:SCICOMMPUBLSPSC SHiP) OR "
        "980__.a:SHiPPUBDRAFTFINAL OR 980__.a:SHiP_Papers OR "
        "(980__.a:INTNOTE AND 693__.e:SHiP) "
        "-980__:DELETED -980__.c:MIGRATED -980__.a:DUMMY"
    )

    __ignore_keys__ = {
        "100__v",  # complete affiliation
        "100__m",  # email of contributor
        "0247_9",  # provenance of the DOI
        "0248_a",
        "0248_p",
        "035__h",  # oai identifiers in 2281295, 2802785
        "035__d",  # oai identifiers in 2281295, 2802785
        "035__t",  # oai identifiers in 2281295, 2802785
        "035__u",  # oai identifiers in 2281295, 2802785
        "035__m",  # oai identifiers in 2281295, 2802785
        "037__c",  # arxiv subject
        "110__u",  # TODO: remove
        "245__9",  # title source
        "270__m",  # document contact email
        "300__a",  # number of pages
        "500__9",  # provenance of the note
        "520__9",  # provenance of the description
        "542__3",  # copyright material
        "8564_8",  # file id
        "8564_s",  # bibdoc id
        "8564_x",  # icon thumbnails sizes
        "8564_y",  # file description - done by files dump, sometimes these are used for open access calculation
        "700__m",  # email of contributor
        "700__v",  # complete affiliation
        "773__o",  # Duplicate meeting title
        "78002r",  # Report number of the related record
        "78502r",  # Report number of the related record
        "8564_z",  # automatic process with EP value:Stamped by WebSubmit
        "903__s",  # public
        "905__q",  # TODO: removespokesperson ...
        "905__k",  # TODO: remove spokesperson ...
        "905__l",  # TODO: remove spokesperson ...
        "937__c",  # last modified by
        "937__s",  # last modification date
        "960__a",  # base number
        "981__a",  # duplicate record id
    }


ship_research_model = SHIPResearchModel(
    bases=(research_model,),
    entry_point_group="cds_migrator_kit.migrator.rdm.rules.ship",
)
