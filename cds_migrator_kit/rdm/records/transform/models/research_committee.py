# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM CMS note model."""
from cds_migrator_kit.rdm.records.transform.models.base_publication_record import (
    rdm_base_publication_model,
)
from cds_migrator_kit.transform.overdo import CdsOverdo


class ResearchCommitteeModel(CdsOverdo):
    """Translation model for research committees."""

    __query__ = '980__:SCICOMMPUBLDRDC OR 980__:SCICOMMPUBLEEC OR 980__:SCICOMMPUBLEMC OR 980__:SCICOMMPUBLISC OR 980__:SCICOMMPUBLISRC OR 980__:SCICOMMPUBLISTC OR 980__:SCICOMMPUBLLEPC OR 980__:SCICOMMPUBLNPRC OR 980__:SCICOMMPUBLNSC OR 980__:SCICOMMPUBLPHI OR 980__:SCICOMMPUBLPHIII OR 980__:SCICOMMPUBLPSC OR 980__:SCICOMMPUBLPSCC OR 980__:SCICOMMPUBLSCC OR 980__.a:SC_and_PS_Advisory_Committee OR (980__:SCICOMMPUBLSPSC AND 260__.c:"0000"->"1990") OR 980__:SCICOMMPUBLSPSLC OR 980__:SCICOMMPUBLTCC -037__:CERN-STUDENTS-Note-* -980__:THESIS -980__:thesis -980__:Thesis -980__:DELETED -980__.c:MIGRATED -980__.a:DUMMY'

    __ignore_keys__ = {
        "0248_a",
        "0248_p",
        "0248_q",
        "0247_9",  # provenance of the DOI
        "030__a",  # TODO coden designation to drop?
        "035__h",  # oai identifiers in 1215391
        "035__d",  # oai identifiers in 1215391
        "035__t",  # oai identifiers in 1215391
        "035__u",  # oai identifiers in 1215391
        "035__m",  # oai identifiers in 1215391
        "035__z",  # oai identifiers in 1215391
        "500__9",  # provenance of the note
        "520__9",  # provenance of the description
        "520__h",  # provenance of the description
        "852__c",  # holdings will be taken separately
        "852__h",
        "037__c",  # arxiv subject
        "100__m",  # email of contributor
        "245__9",  # title provenance
        "300__a",  # number of pages
        "340__a",  # TODO ignore material?
        "540__3",  # TODO still ignore the material of the license?
        "542__3",  # TODO still ignore the material of the license?
        "595__i",  # TODO ??
        "695__e",  # some inspire tag
        "700__m",  # email of contributor
        "700__q",  # TODO ignore? aliteration of the name, used for searching
        "700__v",  # TODO drop?
        "773__x",  # INSPIRE publication note
        "8564_8",  # file id
        "8564_s",  # bibdoc id
        "8564_x",  # icon thumbnails sizes
        "8564_y",  # file description - done by files dump
        "8564_w",  # system field
        "913__y",  # citation
        "913__v",  # citation
        "913__t",  # citation
        "913__a",  # citation
        "913__c",  # citation
        "916__y",  # year, redundant value
        "937__c",  # last modified by
        "937__s",  # last modification date
        "960__a",  # base number
        "961__c",  #
        "961__h",  #
        "961__l",  #
        "961__x",  #
        "964__a",  # TODO: ignore?
        "981__a",  # duplicate record id
        "995__a",  # INSPIRE as value
        "999C50",
        "999C52",  # https://cds.cern.ch/record/2640188/export/hm?ln=en
        "999C59",  # https://cds.cern.ch/record/2284615/export/hm?ln=en
        "999C5a",  # https://cds.cern.ch/record/2678429/export/hm?ln=en
        "999C5c",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C5h",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C5i",  # https://cds.cern.ch/record/2284892/export/hm?ln=en
        "999C5k",  # https://cds.cern.ch/record/2671914/export/hm?ln=en
        "999C5l",  # https://cds.cern.ch/record/2283115/export/hm?ln=en
        "999C5m",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C5o",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C5p",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C5r",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C5s",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C5t",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C5u",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C5v",  # https://cds.cern.ch/record/2283088/export/hm?ln=en
        "999C5x",  # https://cds.cern.ch/record/2710809/export/hm?ln=en
        "999C5y",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C5z",  # https://cds.cern.ch/record/2710809/export/hm?ln=en
        "999C6a",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C6t",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C6v",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
    }

    _default_fields = {
        "custom_fields": {},
        "resource_type": {"id": "publication-other"},
    }


research_comm_model = ResearchCommitteeModel(
    bases=(rdm_base_publication_model,),
    entry_point_group="cds_migrator_kit.migrator.rdm.rules.publication",
)
