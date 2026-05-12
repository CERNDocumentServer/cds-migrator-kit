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


class ResearchModel(CdsOverdo):
    """Translation model for research."""

    __query__ = '980__:L3_Papers OR 980__:INTNOTEALEPHPRIV OR 980__:OPAL_Papers OR 980__:OPAL_Misc OR 980__:DELPHI_Misc OR 980__:DELPHI_Papers OR 980__:L3_Misc OR 693__.e:L3 OR 693__.e:DELPHI OR 693__.e:OPAL OR 693__.e:ALEPH OR 690C_.a:PUBLDELPHINOTE OR 690C_.a:PRIVDELPHINOTE OR 710__.g:"ALEPH Collaboration" OR 710__.g:"Aleph Collaboration" OR 980__.a:ALEPH_Papers OR 980__.a:ALEPHDRAFT OR 037__:CERN-ALEPH-PUB-* OR 037__:CERN-ALEPH-ARCH-DATA-* OR 980__:LCD-Notes OR 980__:LCD-NOTES OR 693__.e:"DAMPE RE29" OR 037__:DIRAC-NOTE* OR 037__:DIRAC-Note* OR 037__:DIRAC-CONF* OR 037__:DIRAC-DOC* OR 037__:DIRAC-PUB* OR 693__:UA2 OR 693__:UA4 OR 693:__UA5 OR 693__:UA8 OR 980__:ANTARESCERNTALK OR (980__.a:"POSTER" AND 693__.e:ANTARES)  OR 980__:INTNOTEHARPCDPPUBL OR 980__:PRIVIMXGAM OR 980__:PRIVANTARES -980__:THESIS -037__:CERN-STUDENTS-Note-* -980__:DELETED -980__.c:MIGRATED -980__.a:DUMMY -690C_.a:SCICOM'

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
        "270__m",  # document contact email
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
        "773__t",  # INSPIRE publication note
        "773__0",  # from SIS: can be ignored
        "8564_8",  # file id
        "8564_s",  # bibdoc id
        "8564_x",  # icon thumbnails sizes
        # "8564_y",  # file description - done by files dump
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
    }


research_model = ResearchModel(
    bases=(rdm_base_publication_model,),
    entry_point_group="cds_migrator_kit.migrator.rdm.rules.publication",
)
