# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM Yellow report model."""
from cds_migrator_kit.rdm.records.transform.models.base_publication_record import (
    rdm_base_publication_model,
)
from cds_migrator_kit.transform.overdo import CdsOverdo


class ITModel(CdsOverdo):
    """Translation model for IT."""

    __query__ = """(980__:PERI AND 65017.a:"Computing and Computers")
                    OR 710__.5:IT OR 710__.5:DD OR 710__.5:CN OR 710__.5:AS OR 710__.5:STS
                    OR 980__:INTNOTEITPUBL OR 980__:INTNOTEASPUBL
                    OR (980__:INTNOTEMISPUBL AND 690C_:INTNOTE)
                    OR 980__:PUBLARDA
                    OR 690C_:CERNITBROCHURE OR 980__:ITCERNTALK
                    OR 980__:ITUDSPUBSOURCEARCHIVE
                    - 980__.b:INDICO_IT-DEP
                    -980__:CERNITArchive -980__:INTNOTECMSPUBL
                    -980__.a:EVENTSFROMINDICO 
                    -980__.a:CONTRIBUTIONSFROMINDICO
                    -980__:BOOK -690C_:YELLOWREPORT
                    -690C_:"YELLOW REPORT" -980__:THESIS"""

    __ignore_keys__ = {
        "0248_a",
        "0248_p",
        "0248_q",
        "037__c",  # arXiv subject https://cds.cern.ch/record/1562050/export/hm
        "100__m",  # Author's email
        "300__a",
        "300__b",  # Physical medium description
        "260__b",  # CERN
        "340__a",  # Physical medium
        "500__9",  # arXiv
        "540__3",  # Material of copyright
        "541__e",  # Original source poster https://cds.cern.ch/record/1034295/export/hm
        "542__3",  # Copyright materials
        "595__i",  # INSPEC number
        "6531_9",  # Keyword provenance
        "700__m",  # Author's email
        "773__u",  # Duplicate meeting url
        "710__b",  # Group name, TBD https://cds.cern.ch/record/2258345/export/hm?ln=en
        "720__a",  # Author's duplicate
        "773__a",  # Duplicate DOI
        "852__a",  # Physical Location https://cds.cern.ch/record/307939/export/hm?ln=en
        "852__c",  # Physical Location https://cds.cern.ch/record/134892/export/hm?ln=en
        "852__h",  # Physical Location https://cds.cern.ch/record/134892/export/hm?ln=en
        "8564_8",  # Files system field
        "8564_s",  # Files system field
        "8564_x",  # Files system field - Icon
        "8564_q",  # Files system field - Link
        "8564_y",  # Files / URLS label
        "916__y",  # year
        "923__r",  # Author's email
        "937__c",  # last modified
        "937__s",  # last modified
        "960__a",  # collection id? usually value 14, to confirm if we ignore
        "961__c",  # CDS modification tag
        "961__h",  # CDS modification tag
        "961__l",  # CDS modification tag
        "961__x",  # CDS modification tag
        "981__a",  # duplicated record marker
        "999C50",  # https://cds.cern.ch/record/2284609/export/hm?ln=en CMS contributions
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
        # "resource_type": {"id": "publication-report"},
        "custom_fields": {},
        "creators": [{"person_or_org": {"type": "organizational", "name": "CERN"}}],
    }


it_model = ITModel(
    bases=(rdm_base_publication_model,),
    entry_point_group="cds_migrator_kit.migrator.rules.it",
)
