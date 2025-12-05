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
                    OR 980__:CNLARTICLE
                    -980__.b:INDICO_IT-DEP
                    -980__.a:EVENTSFROMINDICO
                    -980__.a:CONTRIBUTIONSFROMINDICO
                    -980__:CERNITArchive
                    -980__.a:EVENTSFROMINDICO
                    -980__.a:CONTRIBUTIONSFROMINDICO
                    -980__:BOOK
                    -690C_:YELLOWREPORT
                    -690C_:"YELLOW REPORT"
                    -980__:THESIS
                    -980__:INTNOTECMSPUBL
                    """

    __ignore_keys__ = {
        "021__a",  # Physical Location https://cds.cern.ch/record/181665/export/hm?ln=en
        "022__b",  # material type
        "0247_9",  # source of pid
        "0248_a",
        "0248_p",
        "0248_q",
        "030__a",  # https://cds.cern.ch/record/409840/export/hm
        "037__c",  # arXiv subject https://cds.cern.ch/record/1562050/export/hm
        "044__a",  # country code
        "080__a",  # UDC
        "100__m",  # Author's email
        "300__a",
        "300__b",  # Physical medium description
        "270__m",  # contact e-mail
        "222__a",  # Duplicate title
        "246_39",
        "246_3i",
        "260__b",  # CERN
        "310__a",  # periodicity
        "340__a",  # Physical medium
        "362__a",  # date period
        "500__9",  # arXiv
        "520__9",  # arxiv
        "540__3",  # Material of copyright
        "541__e",  # Original source poster https://cds.cern.ch/record/1034295/export/hm
        "542__3",  # Copyright materials
        "594__a",  # document type
        "595__i",  # INSPEC number
        "6531_9",  # Keyword provenance
        "700__m",  # Author's email
        "710__b",  # Group name, TBD https://cds.cern.ch/record/2258345/export/hm?ln=en
        "710__9",  # Group name, TBD https://cds.cern.ch/record/2258345/export/hm?ln=en
        "720__a",  # Author's duplicate
        "773__a",  # Duplicate DOI
        "773__o",  # Duplicate meeting title
        "773__u",  # Duplicate meeting url
        "785__t",  # Related works platform
        "785__x",  # Related works type
        "7870_r",  # detailed description of record relation
        "852__a",  # Physical Location https://cds.cern.ch/record/307939/export/hm?ln=en
        "852__c",  # Physical Location https://cds.cern.ch/record/134892/export/hm?ln=en
        "852__h",  # Physical Location https://cds.cern.ch/record/134892/export/hm?ln=en
        "866__a",  # Holdings on ILS
        "866__b",
        "866__g",
        "866__x",
        "866__z",
        "8564_8",  # Files system field
        "8564_s",  # Files system field
        "8564_x",  # Files system field - Icon
        "8564_z",  # File comment, handled on files level, not MARC
        "8564_q",  # Files system field - Link
        "856418",  # Files system field
        "85641q",  # Files system field - Link
        "8564_y",  # Files / URLS label
        "85641g",
        "85641m",
        "85641n",
        "85641y",  # Year file
        "85641x",
        "913__t",  # Citation field
        "913__y",  # Citation field
        "913__v",  # Citation field
        "913__c",  # Citation field
        "916__y",  # year
        "916__a",  # year
        "981__b",  # duplicated pid
        "923__r",  # Author's email
        "937__c",  # last modified
        "937__s",  # last modified
        "938__a",
        "938__p",
        "960__a",  # collection id? usually value 14, to confirm if we ignore
        "961__c",  # CDS modification tag
        "961__h",  # CDS modification tag
        "961__l",  # CDS modification tag
        "961__x",  # CDS modification tag
        "964__a",  # Item usually 0001?
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
        "custom_fields": {"cern:departments": ["IT"]},
    }


it_model = ITModel(
    bases=(rdm_base_publication_model,),
    entry_point_group="cds_migrator_kit.migrator.rules.it",
)
