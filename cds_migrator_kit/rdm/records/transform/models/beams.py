# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM Yellow report model."""
from cds_migrator_kit.rdm.records.transform.models.base_record import (
    rdm_base_publication_model,
    rdm_base_record_model,
)
from cds_migrator_kit.rdm.records.transform.models.thesis import thesis_model
from cds_migrator_kit.transform.overdo import CdsOverdo


class BeamsModel(CdsOverdo):
    """Translation model for MoUs."""

    # __query__ = '980__:INTNOTEBEPUBL OR 980__:BELIB'
    #  '710__.5:BE '
    __query__ = "980__:BELIB"

    __ignore_keys__ = {
        "0248_a",
        "0248_p",
        "0248_q",
        "037__c",  # arxiv subject
        "852__c",  # todo holdings
        "852__h",
        "300__a",
        "340__a",  # ignore, spreadsheet
        "8564_8",  # Files system field
        "8564_s",  # Files system field
        "8564_x",  # Files system field
        "8564_y",  # Files / URLS label
        "710__b",  # BI (old beams dep name?)
        "916__y",  # year
        "960__a",  # collection id? usually value 14, to confirm if we ignore
        "961__c",  # CDS modification tag
        "961__h",  # CDS modification tag
        "961__l",  # CDS modification tag
        "961__x",  # CDS modification tag
        "964__a",  # TODO
        "594__a",  # document type TODO
        "540__3",  # material of copyright
        "937__c",  # last modified
        "937__s",  # last modified
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
        # "110__a",
        # "246_1a",
        # "690C_a",
        # "520__b",
        # "773__y",
        # "773__n",
        # "773__p",
        # "773__c",
        # "0248_q",
        # "8564_8",
        # "8564_s",
        # "8564_x",
        # "980__a",
    }

    _default_fields = {
        # "resource_type": {"id": "publication-report"},
        "custom_fields": {},
        # "creators": [{"person_or_org":  {"type": "organizational", "name": "CERN"}}]
    }


beams_model = BeamsModel(
    bases=(rdm_base_publication_model,),
    entry_point_group="cds_migrator_kit.migrator.rules.beams",
)
