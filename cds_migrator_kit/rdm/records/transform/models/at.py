# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

from cds_migrator_kit.rdm.records.transform.models._config import IGNORE_SYSTEM_KEYS
from cds_migrator_kit.rdm.records.transform.models.base_publication_record import (
    rdm_base_publication_model,
)
from cds_migrator_kit.transform.overdo import CdsOverdo


class ATModel(CdsOverdo):
    """Translation model for AT records."""

    __query__ = """
        (980__:ARTICLE OR 980__:PREPRINT)
        AND
        (710__.5:NPA OR 710__.5:AT)
        -710__.5:SI -710__.5:SC -710__.5:SL -710__.5:PS -710__.5:MPS -710__.5:ISR -710__.5:MSC -710__.5:AC -710__.5:SPS
        -710__.5:LEP -710__.5:AB -710__.5:AR -710__.5:TS -710__.5:ST -710__.5:MT -710__.5:EST -710__.5:SB -710__.5:LHC
        -980__:DELETED -980__.c:MIGRATED -980__c:MERGED
    """

    __ignore_keys__ = IGNORE_SYSTEM_KEYS | {
        "030__a",  # coden designation
        "260__b",  # Always CERN
        "300__b",  # Resolution of the video
        "340__a",  # Physical medium
        "518__h",  # Start time of meeting/conference event
        "518__g",  # Meeting/conference identification
        "520__9",  # Source of the additional description (e.g. arxiv)
        "542__3",  # Part of the license
        "595__i",  # INSPEC number
        "773__a",  # Duplicate DOI
        "901__u",  # Affiliation at Conversion?
        "913__y",  # citation
        "913__v",  # citation
        "913__t",  # citation
        "913__a",  # citation
        "913__c",  # citation
        "964__a",  # number of physical copies
        "970__b",  # spreadsheet
    }

    _default_fields = {
        "custom_fields": {},
    }


at_model = ATModel(
    bases=(rdm_base_publication_model,),
    entry_point_group="cds_migrator_kit.migrator.rules.at",
)
