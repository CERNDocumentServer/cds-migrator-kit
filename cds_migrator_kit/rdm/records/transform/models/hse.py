# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM HSE model."""
from cds_migrator_kit.rdm.records.transform.models.base_publication_record import (
    rdm_base_publication_model,
)
from cds_migrator_kit.transform.overdo import CdsOverdo


class HseModel(CdsOverdo):
    """Translation model for HSE records."""

    __query__ = """595__:CERN-HSE"""

    __ignore_keys__ = {
        "0247_9",
        "0248_a",
        "0248_p",
        "0248_q",
        "100__m",
        "035__d",  # oai harvest tag
        "035__h",  # oai harvest tag
        "035__m",  # oai harvest tag
        "245__9",  # only value is submitter
        "300__a",  # number of pages
        "300__c",  # value only '9 p': 2712787, 2789695
        "506__a",  # Access note restrictions, only 2 records have "Free" but they are restricted
        "595__a",  # Unneeded: CERN-HSE or "To be check by ..."
        "690C_a",  # Duplicate of 980__a resource type
        "6531_9",  # keyword scheme
        "700__m",
        "7870_r",  # detailed description of record relation (2862345)
        "8564_8",
        "8564_s",
        "8564_x",
        "8564_y",  # file description - done by files dump
        "8564_z",
        "720__a",  # Author's duplicate
        "960__a",  # base number
        "961__c",  # CDS modification tag # TODO
        "961__h",  # CDS modification tag # TODO
        "961__l",  # CDS modification tag # TODO
        "961__x",  # CDS modification tag # TODO
        "916__y",  # year of publication, redundant
        "981__a",  # duplicate record id
        "852__c",  # Physical Location https://cds.cern.ch/record/184322/export # TODO
        "852__h",  # Physical Location https://cds.cern.ch/record/184322/export # TODO
        "100__j",
        "700__j",
        # Drop citation field https://cds.cern.ch/record/202927/
        "913__t",  # Citation field
        "913__y",  # Citation field
        "913__v",  # Citation field
        "913__c",  # Citation field
    }

    _default_fields = {
        "custom_fields": {},
        "record_restriction": "public", # All HSE papers are public, but some of them are missing 963:PUBLIC
    }


hse_model = HseModel(
    bases=(rdm_base_publication_model,),
    entry_point_group="cds_migrator_kit.migrator.rules.hse",
)
