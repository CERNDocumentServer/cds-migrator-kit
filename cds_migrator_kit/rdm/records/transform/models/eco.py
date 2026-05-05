# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM ECO model."""

from cds_migrator_kit.rdm.records.transform.models.base_record import (
    rdm_base_record_model,
)
from cds_migrator_kit.transform.overdo import CdsOverdo


class ECOModel(CdsOverdo):
    """Translation model for ECO records."""

    __query__ = """
        (
            980__:POSTER
            OR (980__:BROCHURE AND 690C_:CERNOFFICIALPRESSBROCHURE)
            OR (
                (980__:BROCHURE AND 690C_:CERNEXPERIMENTBROCHURE)
                OR (
                    980__:CMSOUTREACH
                    AND (
                        6531_.a:Brochure
                        OR 6531_.a:brochure
                        OR 6531_a:Brochure
                        OR 6531_a:brochure
                    )
                )
            )
            OR (980__:NOTE AND 710__.5:IR)
        )
        AND -595__a:Press
        AND -980__:LHCb_Misc
        AND -690C_a:PRIVATLAS
    """

    __ignore_keys__ = {
        "0247_9",  # source of pid, only value: OSTI, 2948638, 2853279
        "0248_a",
        "0248_p",
        "0248_q",
        "035__d",  # oai harvest tag
        "035__h",  # oai harvest tag
        "035__m",  # oai harvest tag
        "100__m",  # email of contributor
        "245__9",  # source of title, only value: submitter
        "270__m",  # email of contact person - TODO: is it okay to ignore? example: 2908973
        "270__p",  # contact person name - TODO: is it okay to ignore?
        "300__a",  # number of pages
        "340__a",  # Physical medium
        "520__9",  # abstract provenance
        "541__e",  # Original source poster https://cds.cern.ch/record/2695195/export/hm
        "594__a",  # PUB: 2749806, 2749822
        "6531_9",  # scheme of keywords
        "700__m",  # email of contributor
        "773__p",  # display name of the related link TODO: is it okay to ignore?
        "773__y",  # year, TODO: is it okay to ignore? https://cds.cern.ch/record/1452204/export/xm
        "773__v",  # TODO: is it okay to ignore? https://cds.cern.ch/record/1452204/export/xm
        "852__c",
        "852__h",
        "8560_f",  # contact email
        "8564_8",  # file id
        "8564_s",  # bibdoc id
        "8564_x",  # icon thumbnails sizes
        "8564_y",  # file description - handled by files dump
        "8564_z",  # DM metadata
        "937__c",  # last modified by
        "937__s",  # last modification date
        "960__a",  # base number
        "961__a",  # CDS modification tag
        "961__b",  # CDS modification tag
        "961__c",  # CDS modification tag
        "961__h",  # CDS modification tag
        "961__l",  # CDS modification tag
        "961__x",  # CDS modification tag
        "981__a",  # duplicate record id
    }

    _default_fields = {
        "custom_fields": {},
        "languages": [],
        "related_identifiers": [],
        "creators": [{"person_or_org": {"type": "organizational", "name": "CERN"}}],
    }


eco_model = ECOModel(
    bases=(rdm_base_record_model,),
    entry_point_group="cds_migrator_kit.migrator.rules.eco",
)
