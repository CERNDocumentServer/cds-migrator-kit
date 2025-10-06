# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM CMS note model."""
from cds_migrator_kit.rdm.records.transform.models.base_record import (
    rdm_base_record_model,
)
from cds_migrator_kit.transform.overdo import CdsOverdo


class BulletinIssueModel(CdsOverdo):
    """Translation model for Bulletin Issue."""

    __query__ = """980__:CERN_BULLETIN_ISSUE OR
                   980__:CERN_BULLETIN_ARTICLE OR
                   980__:BULLETINGENERAL OR
                   980__:BULLETINEVENTS OR
                   980__:BULLETINANNOUNCE OR
                   980__:BULLETINBREAKING OR
                   980__:BULLETINNEWS OR
                   980__:BULLETINOFFICIAL OR
                   980__:BULLETINPENSION OR
                   980__:BULLETINTRAINING OR
                   980__:BULLETINSOCIAL"""

    __ignore_keys__ = {
        "0248_a",
        "0248_p",
        "0248_q",
        "100__m",  # email of contributor
        "110__a",  # corporate author, always CERN, safe to ignore
        "300__a",  # number of pages
        "336__a",  # DM metadata
        "506__m",  # 2120833, ignored with confirmation from IR-ECO-CO
        "5831_2",  # DM tags 1054836
        "5831_5",  # DM tags
        "5831_a",  # DM tags
        "5831_c",  # DM tags
        "5831_f",  # DM tags
        "5831_i",  # DM tags
        "5831_k",  # DM tags
        "5831_u",  # DM tags
        "5831_3",  # DM tags
        "5831_6",  # DM tags
        "5831_n",  # DM tags
        "5831_b",  # DM tags
        "5831_o",  # DM tags
        "583__a",  # DM tags
        "583__c",  # DM tags
        "583__z",  # DM tags
        "590__b",  # staff association? value, redundant with language
        "594__a",  # specifies if the related articles menu has a separator or not (display feature)
        "650172",  # scheme of subjects
        "6531_9",  # scheme of keywords
        "691__a",  # draft/online values, redundant
        "700__m",  # email of contributor
        "773__p",  # title of the "CERN Bulletin" series
        "773__t",  # CERN Bulletin value, redundant
        "773__y",  # year, duplicate of 260
        "773__u",  # broken links on record 44920
        "787__i",  # one referenced record (video in 1755835, 1754359)
        "859__a",  # empty value
        "856__q",  # 619830 broken link
        "8560_f",  # contact email
        "8564_8",  # file id
        "8564_s",  # bibdoc id
        "8564_x",  # icon thumbnails sizes
        "8564_y",  # file description - done by files dump
        "8564_2",  # DM metadata
        "8564_q",  # DM metadata
        "8564_w",  # DM metadata
        "8564_z",  # DM metadata
        "8567_2",  # DM tags
        "8567_q",  # DM tags
        "8567_w",  # DM tags
        "8567_d",  # DM tags
        "906__m",  # edit rights, will be granted by the community
        "937__c",  # last modified by
        "937__s",  # last modification date
        "960__a",  # base number
        "961__a",  # CDS modification tag # TODO
        "961__b",  # CDS modification tag # TODO
        "961__c",  # CDS modification tag # TODO
        "961__h",  # CDS modification tag # TODO
        "961__l",  # CDS modification tag # TODO
        "961__x",  # CDS modification tag # TODO
        "981__a",  # duplicate record id
        "980__b",
        # "246_1a",
        # "690C_a",
    }

    _default_fields = {
        "custom_fields": {"journal:journal": {"title": "CERN Bulletin"}},
        "creators": [{"person_or_org": {"type": "organizational", "name": "CERN"}}],
    }


bull_issue_model = BulletinIssueModel(
    bases=(rdm_base_record_model,),
    entry_point_group="cds_migrator_kit.migrator.rules.bulletin_issue",
)
