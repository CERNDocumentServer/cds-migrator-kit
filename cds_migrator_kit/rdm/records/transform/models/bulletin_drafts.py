# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM Bulletin Drafts model."""

from cds_migrator_kit.rdm.records.transform.models.bulletin_issue import (
    bull_issue_model,
)
from cds_migrator_kit.rdm.records.transform.models.staff_association import (
    staff_association_model,
)
from cds_migrator_kit.transform.overdo import CdsOverdo


class BulletinDraftsModel(CdsOverdo):
    """Translation model for Bulletin Drafts."""

    __query__ = """(
        980__:"BULLETINSTAFFDRAFT" OR
        980__:"BULLETINNEWSDRAFT" OR
        980__:"BULLETINOFFICIALDRAFT" OR
        980__:"BULLETINTRAININGDRAFT" OR
        980__:"BULLETINANNOUNCEDRAFT" OR
        980__:"BULLETINEVENTSDRAFT"
    )
    """

    # Copy-pasted from bulletin issue
    __ignore_keys__ = {
        "0248_a",
        "0248_p",
        "0248_q",
        "100__m",  # email of contributor
        "110__a",  # corporate author, always CERN, safe to ignore
        "300__a",  # number of pages
        "336__a",  # DM metadata
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
        "594__a",  # values: "no", "pub"
        "650172",  # scheme of subjects
        "6531_9",  # scheme of keywords
        "691__a",  # draft/online values, redundant
        "700__m",  # email of contributor
        "773__p",  # title of the "CERN Bulletin" series
        "773__t",  # CERN Bulletin value, redundant
        "773__y",  # year, duplicate of 260
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
        "961__a",  # Curation Auditing tag
        "961__b",  # Curation Auditing tag
        "961__c",  # Curation Auditing tag
        "961__h",  # Curation Auditing tag
        "961__l",  # Curation Auditing tag
        "961__x",  # Curation Auditing tag
        "981__a",  # duplicate record id
        # "246_1a",
        # "690C_a",
    }

    _default_fields = {
        "custom_fields": {"journal:journal": {"title": "CERN Bulletin"}},
        "creators": [{"person_or_org": {"type": "organizational", "name": "CERN"}}],
    }


bulletin_drafts_model = BulletinDraftsModel(
    bases=(staff_association_model, bull_issue_model,),
    entry_point_group="cds_migrator_kit.migrator.rules.bulletin_drafts",
)
