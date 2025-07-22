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
    """Translation model for MoUs."""

    __query__ = "980__:CERN_BULLETIN_ISSUE OR 980__:CERN_BULLETIN_ARTICLE"

    __ignore_keys__ = {
        "0248_a",
        "0248_p",
        "110__a",
        "246_1a",
        "690C_a",
        "520__b",
        "773__y",
        "773__n",
        "773__p",
        "773__c",
        "0248_q",
        "8564_8",
        "8564_s",
        "8564_x",
        "980__a",
    }

    _default_fields = {
        "resource_type": {"id": "publication-other"},
        "custom_fields": {},
        "creators": [{"person_or_org": {"type": "organizational", "name": "CERN"}}],
    }


bull_issue_model = BulletinIssueModel(
    bases=(rdm_base_record_model,),
    entry_point_group="cds_migrator_kit.migrator.rules.bulletin_issue",
)
