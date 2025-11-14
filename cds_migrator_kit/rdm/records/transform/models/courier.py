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
from cds_migrator_kit.rdm.records.transform.models.bulletin_issue import (
    bull_issue_model,
)
from cds_migrator_kit.transform.overdo import CdsOverdo


class CourierIssueModel(CdsOverdo):
    """Translation model for MoUs."""

    __query__ = "980__:CERN_COURIER_ISSUE OR 980__:CERN_COURIER_ARTICLE"

    __ignore_keys__ = {
        "0248_a",
        "0248_p",
        "110__a",
        "100__m",
        "246_1a",
        "300__a",
        "540__3",
        "542__3",
        "595__s",
        "595__a",
        "6531_9",  # 1733485 scheme CERN
        "690C_a",
        "690__a",  # only CERN value
        "700__m",
        "773__y",
        "773__n",
        "773__p",
        "773__c",
        "773__v",
        "0248_q",
        "8564_8",
        "8564_s",
        "8564_x",
        "8564_y",  # file description
        "960__a",  # 2265255
        "981__a",
    }

    _default_fields = {
        "resource_type": {"id": "publication-other"},
        "custom_fields": {"journal:journal": {"title": "CERN Courier"}},
        "creators": [{"person_or_org": {"type": "organizational", "name": "CERN"}}],
    }


courier_issue_model = CourierIssueModel(
    bases=(rdm_base_record_model,),
    entry_point_group="cds_migrator_kit.migrator.rules.courier",
)
