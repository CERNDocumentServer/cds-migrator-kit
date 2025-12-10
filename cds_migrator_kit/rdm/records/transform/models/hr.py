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


class HrModel(CdsOverdo):
    """Translation model for MoUs."""

    __query__ = """980__:INTNOTEHRPUBL OR
                   980__:STAFFRULES OR
                   980__:STAFFRULESVD OR
                   980__:ADMINCIRCULAR OR
                   980__:OPERCIRCULAR OR
                   980__:ANNUALSTATS OR
                   980__:CHISBULLETIN OR
                   980__:CCP OR
                   980__:CERN-ADMIN-E-GUIDE OR
                   980__:HR-SMC
                   """

    __ignore_keys__ = {
        "0247_9",
        "0248_a",
        "0248_p",
        "0248_q",
        "100__m",
        "300__a",  # number of pages
        "591__b",  #
        "6531_9",  # keyword scheme
        "700__m",
        "7870_r",  # detailed description of record relation (2862345)
        "8564_8",
        "8564_s",
        "8564_x",
        "8564_y",  # file description - done by files dump
        "8564_z",
        "937__c",
        "937__s",
        "960__a",  # base number
        "961__c",  # CDS modification tag # TODO
        "961__h",  # CDS modification tag # TODO
        "961__l",  # CDS modification tag # TODO
        "961__x",  # CDS modification tag # TODO
        "980__a",
    }

    _default_fields = {
        "resource_type": {"id": "publication-other"},
        "custom_fields": {},
        "creators": [{"person_or_org": {"type": "organizational", "name": "HR"}}],
    }


hr_model = HrModel(
    bases=(rdm_base_record_model,),
    entry_point_group="cds_migrator_kit.migrator.rules.hr",
)
