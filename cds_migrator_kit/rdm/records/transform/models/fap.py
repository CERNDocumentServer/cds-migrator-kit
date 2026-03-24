# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM FAP (Finance and Administrative Processes) model."""
from cds_migrator_kit.rdm.records.transform.models.base_record import (
    rdm_base_record_model,
)
from cds_migrator_kit.transform.overdo import CdsOverdo


class FAPModel(CdsOverdo):
    """Translation model for FAP records."""

    __query__ = "980__:INTNOTEFAPPUBL"

    __ignore_keys__ = {
        "100__m",
        "300__a",  # number of pages
        "8564_8",
        "8564_s",
        "8564_x",
        "8564_y",  # file description - done by files dump
        "937__c",  # last modified by
        "937__s",  # last modification date
        "960__a",  # base number usually 12
    }

    _default_fields = {
        # TODO: is this resource type correct?
        "resource_type": {"id": "publication-report"},
        "custom_fields": {},
    }


fap_model = FAPModel(
    bases=(rdm_base_record_model,),
    entry_point_group="cds_migrator_kit.migrator.rules.fap",
)
