# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM CMS note model."""
from cds_migrator_kit.rdm.records.transform.models.base_publication_record import (
    rdm_base_publication_model,
)
from cds_migrator_kit.transform.overdo import CdsOverdo


class TEModel(CdsOverdo):
    """Translation model for MoUs."""

    __query__ = "980__:INTNOTETEPUBL OR 710__.5:TE -980__:BOOK -710__.5:EN -690C_:YELLOWREPORT -690C_:YELLOW REPORT"

    __ignore_keys__ = {
        "0248_a",
        "0248_p",
        "0248_q",
        "100__m",
        "300__a",
        "270__m",
        "700__m",
        "690C_a",
        "8564_8",
        "8564_s",
        "8564_x",
        "8564_y",  # file description - done by files dump
        "916__y",  # year of publication, redundant
        "937__c",
        "937__s",
        "960__a",  # base number
        "961__c",  # CDS modification tag # TODO
        "961__h",  # CDS modification tag # TODO
        "961__l",  # CDS modification tag # TODO
        "961__x",  # CDS modification tag # TODO
        "981__a",  # duplicate record id
    }

    _default_fields = {
        # "resource_type": {"id": "publication-other"},
        "custom_fields": {},
        "creators": [{"person_or_org": {"type": "organizational", "name": "CERN"}}],
    }


te_model = TEModel(
    bases=(rdm_base_publication_model,),
    entry_point_group="cds_migrator_kit.migrator.rules.te",
)
