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


class SYModel(CdsOverdo):
    """Translation model for MoUs."""

    __query__ = "(980__:PREPRINT OR 980__:ARTICLE OR 980__:ConferencePaper AND 710__.5:SY) OR 980__:INTNOTESYPUBL"

    __ignore_keys__ = {
        "0248_a",
        "0248_p",
        "0248_q",
        "100__m",  # email of contributor
        "300__a",  # number of pages
        "700__m",  # email of contributor
        "8564_8",  # file id
        "8564_s",  # bibdoc id
        "8564_x",  # icon thumbnails sizes
        "8564_y",  # file description - done by files dump
        "937__c",  # last modified by
        "937__s",  # last modification date
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


sy_model = SYModel(
    bases=(rdm_base_record_model,),
    entry_point_group="cds_migrator_kit.migrator.rules.sy",
)
