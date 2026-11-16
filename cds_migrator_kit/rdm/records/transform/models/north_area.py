# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM North Area models (NA58 & NA61-66)."""

from cds_migrator_kit.rdm.collection_conf import conf_query
from cds_migrator_kit.rdm.records.transform.models.base_publication_record import (
    rdm_base_publication_model,
)
from cds_migrator_kit.rdm.records.transform.models.research import ResearchModel
from cds_migrator_kit.transform.overdo import CdsOverdo


class NorthAreaModel(CdsOverdo):
    """Translation model for North Area experiments."""

    __query__ = conf_query(
        ["na58", "na61", "na62", "na63", "na64", "na65", "na66"],
        exclusions="-037:CERN-STUDENTS-Note-* -980:THESIS -980:SCICOMMPUBLSPSC -980:PHOTOLAB",
    )

    # the keys of the research model (the source of truth, see research.py) plus the
    # fields ignored only here
    __ignore_keys__ = ResearchModel.__ignore_keys__ | {
        "595_Da",  # From SIS: these can be ignored
        "595_Dd",  # From SIS: these can be ignored
        "595_Ds",  # From SIS: these can be ignored
        "595__9",  # From SIS: these can be ignored
        "903__s",  # 'public'
        "995__a",  # "Inspire"
    }

    _default_fields = {
        "custom_fields": {},
    }


north_area_model = NorthAreaModel(
    bases=(rdm_base_publication_model,),
    entry_point_group="cds_migrator_kit.migrator.rules.north_area",
)
