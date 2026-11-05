# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM ECO model."""

from cds_migrator_kit.rdm.records.transform.models._config import IGNORE_SYSTEM_KEYS
from cds_migrator_kit.rdm.records.transform.models.base_record import (
    rdm_base_record_model,
)
from cds_migrator_kit.transform.overdo import CdsOverdo


class ECOModel(CdsOverdo):
    """Translation model for ECO records."""

    __query__ = """
        (
            (980__:POSTER -693__.e:LHCb -693__.e:ALICE -693__.e:ATLAS -693__.e:CMS -693__.e:NA64) OR
            (980__:NOTE AND 710__.5:IR)
            -980:MIGRATED -980:DELETED
        )
        OR (980__:NOTE AND 710__.5:IR)
    """

    __ignore_keys__ = IGNORE_SYSTEM_KEYS | {
        # "270__m",  # email of contact person - TODO: is it okay to ignore? example: 2908973
        # "270__p",  # contact person name - TODO: is it okay to ignore?
        "541__e",  # Original source poster https://cds.cern.ch/record/2695195/export/hm
        "594__a",  # PUB: 2749806, 2749822
        "6531_9",  # scheme of keywords
        # "773__p",  # display name of the related link TODO: is it okay to ignore?
        # "773__y",  # year, TODO: is it okay to ignore? https://cds.cern.ch/record/1452204/export/xm
        "773__v",  # TODO: is it okay to ignore? https://cds.cern.ch/record/1452204/export/xm
        "8560_f",  # contact email
        "961__a",  # CDS modification tag
        "961__b",  # CDS modification tag
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
