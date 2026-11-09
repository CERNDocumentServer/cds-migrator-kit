# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM MoEDAL model."""

from cds_migrator_kit.rdm.collection_conf import conf_query
from cds_migrator_kit.rdm.records.transform.models._config import IGNORE_SYSTEM_KEYS
from cds_migrator_kit.rdm.records.transform.models.base_publication_record import (
    rdm_base_publication_model,
)
from cds_migrator_kit.rdm.records.transform.models.research import ResearchModel
from cds_migrator_kit.transform.overdo import CdsOverdo


class MoEDALModel(CdsOverdo):
    """Translation model for MoEDAL."""

    __query__ = conf_query("moedal")

    __ignore_keys__ = IGNORE_SYSTEM_KEYS | ResearchModel.__ignore_keys__

    _default_fields = {
        "custom_fields": {},
    }


moedal_model = MoEDALModel(
    bases=(rdm_base_publication_model,),
    entry_point_group="cds_migrator_kit.migrator.rules.moedal",
)
