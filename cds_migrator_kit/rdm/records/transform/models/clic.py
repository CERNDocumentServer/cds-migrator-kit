# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM CMS note model."""

from cds_migrator_kit.rdm.collection_conf import conf_query
from cds_migrator_kit.rdm.records.transform.models.base_publication_record import (
    rdm_base_publication_model,
)
from cds_migrator_kit.rdm.records.transform.models.research import ResearchModel
from cds_migrator_kit.transform.overdo import CdsOverdo


class ClicResearchModel(CdsOverdo):
    """Translation model for research."""

    # the queries of the collection configuration (migration_cli/collections/clic.yaml)
    # (deleted, hidden and dummy records are always excluded), translated to the
    # notation of the models
    __query__ = conf_query(
        "clic",
    )

    # the keys of the research model (the source of truth, see research.py); the fields
    # ignored only here would be added with `| {...}`
    # "970__a",  # merged tag
    __ignore_keys__ = ResearchModel.__ignore_keys__

    _default_fields = {
        "custom_fields": {},
    }


clic_research_model = ClicResearchModel(
    bases=(rdm_base_publication_model,),
    entry_point_group="cds_migrator_kit.migrator.rdm.rules.research",
)
