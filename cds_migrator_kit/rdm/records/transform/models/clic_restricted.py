# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM restricted CLIC (LCD) notes model."""

from cds_migrator_kit.rdm.collection_conf import conf_query
from cds_migrator_kit.rdm.records.transform.models.base_publication_record import (
    rdm_base_publication_model,
)
from cds_migrator_kit.rdm.records.transform.models.research import ResearchModel
from cds_migrator_kit.transform.overdo import CdsOverdo


class ClicRestrictedModel(CdsOverdo):
    """Translation model for the restricted (non-approved) CLIC/LCD notes."""

    # the queries of the collection configuration
    # (migration_cli/collections/clic_restr.yaml): the LCD notes that are NOT
    # approved. Disjoint from the public `clic` model (which requires
    # `591__b:approved`), so a record matches exactly one of the two.
    __query__ = conf_query(
        "clic_restr",
    )

    __ignore_keys__ = ResearchModel.__ignore_keys__

    _default_fields = {
        "custom_fields": {},
    }


clic_restricted_model = ClicRestrictedModel(
    bases=(rdm_base_publication_model,),
    entry_point_group="cds_migrator_kit.migrator.rdm.rules.research",
)
