# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM CMS note model."""

from cds_migrator_kit.rdm.records.transform.models._config import IGNORE_SYSTEM_KEYS
from cds_migrator_kit.rdm.records.transform.models.base_publication_record import (
    rdm_base_publication_model,
)
from cds_migrator_kit.transform.overdo import CdsOverdo


class ResearchModel(CdsOverdo):
    """Translation model for research."""

    __query__ = '693__.e:"DAMPE RE29" OR 693__.e:RE29 OR 693__.e:DAMPE OR 037__:DIRAC-NOTE* OR 037__:DIRAC-Note* OR 037__:DIRAC-CONF* OR 037__:DIRAC-DOC* OR 037__:DIRAC-PUB* OR 693__:UA2 OR 693__:UA4 OR 693__:UA5 OR 693__:UA8 OR 980__:INTNOTEHARPCDPPUBL OR 980__:PRIVIMXGAM -980__:THESIS -037__:CERN-STUDENTS-Note-* -980__:DELETED -980__.a:DUMMY -690C_.a:SCICOM -980:BULLETINNEWS'

    # IGNORE_SYSTEM_KEYS (models/_config.py) plus the fields ignored for research
    __ignore_keys__ = IGNORE_SYSTEM_KEYS | {
        "520__h",  # provenance of the description
        "540__8",  # agreed not to migrate open access related fields
        "542__8",  # agreed not to migrate open access related fields
        "695__e",  # some inspire tag
        "700__q",  # TODO ignore? aliteration of the name, used for searching
        "700__v",  # TODO drop?
        "913__y",  # citation
        "913__v",  # citation
        "913__t",  # citation
        "913__a",  # citation
        "913__c",  # citation
        "964__a",  # TODO: ignore?
    }

    _default_fields = {
        "custom_fields": {},
    }


research_model = ResearchModel(
    bases=(rdm_base_publication_model,),
    entry_point_group="cds_migrator_kit.migrator.rdm.rules.research",
)
