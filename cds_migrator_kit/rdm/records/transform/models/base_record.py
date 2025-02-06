# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM base migration model."""

from cds_migrator_kit.transform.overdo import CdsOverdo
from cds_migrator_kit.transform.xml_processing.models.base import model as base_model

class CDSRDMBase(CdsOverdo):
    """Base XML transform model."""


rdm_base_record_model = CDSRDMBase(bases=(base_model,), entry_point_group="cds_migrator_kit.migrator.rdm.rules.base")
