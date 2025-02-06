# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM migration streams module."""
from invenio_rdm_migrator.streams import StreamDefinition

from cds_migrator_kit.extract.extract import LegacyExtract
from .transform.users import CDSRDMUserTransform, CDSUserIntermediaryLoad

UserStreamDefinition = StreamDefinition(
    name="users",
    extract_cls=LegacyExtract,
    transform_cls=CDSRDMUserTransform,
    load_cls=CDSUserIntermediaryLoad,
)
"""ETL stream for CDS to import users."""
