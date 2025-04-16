# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# cds-migration-kit is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""cds-migration-kit migration streams module."""

from invenio_rdm_migrator.streams import StreamDefinition

from cds_migrator_kit.extract.extract import LegacyExtract

from cds_migrator_kit.users.load import CDSSubmitterLoad
from cds_migrator_kit.users.transform import SubmitterTransform

SubmitterStreamDefinition = StreamDefinition(
    name="submitters",
    extract_cls=LegacyExtract,
    transform_cls=SubmitterTransform,
    load_cls=CDSSubmitterLoad,
)
"""ETL stream for Videos to import users."""
