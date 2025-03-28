# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# cds-migration-kit is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""cds-migration-kit migration streams module."""

from invenio_rdm_migrator.streams import StreamDefinition

from cds_migrator_kit.extract.extract import LegacyExtract

from .load import VideosSubmitterLoad
from .transform.transform import CDSToVideosSubmitterTransform

SubmitterStreamDefinition = StreamDefinition(
    name="submitters",
    extract_cls=LegacyExtract,
    transform_cls=CDSToVideosSubmitterTransform,
    load_cls=VideosSubmitterLoad,
)
"""ETL stream for Videos to import users."""
