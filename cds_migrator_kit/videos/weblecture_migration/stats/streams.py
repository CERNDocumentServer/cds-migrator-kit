# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos migration streams module."""
from invenio_rdm_migrator.streams import StreamDefinition
from invenio_rdm_migrator.transform import IdentityTransform

from .extract import LegacyRecordStatsExtract
from .load import VideosRecordStatsLoad

RecordStatsStreamDefinition = StreamDefinition(
    name="stats",
    extract_cls=LegacyRecordStatsExtract,
    transform_cls=IdentityTransform,
    load_cls=VideosRecordStatsLoad,
)
"""ETL stream for CDS to Videos records statistics."""
