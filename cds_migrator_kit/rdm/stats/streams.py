# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM migration streams module."""
from invenio_rdm_migrator.streams import StreamDefinition
from invenio_rdm_migrator.transform import IdentityTransform


from .extract import LegacyRecordStatsExtract
from .load import CDSRecordStatsLoad

RecordStatsStreamDefinition = StreamDefinition(
    name="stats",
    extract_cls=LegacyRecordStatsExtract,
    transform_cls=IdentityTransform,
    load_cls=CDSRecordStatsLoad,
)
"""ETL stream for CDS to RDM records statistics."""
