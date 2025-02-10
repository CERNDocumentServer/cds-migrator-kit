# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos migration streams module."""
from invenio_rdm_migrator.streams import StreamDefinition

from cds_migrator_kit.extract.extract import LegacyExtract
from cds_migrator_kit.videos.weblecture_migration.load.load import CDSVideosLoad
from cds_migrator_kit.videos.weblecture_migration.transform.transform import (
    CDSToVideosRecordTransform,
)

RecordStreamDefinition = StreamDefinition(
    name="records",
    extract_cls=LegacyExtract,
    transform_cls=CDSToVideosRecordTransform,
    load_cls=CDSVideosLoad,
)
"""ETL stream for CDS to CDS Videos records."""
