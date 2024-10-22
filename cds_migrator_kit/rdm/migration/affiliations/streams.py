# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM migration streams module."""
from invenio_rdm_migrator.streams import StreamDefinition
from invenio_rdm_migrator.transform import IdentityTransform

from cds_migrator_kit.rdm.migration.extract import LegacyExtract

from .load import CDSAffiliationsLoad
from .transform import CDSToRDMAffiliationTransform

AffiliationsStreamDefinition = StreamDefinition(
    name="affiliations",
    extract_cls=LegacyExtract,
    transform_cls=CDSToRDMAffiliationTransform,
    load_cls=CDSAffiliationsLoad,
)
"""ETL stream for CDS to RDM records statistics."""
