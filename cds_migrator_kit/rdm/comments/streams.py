# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-Migrator-Kit is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Migrator-Kit comments streams module."""
from invenio_rdm_migrator.streams import StreamDefinition
from invenio_rdm_migrator.transform import IdentityTransform

from cds_migrator_kit.rdm.comments.extract import (
    LegacyCommentersExtract,
    LegacyCommentsExtract,
)
from cds_migrator_kit.users.load import CDSSubmitterLoad

from .load import CDSCommentsLoad

CommentsStreamDefinition = StreamDefinition(
    name="comments",
    extract_cls=LegacyCommentsExtract,
    transform_cls=IdentityTransform,
    load_cls=CDSCommentsLoad,
)
"""ETL stream for CDS to RDM comments."""

CommenterStreamDefinition = StreamDefinition(
    name="commenters",
    extract_cls=LegacyCommentersExtract,
    transform_cls=IdentityTransform,
    load_cls=CDSSubmitterLoad,
)
"""ETL stream for CDS to RDM commenters."""
