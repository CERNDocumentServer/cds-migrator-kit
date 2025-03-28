# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# cds-migration-kit is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""cds-migration-kit transform step module."""
import logging
from copy import deepcopy

import requests
from invenio_rdm_migrator.streams.records.transform import RDMRecordTransform

from cds_migrator_kit.transform.dumper import CDSRecordDump

from . import users_migrator_marc21

cli_logger = logging.getLogger("migrator")


class CDSToVideosSubmitterTransform(RDMRecordTransform):
    """CDSToRDMAffiliationTransform."""

    def __init__(
        self,
        dry_run=False,
    ):
        """Constructor."""
        self.dry_run = dry_run
        super().__init__()

    def _transform(self, entry):
        """Transform a single entry."""
        # creates the output structure for load step
        try:
            record_dump = CDSRecordDump(entry, dojson_model=users_migrator_marc21)
            record_dump.prepare_revisions()

            timestamp, json_data = record_dump.latest_revision
            email = json_data.get("submitter")
            return {"submitter": email}
        except Exception as e:
            cli_logger.exception(e)

    def _draft(self, entry):
        return None

    def _parent(self, entry):
        return None

    def _record(self, entry):
        return None
