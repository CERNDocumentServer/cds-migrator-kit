# -*- coding: utf-8 -*-
#
# Copyright (C) 2022-2025 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM transform step module."""
import logging
from copy import deepcopy

import requests
from invenio_rdm_migrator.streams.records.transform import RDMRecordTransform

from cds_migrator_kit.transform.dumper import CDSRecordDump

cli_logger = logging.getLogger("migrator")


class SubmitterTransform(RDMRecordTransform):
    """CDSToRDMAffiliationTransform."""

    def __init__(self, dry_run=False, dojson_model=None):
        """Constructor."""
        self.dry_run = dry_run
        self.dojson_model = dojson_model
        super().__init__()

    def _transform(self, entry):
        """Transform a single entry."""
        # creates the output structure for load step
        try:
            record_dump = CDSRecordDump(entry, dojson_model=self.dojson_model,
                                        raise_on_missing_rules=False)
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
