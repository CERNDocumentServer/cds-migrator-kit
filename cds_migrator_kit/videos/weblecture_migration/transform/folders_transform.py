# -*- coding: utf-8 -*-
#
# Copyright (C) 2022-2025 CERN.
#
# CDS is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS transform step module."""
import logging
from copy import deepcopy
from pathlib import Path

import requests
from invenio_rdm_migrator.streams.records.transform import RDMRecordTransform

from cds_migrator_kit.errors import (
    ManualImportRequired,
    MissingRequiredField,
    UnexpectedValue,
)
from cds_migrator_kit.transform.dumper import CDSRecordDump

cli_logger = logging.getLogger("migrator")


class GenerateFilesFoldersTransform(RDMRecordTransform):
    """CDS to Videos generate files folders transform class."""

    def __init__(self, output_path, dry_run=False, dojson_model=None):
        """Constructor."""
        self.dry_run = dry_run
        self.dojson_model = dojson_model
        # Where to write collected master folder paths
        self.output_path = Path(output_path).absolute()
        super().__init__()

    def _transform(self, entry):
        """Transform a single entry."""
        try:
            record_dump = CDSRecordDump(entry, dojson_model=self.dojson_model)
            record_dump.prepare_revisions()

            timestamp, json_data = record_dump.latest_revision
            recid = json_data["legacy_recid"]
            files_list = entry.get("files") or json_data.get("files") or []
            master_paths = [
                item["master_path"] for item in files_list if "master_path" in item
            ]
            # Avoid writing duplicates within a single file open by normalizing set
            for master_path in master_paths:
                media_path = master_path.replace(
                    "/mnt/master_share/master_data", "/media_data"
                )
                with self.output_path.open("a") as fp:
                    fp.write(str(recid) + "--" + media_path + "\n")
            return None
        except UnexpectedValue as e:
            cli_logger.error(
                f"Unexpected value {e.field}: {e.subfield}",
                exc_info=False,
            )
        except Exception as e:
            cli_logger.error(f"Unexpected error {type(e).__name__}", exc_info=False)

    def _draft(self, entry):
        return None

    def _parent(self, entry):
        return None

    def _record(self, entry):
        return None
