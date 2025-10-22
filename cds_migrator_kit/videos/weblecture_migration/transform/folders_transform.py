# -*- coding: utf-8 -*-
#
# Copyright (C) 2022-2025 CERN.
#
# CDS is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS transform step module."""
import json
import logging

from flask import current_app
from invenio_rdm_migrator.streams.records.transform import RDMRecordTransform

from cds_migrator_kit.errors import (
    UnexpectedValue,
)
from cds_migrator_kit.transform.dumper import CDSRecordDump

cli_logger = logging.getLogger("migrator")


class GenerateFilesFoldersTransform(RDMRecordTransform):
    """CDS to Videos generate files folders transform class."""

    def __init__(self, dry_run=False, dojson_model=None, output_file=None):
        """Constructor."""
        self.dry_run = dry_run
        self.dojson_model = dojson_model
        self.output_file = output_file
        self.first_entry = True
        super().__init__()

    def find_files(self, entry):
        """Find files in the entry and return list of dicts with files info."""
        master_paths = [
            item["master_path"] for item in entry.get("files") if "master_path" in item
        ]
        group_files = []
        if len(master_paths) == 1:
            files = entry.get("files")
            record_entry = {"files": files, "recid": self.recid}
            group_files.append(record_entry)
        elif len(master_paths) > 1:
            files = entry.get("files")

            for master_path in master_paths:
                # master paths always has indico id
                indico_id = master_path.split("/")[-1]
                # get the files that has the indico id in the path
                files_list = [
                    file
                    for file in files
                    if "path" in file and indico_id in file["path"]
                ]
                files_list.append({"master_path": master_path})
                record_entry = {"files": files_list, "recid": self.recid}
                group_files.append(record_entry)
        return group_files

    def _transform(self, entry):
        """Transform a single entry."""
        try:
            record_dump = CDSRecordDump(entry, dojson_model=self.dojson_model)
            record_dump.prepare_revisions()

            timestamp, json_data = record_dump.latest_revision
            recid = json_data["legacy_recid"]
            self.recid = recid

            files_list = self.find_files(json_data)

            for file_entry in files_list:
                if not self.first_entry:
                    self.output_file.write(",\n")
                else:
                    self.first_entry = False

                json.dump(file_entry, self.output_file, ensure_ascii=False, indent=4)

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
