# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos transform step module."""


import json
import os

from cds_migrator_kit.errors import ManualImportRequired


def get_files_by_recid(recid, directory):
    """
    Search JSON files in a directory for a given recid.
    Return a list of all "files" dicts for that recid.
    Stop searching after the first file that contains it.
    """
    matching_files = []

    # List all .json files in the directory
    json_files = sorted(
        [
            os.path.join(directory, f)
            for f in os.listdir(directory)
            if f.endswith(".json")
        ]
    )

    for json_file in json_files:
        with open(json_file, "r") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError as e:
                raise ManualImportRequired(
                    message=f"JSON decode error while using generated EOS paths!",
                    value=f"File: {json_file}",
                    stage="transform",
                    priority="critical",
                )

        # Find all entries with matching recid in the file
        matches = [entry["files"] for entry in data if entry.get("recid") == recid]

        if matches:
            matching_files.extend(matches)
            break  # No need to read further

    return matching_files
