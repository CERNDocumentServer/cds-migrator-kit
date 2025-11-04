# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos transform step module."""


import json
import os

from flask import current_app

from cds_migrator_kit.errors import ManualImportRequired


def get_files_by_recid(recid, directory):
    """
    Search JSON files in a directory for a given recid.
    Return a list of all "files" dicts for that recid.
    Stop searching after the first file that contains it.
    """
    recid = str(recid)
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

        if recid not in data:
            continue  # not in this dump, skip

        files = data[recid]

        eos_path = current_app.config["MOUNTED_MEDIA_CEPH_PATH"]
        missing = []

        # Check all paths inside the record
        for record in files:
            paths_to_check = []
            for key in ["master_video", "poster"]:
                if record.get(key):
                    paths_to_check.append(record[key])
            for key in ["frames", "additional_files"]:
                for item in record.get(key, []):
                    paths_to_check.append(item)
            for sub in record.get("subformats", []):
                if sub.get("path"):
                    paths_to_check.append(sub["path"])

            for path in paths_to_check:
                # For local
                if not eos_path.startswith("/eos"):
                    relative_path = path.split("/media_data")[-1]
                    path = eos_path + relative_path
                if not os.path.exists(path):
                    missing.append(path)

            if missing:
                raise ManualImportRequired(
                    message=f"Missing {len(missing)} files for recid {recid}",
                    stage="transform",
                    value=f"Missing: {missing}",
                    priority="critical",
                )
        return files
    return []
