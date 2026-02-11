# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-Migrator-Kit is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Migrator-Kit comments extract module."""

import json
from pathlib import Path

import click
from invenio_rdm_migrator.extract import Extract


class LegacyCommentsExtract(Extract):
    """LegacyCommentsExtract."""

    def __init__(self, filepath, **kwargs):
        """Constructor."""
        self.filepath = Path(filepath).absolute()

    def run(self):
        """Run."""
        with open(self.filepath, "r") as dump_file:
            data = json.load(dump_file)
            with click.progressbar(
                data.items(), label="Processing comments"
            ) as metadata:
                for recid, comments in metadata:
                    yield (recid, comments)


class LegacyCommentersExtract(Extract):
    """LegacyCommentersExtract."""

    def __init__(self, filepath, **kwargs):
        """Constructor."""
        self.filepath = Path(filepath).absolute()

    def run(self):
        """Run."""
        with open(self.filepath, "r") as dump_file:
            data = json.load(dump_file)
            with click.progressbar(
                data.items(), label="Processing commenters"
            ) as metadata:
                for user_data in metadata:
                    # user_data is a list (from JSON): [user_id, user_email, user_nickname, user_note, user_last_login]
                    email = user_data[1]
                    yield {"submitter": email}
