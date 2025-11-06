# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos migration extract module."""

import json
from pathlib import Path

import click
from invenio_rdm_migrator.extract import Extract


class LegacyRecordStatsExtract(Extract):
    """LegacyRecordStatsExtract."""

    EVENT_TYPES = ["events.pageviews"]

    def __init__(self, filepath, **kwargs):
        """Constructor."""
        self.filepath = Path(filepath).absolute()

    def run(self):
        """Run."""
        with open(self.filepath, "r") as dump_file:
            data = json.load(dump_file)
            with click.progressbar(data) as records:
                for dump_record in records:
                    for t in self.EVENT_TYPES:
                        yield (t, dump_record)
