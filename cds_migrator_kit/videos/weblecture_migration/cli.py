# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos command line module."""
import logging
from pathlib import Path

import click
from flask import current_app
from flask.cli import with_appcontext

from cds_migrator_kit.rdm.runner import Runner
from cds_migrator_kit.videos.weblecture_migration.streams import RecordStreamDefinition

cli_logger = logging.getLogger("migrator")


@click.group()
def videos():
    """Migration CLI for videos."""
    pass


@videos.group()
def weblectures():
    """Migration CLI for weblectures."""
    pass


@weblectures.command()
@click.option(
    "--dry-run",
    is_flag=True,
)
@with_appcontext
def run(dry_run=False):
    """Run."""
    stream_config = current_app.config["CDS_MIGRATOR_KIT_VIDEOS_STREAM_CONFIG"]
    runner = Runner(
        stream_definitions=[RecordStreamDefinition],
        config_filepath=Path(stream_config).absolute(),
        dry_run=dry_run,
        collection="weblectures",
    )
    runner.run()
