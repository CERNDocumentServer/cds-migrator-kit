# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM command line module."""
import logging
from pathlib import Path

import click
from flask import current_app
from flask.cli import with_appcontext

from cds_migrator_kit.rdm.migration.affiliations.runner import RecordAffiliationsRunner
from cds_migrator_kit.rdm.migration.runner import Runner
from cds_migrator_kit.rdm.migration.stats.runner import RecordStatsRunner
from cds_migrator_kit.rdm.migration.streams import (
    RecordStreamDefinition,
    UserStreamDefinition,
)
from cds_migrator_kit.rdm.migration.affiliations.streams import (
    AffiliationsStreamDefinition,
)
from cds_migrator_kit.rdm.migration.stats.streams import RecordStatsStreamDefinition

cli_logger = logging.getLogger("migrator")


@click.group()
def migration():
    """Migration CLI."""
    pass


@migration.command()
@click.option(
    "--dry-run",
    is_flag=True,
)
@with_appcontext
def run(dry_run=False):
    """Run."""
    stream_config = current_app.config["CDS_MIGRATOR_KIT_STREAM_CONFIG"]
    runner = Runner(
        stream_definitions=[RecordStreamDefinition],
        # stream_definitions=[UserStreamDefinition],
        config_filepath=Path(stream_config).absolute(),
        dry_run=dry_run,
    )
    runner.run()


@migration.group()
def stats():
    """Migration CLI for statistics."""
    pass


@stats.command()
@click.option(
    "--dry-run",
    is_flag=True,
)
@click.option(
    "--filepath",
    help="Path to the list of records file that the legacy statistics will be migrated.",
)
@with_appcontext
def run(filepath, dry_run=False):
    """Migrate the legacy statistics for the records in `filepath`"""
    stream_config = current_app.config["CDS_MIGRATOR_KIT_RECORD_STATS_STREAM_CONFIG"]
    stream_config["DEST_SEARCH_INDEX_PREFIX"] = (
        f"{current_app.config['SEARCH_INDEX_PREFIX']}events-stats"
    )
    stream_config["DEST_SEARCH_HOSTS"] = current_app.config["SEARCH_HOSTS"]
    log_dir = Path(current_app.config["CDS_MIGRATOR_KIT_LOGS_PATH"]) / "stats"
    runner = RecordStatsRunner(
        stream_definition=RecordStatsStreamDefinition,
        filepath=filepath,
        config=stream_config,
        log_dir=log_dir,
        dry_run=dry_run,
    )
    runner.run()


@migration.group()
def affiliations():
    """Migration CLI for affiliations."""
    pass


@affiliations.command()
@click.option(
    "--dry-run",
    is_flag=True,
)
@click.option(
    "--filepath",
    help="Path to the list of records file that the legacy statistics will be migrated.",
)
@with_appcontext
def run(filepath, dry_run=False):
    """Migrate the legacy statistics for the records in `filepath`"""
    log_dir = Path(current_app.config["CDS_MIGRATOR_KIT_LOGS_PATH"]) / "affiliations"
    runner = RecordAffiliationsRunner(
        stream_definition=AffiliationsStreamDefinition,
        filepath=filepath,
        log_dir=log_dir,
        dry_run=dry_run,
    )
    runner.run()
