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
from cds_migrator_kit.rdm.migration.affiliations.streams import (
    AffiliationsStreamDefinition,
)
from cds_migrator_kit.rdm.migration.runner import Runner
from cds_migrator_kit.rdm.migration.stats.runner import RecordStatsRunner
from cds_migrator_kit.rdm.migration.stats.streams import RecordStatsStreamDefinition
from cds_migrator_kit.rdm.migration.streams import (
    RecordStreamDefinition,
    UserStreamDefinition,
)

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
    """Migrate the legacy statistics for the records in `filepath`."""
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
    """Migrate the legacy statistics for the records in `filepath`."""
    log_dir = Path(current_app.config["CDS_MIGRATOR_KIT_LOGS_PATH"]) / "affiliations"
    runner = RecordAffiliationsRunner(
        stream_definition=AffiliationsStreamDefinition,
        filepath=filepath,
        log_dir=log_dir,
        dry_run=dry_run,
    )
    runner.run()


@migration.group()
def community():
    """Create and dump community id in streams.yaml."""
    pass


@community.command()
@click.option(
    "--slug",
    help="Slug of the community to be created. If found then fetch and dump the id.",
    required=True,
)
@click.option("--title", help="Title of the community to be created.")
@click.option(
    "--filepath",
    help="Path to the streams.yaml that the community id should be dumped.",
    required=True,
)
@with_appcontext
def dump(slug, title, filepath):
    """Read or create community slug and dump it to streams.yaml."""
    import yaml
    from invenio_access.permissions import system_identity
    from invenio_communities.proxies import current_communities
    from invenio_pidstore.errors import PIDDoesNotExistError

    try:
        res = current_communities.service.read(system_identity, slug)
    except PIDDoesNotExistError as exc:
        data = {
            "slug": slug,
            "metadata": {"title": title},
            "access": {"visibility": "public"},
        }
        res = current_communities.service.create(system_identity, data)

    streams = {}
    with open(filepath, "r") as fp:
        streams = yaml.safe_load(fp)

    streams["records"]["transform"]["community_id"] = str(res._record.id)

    with open(filepath, "w") as fp:
        yaml.safe_dump(streams, fp, default_flow_style=False, sort_keys=False)
