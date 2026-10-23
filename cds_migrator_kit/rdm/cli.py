# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM command line module."""

import logging
import os
from datetime import date, datetime
from pathlib import Path

import click
from flask import current_app
from flask.cli import with_appcontext

from cds_migrator_kit.rdm import collection_conf
from cds_migrator_kit.rdm.affiliations.runner import RecordAffiliationsRunner
from cds_migrator_kit.rdm.affiliations.streams import AffiliationsStreamDefinition
from cds_migrator_kit.rdm.comments.runner import CommenterRunner, CommentsRunner
from cds_migrator_kit.rdm.comments.streams import (
    CommenterStreamDefinition,
    CommentsStreamDefinition,
)
from cds_migrator_kit.rdm.records.streams import (  # UserStreamDefinition,
    RecordStreamDefinition,
)
from cds_migrator_kit.rdm.stats.runner import RecordStatsRunner
from cds_migrator_kit.rdm.stats.streams import RecordStatsStreamDefinition
from cds_migrator_kit.rdm.users.runner import PeopleAuthorityRunner, SubmitterRunner
from cds_migrator_kit.rdm.users.streams import (
    SubmitterStreamDefinition,
    UserStreamDefinition,
)
from cds_migrator_kit.rdm.users.transform.xml_processing.models.people import (
    PeopleAuthority,
)
from cds_migrator_kit.runner.runner import Runner

cli_logger = logging.getLogger("migrator")

BOOKMARK_START_DATE = "2000-06-26T15:56:05.755394"


def _env_and_root():
    """Environment the CLI runs in (CDS_MIGRATOR_KIT_ENV) and the data root."""
    env = current_app.config["CDS_MIGRATOR_KIT_ENV"]
    root = current_app.config.get("CDS_MIGRATOR_KIT_MIGRATION_ROOT")
    if env == "local" and not root:
        root = current_app.config.get("CDS_MIGRATOR_KIT_LOCAL_MIGRATION_ROOT")
    return env, root


def _resolve_community(slug):
    """Id of the community with the given slug in this instance."""
    from invenio_access.permissions import system_identity
    from invenio_communities.proxies import current_communities
    from invenio_pidstore.errors import PIDDoesNotExistError

    try:
        return str(current_communities.service.read(system_identity, slug)._record.id)
    except PIDDoesNotExistError:
        raise click.UsageError(
            f"The community `{slug}` does not exist in this environment: create it first."
        )


def _collection_config(collection):
    """Config of the collection from migration_cli/collections/<collection>.yaml.

    Returns None when the collection has none (it is then read from the
    streams.yaml given by CDS_MIGRATOR_KIT_STREAM_CONFIG).
    """
    try:
        env, root = _env_and_root()
    except click.UsageError:
        return None  # local setup, no EOS folder: streams.yaml only
    try:
        return collection_conf.build_stream_config(
            collection,
            env,
            db_uri=current_app.config["SQLALCHEMY_DATABASE_URI"],
            resolve_community=_resolve_community,
            root=root,
            confs_dir=current_app.config.get("CDS_MIGRATOR_KIT_COLLECTIONS_DIR"),
        )
    except FileNotFoundError:
        return None


def _config_for(collection, section="records"):
    """Config of the collection, or None to use the entry of streams.yaml.

    Fails with a clear message when the collection is in neither.
    """
    config = _collection_config(collection)
    if config is not None:
        return config
    import yaml

    with open(current_app.config["CDS_MIGRATOR_KIT_STREAM_CONFIG"]) as fp:
        entries = (yaml.safe_load(fp) or {}).get(section) or {}
    if collection not in entries:
        confs_dir = (
            current_app.config.get("CDS_MIGRATOR_KIT_COLLECTIONS_DIR")
            or collection_conf.default_confs_dir()
        )
        raise click.UsageError(
            f"No configuration for the collection `{collection}`: add "
            f"{os.path.join(confs_dir, collection + '.yaml')}"
        )
    return None


def _collection_dirs(collection):
    """Return the (dump dir, shared users dir) of a collection."""
    env, root = _env_and_root()
    try:
        conf = collection_conf.read_conf(
            collection, current_app.config.get("CDS_MIGRATOR_KIT_COLLECTIONS_DIR")
        )
    except FileNotFoundError as exc:
        raise click.UsageError(str(exc))
    base = collection_conf.working_dir(conf, env, root)
    return os.path.join(base, "dump"), collection_conf.users_dir(env, root)


@click.group()
def migration():
    """Migration CLI."""
    pass


@migration.command()
@click.option(
    "--dry-run",
    is_flag=True,
)
@click.option(
    "--collection",
    help="Collection name to be migrated",
    required=True,
)
@click.option(
    "--keep-logs",
    is_flag=True,
)
@click.option(
    "--workers",
    type=int,
    default=None,
    help=(
        "Number of threads for parallel record transformation. "
        "Defaults to sequential (no threads). "
        "Can also be set per-collection in streams.yaml under transform.workers."
    ),
)
@with_appcontext
def run(collection, dry_run=False, keep_logs=False, workers=None):
    """Run."""
    stream_config = current_app.config["CDS_MIGRATOR_KIT_STREAM_CONFIG"]
    runner = Runner(
        stream_definitions=[RecordStreamDefinition],
        # stream_definitions=[UserStreamDefinition],
        config_filepath=Path(stream_config).absolute(),
        dry_run=dry_run,
        collection=collection,
        keep_logs=keep_logs,
        workers=workers,
        config=_config_for(collection, "records"),
    )

    runner.run()


@migration.group()
def stats():
    """Migration CLI for statistics."""
    pass


def _run_stats_events(filepath, less_than_date, dry_run):
    """Migrate the raw legacy statistics events of the records in `filepath`."""
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
        less_than_date=less_than_date,
        log_dir=log_dir,
        dry_run=dry_run,
    )
    runner.run()


_less_than_date_option = click.option(
    "--less-than-date",
    default=lambda: datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    help="ISO string e.g 2024-12-13T20:00:00 to migrate events up to this date.",
)


@stats.command()
@click.option(
    "--dry-run",
    is_flag=True,
)
@click.option(
    "--filepath",
    help="Path to the list of records file that the legacy statistics will be migrated.",
)
@_less_than_date_option
@with_appcontext
def run(filepath, less_than_date, dry_run=False):
    """Migrate the legacy statistics for the records in `filepath`."""
    _run_stats_events(filepath, less_than_date, dry_run)


@stats.group()
def events():
    """Raw statistics events."""
    pass


@events.command("run")
@click.option(
    "--dry-run",
    is_flag=True,
)
@click.option(
    "--collection",
    help="Collection whose records' legacy events are migrated.",
    required=True,
)
@click.option(
    "--filepath",
    help="Records state file. Default: the one written by `migration run` "
    "for the collection.",
)
@_less_than_date_option
@with_appcontext
def events_run(collection, filepath, less_than_date, dry_run=False):
    """Migrate the legacy statistics events of a migrated collection."""
    if not filepath:
        filepath = os.path.join(
            current_app.config["CDS_MIGRATOR_KIT_LOGS_PATH"],
            collection,
            "rdm_records_state.json",
        )
    if not os.path.isfile(filepath):
        raise click.UsageError(f"Records state file not found: {filepath}")
    _run_stats_events(filepath, less_than_date, dry_run)


@stats.command()
@click.option(
    "--start-date",
    default=BOOKMARK_START_DATE,
    show_default=True,
    help="Date the aggregation bookmarks are reset to.",
)
@click.option(
    "--end-date",
    default=lambda: date.today().isoformat(),
    help="Aggregate events up to this date. Default: today.",
)
@with_appcontext
def aggregate(start_date, end_date):
    """Reset the aggregation bookmarks, aggregate the events and reindex stats.

    Run it once all the events of all the collections are ingested.
    """
    from invenio_rdm_records.services.tasks import reindex_stats
    from invenio_search import current_search_client
    from invenio_search.utils import prefix_index
    from invenio_stats.tasks import aggregate_events

    bookmarks_index = prefix_index("stats-bookmarks")
    if not current_search_client.indices.exists(index=bookmarks_index):
        raise click.UsageError(f"Index {bookmarks_index} does not exist.")

    click.echo(f"Resetting the bookmarks in {bookmarks_index} to {start_date}")
    current_search_client.delete_by_query(
        index=bookmarks_index, body={"query": {"match_all": {}}}, refresh=True
    )
    for aggregation_type in ("file-download-agg", "record-view-agg", "stats_reindex"):
        current_search_client.index(
            index=bookmarks_index,
            body={"date": start_date, "aggregation_type": aggregation_type},
            refresh=True,
        )

    click.echo(f"Aggregating events up to {end_date}")
    aggregate_events(["record-view-agg", "file-download-agg"], end_date=end_date)

    click.echo("Reindexing the stats")
    reindex_stats(["stats-record-view", "stats-file-download"])


@migration.group()
def users():
    """Migration CLI for users by collection."""
    pass


@users.command()
@click.option(
    "--dry-run",
    is_flag=True,
)
@click.option(
    "--filepath",
    help=".",
)
@click.option(
    "--dirpath",
    help=".",
)
@click.option("--collection")
@with_appcontext
def people_run(filepath, collection, dirpath, dry_run=False):
    """Migrate the legacy people collection`."""
    log_dir = Path(current_app.config["CDS_MIGRATOR_KIT_LOGS_PATH"]) / "users"
    runner = PeopleAuthorityRunner(
        stream_definition=UserStreamDefinition,
        filepath=filepath,
        log_dir=log_dir,
        dry_run=dry_run,
        dirpath=dirpath,
    )
    runner.run()


@users.command()
@click.option(
    "--dry-run",
    is_flag=True,
)
@click.option(
    "--dirpath",
    help="Path to the record dumps dir to extract submitters from.",
)
@click.option(
    "--users-dir-path",
    help="Path to the record dumps dir to extract submitters from.",
)
@with_appcontext
def submitters_run(dirpath, users_dir_path, dry_run=False):
    """Migrate the legacy statistics for the records in `filepath`."""
    log_dir = Path(current_app.config["CDS_MIGRATOR_KIT_LOGS_PATH"]) / "submitters"
    runner = SubmitterRunner(
        stream_definition=SubmitterStreamDefinition,
        missing_users_dir=users_dir_path,
        dirpath=dirpath,
        log_dir=log_dir,
        dry_run=dry_run,
    )
    runner.run()


@users.group()
def submitters():
    """Submitters accounts of a collection."""
    pass


@submitters.command("run")
@click.option("--dry-run", is_flag=True)
@click.option(
    "--collection",
    help="Collection whose record submitters are created.",
    required=True,
)
@with_appcontext
def submitters_collection_run(collection, dry_run=False):
    """Create the missing submitters accounts of a collection."""
    dirpath, users_dir_path = _collection_dirs(collection)
    ctx = click.get_current_context()
    ctx.invoke(
        submitters_run, dirpath=dirpath, users_dir_path=users_dir_path, dry_run=dry_run
    )


@users.group()
def commenters():
    """Commenters accounts of a collection."""
    pass


@commenters.command("run")
@click.option("--dry-run", is_flag=True)
@click.option(
    "--collection",
    help="Collection whose commenters are created.",
    required=True,
)
@with_appcontext
def commenters_collection_run(collection, dry_run=False):
    """Create the commenters accounts of a collection."""
    ctx = click.get_current_context()
    ctx.invoke(commenters_run, collection=collection, dry_run=dry_run)


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
def affiliations_run(filepath, dry_run=False):
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


@migration.group()
def comments():
    """Migration CLI for comments."""
    pass


@comments.command()
@click.option(
    "--dry-run",
    is_flag=True,
)
@click.option(
    "--collection",
    help="Collection name to be migrated (paths loaded from streams.yaml).",
    required=True,
)
@with_appcontext
def run(collection, dry_run=False):
    """Migrate the comments for the given collection."""
    stream_config = current_app.config["CDS_MIGRATOR_KIT_STREAM_CONFIG"]
    log_dir = Path(current_app.config["CDS_MIGRATOR_KIT_LOGS_PATH"]) / "comments"
    runner = CommentsRunner(
        stream_definition=CommentsStreamDefinition,
        config_filepath=Path(stream_config).absolute(),
        log_dir=log_dir,
        collection=collection,
        dry_run=dry_run,
        config=_config_for(collection, "comments"),
    )
    runner.run()


@comments.command()
@click.option(
    "--dry-run",
    is_flag=True,
)
@click.option(
    "--collection",
    help="Collection name whose commenters should be pre-created (paths loaded from streams.yaml).",
    required=True,
)
@with_appcontext
def commenters_run(collection, dry_run=False):
    """Pre-create commenters accounts for the given collection."""
    stream_config = current_app.config["CDS_MIGRATOR_KIT_STREAM_CONFIG"]
    log_dir = Path(current_app.config["CDS_MIGRATOR_KIT_LOGS_PATH"]) / "comments"
    runner = CommenterRunner(
        stream_definition=CommenterStreamDefinition,
        config_filepath=Path(stream_config).absolute(),
        log_dir=log_dir,
        collection=collection,
        dry_run=dry_run,
        config=_config_for(collection, "comments"),
    )
    runner.run()
