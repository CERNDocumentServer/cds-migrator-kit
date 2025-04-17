# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos command line module."""
import logging
from pathlib import Path
import re

from cds_migrator_kit.videos.weblecture_migration.users.api import CDSVideosMigrationUserAPI
import click
from flask import current_app
from flask.cli import with_appcontext
from invenio_accounts.models import User
from sqlalchemy.exc import NoResultFound

from cds_migrator_kit.runner.runner import Runner
from cds_migrator_kit.videos.weblecture_migration.logger import VideosJsonLogger
from cds_migrator_kit.videos.weblecture_migration.streams import RecordStreamDefinition
from cds_migrator_kit.videos.weblecture_migration.users.runner import (
    VideosSubmitterRunner,
)
from cds_migrator_kit.videos.weblecture_migration.users.streams import (
    SubmitterStreamDefinition,
)

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
    VideosJsonLogger.initialize(runner.log_dir)
    runner.run()


@videos.group()
def submitters():
    """Migration CLI for weblectures."""
    pass


@submitters.command()
@click.option(
    "--dry-run",
    is_flag=True,
)
@with_appcontext
def run(dry_run=False):
    """Migrate the users(submitters) if missing."""
    stream_config = current_app.config["CDS_MIGRATOR_KIT_VIDEOS_STREAM_CONFIG"]
    runner = VideosSubmitterRunner(
        stream_definition=SubmitterStreamDefinition,
        config_filepath=Path(stream_config).absolute(),
        dry_run=dry_run,
    )
    runner.run()


@submitters.command()
@with_appcontext
def create_system_user():
    """Create the sytem user if missing."""
    email = current_app.config["WEBLECTURES_MIGRATION_SYSTEM_USER"]

    if not email:
        cli_logger.error("System user email is not configured.")
        return

    try:
        user = User.query.filter_by(email=email).one()
        cli_logger.info(f"User {email} exists.")
        return
    except NoResultFound:
        username = email.split("@")[0].replace(".", "")
        username = re.sub(r"\W+", "", username).lower()
        extra_data = {"migration": {"note": "System user for migration"}}
        user_api = CDSVideosMigrationUserAPI()

        try:
            user = user_api.create_user(
                email,
                name="Weblectures System User",
                username=username,
                person_id=None,
                extra_data=extra_data,
            )
        except Exception as exc:
            cli_logger.error(
                f"System user creation failed: {email}, {username}\n {exc}"
            )
