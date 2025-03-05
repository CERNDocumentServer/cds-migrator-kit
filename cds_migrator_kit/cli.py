# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.
"""cds-migrator-kit command line module."""

import click

from cds_migrator_kit.import_utils import import_module


@click.group()
def cli():
    """Base CLI command that loads the subcommands."""
    pass


# Check for `rdm` dependencies
if import_module("cds_rdm.__init__"):
    from cds_migrator_kit.rdm.cli import migration

    cli = migration

# Check for `videos` dependencies
if import_module("cds.version"):
    from cds_migrator_kit.videos.weblecture_migration.cli import videos

    cli.add_command(videos, "videos")
