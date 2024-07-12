# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM command line module."""

from pathlib import Path
from flask.cli import with_appcontext
import click


from cds_migrator_kit.rdm.migration.streams import RecordStreamDefinition
from cds_migrator_kit.rdm.migration.runner import Runner


@click.group()
def migration():
    """Migration CLI."""
    pass


@migration.command()
@with_appcontext
def run():
    """Run."""
    # from flask.globals import _app_ctx_stack
    # app = _app_ctx_stack.top.app
    # with app.app_context():
    runner = Runner(
        stream_definitions=[
            RecordStreamDefinition
        ],
        config_filepath=Path("cds_migrator_kit/rdm/migration/streams.yaml").absolute(),
    )
    runner.run()
