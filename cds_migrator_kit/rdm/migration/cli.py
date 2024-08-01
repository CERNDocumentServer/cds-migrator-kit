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

from cds_migrator_kit.rdm.migration.runner import Runner
from cds_migrator_kit.rdm.migration.streams import RecordStreamDefinition
from cds_migrator_kit.records.log import RDMJsonLogger

cli_logger = logging.getLogger("migrator")


@click.group()
def migration():
    """Migration CLI."""
    pass


@migration.command()
@with_appcontext
def run():
    """Run."""
    stream_config = current_app.config["CDS_MIGRATOR_KIT_STREAM_CONFIG"]
    runner = Runner(
        stream_definitions=[RecordStreamDefinition],
        config_filepath=Path(stream_config).absolute(),
    )
    runner.run()
