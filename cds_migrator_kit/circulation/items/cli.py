# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS Migrator Items CLI."""

import json

import click
from flask.cli import with_appcontext


class LibrariesMigrator():
    """.

    Expected format of libraries JSON:
        [
          ...,
          {
            'id': ,
            'name': ,
            'address': ,
            'email': ,
            'phone': ,
            'type': ,
            'notes': ,
          },
          ...,
        ]

    """

    def __init__(self, libraries):
        """Constructor."""
        self.libraries = libraries

    def migrate(self):
        """Create new libraries record from the legacy libraries."""
        for library in self.libraries:
            pass


@click.command()
@click.argument('filepath', type=click.Path(exists=True))
@with_appcontext
def libraries(filepath):
    """Load libraries from JSON files and output ILS Records."""
    libraries = json.load(filepath)
    LibrariesMigrator(libraries).migrate()


@click.command()
@click.argument('sources', type=click.Path(exists=True))
@with_appcontext
def items(sources):
    """Load items from JSON files."""
    pass
