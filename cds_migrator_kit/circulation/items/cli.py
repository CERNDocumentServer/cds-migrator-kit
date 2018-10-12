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

    INVALID_LIBRARY_IDS = [43]
    VALID_LIBRARY_TYPES = ['main', 'internal']

    def __init__(self, libraries):
        """Constructor."""
        self.libraries = [l for l in libraries
                          if l['type'] in self.VALID_LIBRARY_TYPES and
                          l['id'] not in self.INVALID_LIBRARY_IDS]

    def _migrate_internal_libraries(self, location_pid):
        """Return new internal libraries records."""
        internal_libraries = []
        for i, library in enumerate(self.libraries):
            internal_library = {
                'libid': i,
                'locid': location_pid,
                'legacy_id': library['id'],
                'name': library['name'],
                'address': library.get('address', ''),
                'email': library.get('email', ''),
                'phone': library.get('phone', ''),
                'notes': library.get('notes', ''),
            }
            internal_libraries.append(internal_library)
        return internal_libraries

    def migrate(self):
        """Return location and internal libraries records."""
        location_pid = '1'
        internal_libraries = self._migrate_internal_libraries(location_pid)

        location = {
            'locid': '1',
            'name': 'CERN Central Library',
        }

        return location, internal_libraries


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
