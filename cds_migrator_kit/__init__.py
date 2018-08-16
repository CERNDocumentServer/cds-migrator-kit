# -*- coding: utf-8 -*-
#
# Copyright (C) 2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Migration tool kit from old invenio to new flavours."""

from __future__ import absolute_import, print_function

from .ext import cdsmigratorkit
from .version import __version__

__all__ = ('__version__', 'cdsmigratorkit')
