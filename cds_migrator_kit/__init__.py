# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Migration tool kit from old Invenio to new Invenio."""

from .ext import CdsMigratorKit

__all__ = ("__version__", "CdsMigratorKit")

# Fix for Python3.10 breaking changes
# The underlying module `invenio-query-parser` uses MutableMapping and Sequence from collections
# But they are moved to collections.abc starting from Python3.10

import collections
import collections.abc

collections.MutableMapping = collections.abc.MutableMapping
collections.Sequence = collections.abc.Sequence

__version__ = "0.1.0.dev20260000"
