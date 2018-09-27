# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS base Invenio configuration."""

from __future__ import absolute_import, print_function

from invenio_base.app import create_cli

from .factory import create_app

cli = create_cli(create_app=create_app)
