# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""cds Invenio WSGI application."""

from __future__ import absolute_import, print_function

from cds_migrator_kit.factory import create_app

application = create_app()
