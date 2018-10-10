# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Helper functions for usage in tests."""

import json
from os.path import join


def load_json(datadir, filename):
    """Load file in json format."""
    filepath = join(datadir, filename)
    data = None
    with open(filepath, 'r') as file_:
        data = json.load(file_)
    return data
