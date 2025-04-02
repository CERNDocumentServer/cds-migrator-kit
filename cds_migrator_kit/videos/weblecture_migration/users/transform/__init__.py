# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""cds-migrator-kit migration stats module."""
from cds_dojson.overdo import OverdoBase

users_migrator_marc21 = OverdoBase(
    entry_point_models="cds_migrator_kit.videos.submitter.model"
)
