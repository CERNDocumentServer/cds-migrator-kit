# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos base migration model module."""

from cds_dojson.overdo import OverdoBase

# Matching to a correct model is happening here
videos_migrator_marc21 = OverdoBase(entry_point_models="cds_migrator_kit.videos.models")
