# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos base migration model."""

from cds_migrator_kit.transform.overdo import CdsOverdo


class CDSVideosBase(CdsOverdo):
    """Translation Index for CDS Weblectures."""


model = CDSVideosBase(bases=(), entry_point_group="cds_migrator_kit.videos.rules.base")
