# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM migration errors module."""

from dojson.errors import DoJSONException


class LossyConversion(DoJSONException):
    """Data lost during migration."""

    description = "[Migration rule missing]"

    def __init__(self, missing=None, *args, **kwargs):
        """Exception custom initialisation."""
        self.missing = missing
        self.stage = "transform"
        self.field = self.missing
        self.type = self.__class__.__name__
        self.priority = "warning"
        super().__init__(*args)


