# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM migration rules module."""


from ...models.note import model
from ..quality.decorators import (
    require,
)


@model.over("communities", "^980__")
@require(["a"])
def communities(self, key, value):
    """Translates communities."""
    return ["cms-notes"]
