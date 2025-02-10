# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM migration rules module."""


from cds_migrator_kit.transform.xml_processing.quality.decorators import require

from ...models.note import cms_note_model


@cms_note_model.over("communities", "^980__")
@require(["a"])
def communities(self, key, value):
    """Translates communities."""
    return ["cms-notes"]
