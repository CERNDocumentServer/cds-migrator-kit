# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM CMS note model."""

from cds_migrator_kit.transform.overdo import CdsOverdo


class CMSNote(CdsOverdo):
    """Translation Index for CDS Books."""

    __query__ = "980__:INTNOTECMSPUBL 980__:NOTE"

    __model_ignore_keys__ = {}

    _default_fields = None


cms_note_model = CMSNote(bases=(), entry_point_group="cds_rdm.migrator.rules")
