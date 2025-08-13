# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM CMS note model."""
from cds_migrator_kit.rdm.records.transform.models.base_publication_record import (
    rdm_base_publication_model,
)
from cds_migrator_kit.transform.overdo import CdsOverdo


class ENModel(CdsOverdo):
    """Translation model for MoUs."""

    __query__ = "980__:INTNOTEENPUBL OR (980__:ARTICLE OR 980__:PREPRINT AND 710__.5:EN) OR 980__:ENLIB"

    __ignore_keys__ = {
        "0248_a",
        "0248_p",
        "0248_q",
        "035__h",  # oai identifiers in 1215391
        "035__d",  # oai identifiers in 1215391
        "035__t",  # oai identifiers in 1215391
        "035__m",  # oai identifiers in 1215391
        "037__c",  # arxiv subjects
        "100__m",  # email of contributor
        "300__a",  # number of pages
        "520__9",  # arxiv abstract
        "500__9",  # arxiv comments
        "540__3",  # material of license
        "542__3",  # material of copyrights
        "700__m",  # email of contributor
        "8564_8",  # file id
        "8564_s",  # bibdoc id
        "8564_x",  # icon thumbnails sizes
        "8564_y",  # file description - done by files dump
        "8564_y",  # file description - done by files dump
        "916__y",  # year of publication, redundant
        "937__c",  # last modified by
        "937__s",  # last modification date
        "960__a",  # base number
        "961__c",  # CDS modification tag # TODO
        "961__h",  # CDS modification tag # TODO
        "961__l",  # CDS modification tag # TODO
        "961__x",  # CDS modification tag # TODO
        "981__a",  # duplicate record id
        "980__b",  # additional article tag
    }

    _default_fields = {
        "custom_fields": {},
    }


en_model = ENModel(
    bases=(rdm_base_publication_model,),
    entry_point_group="cds_migrator_kit.migrator.rules.en",
)

