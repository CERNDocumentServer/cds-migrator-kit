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


class TechnicalSupportModel(CdsOverdo):
    """Translation model for MoUs."""

    __query__ = """980__:ARTICLE OR
                   980__:PREPRINT OR
                   980__:DEMSUPPLIERS OR
                   980__:INTNOTETSPUBL OR 690C_:INTNOTETSPUBL
                   AND 710__.5:TS OR 710__.5:ST OR 710__.5:MT OR 710__.5:EST OR 710__.5:SB
                   """

    __ignore_keys__ = {
        "0248_a",
        "0248_p",
        "0248_q",
        "100__m",  # email of contributor
        "110__c", # Location of corporate author
        "110__g", # Type of corporate author
        "340__a",  # Physical medium
        "037__c",  # arxiv subject
        "300__a",  # number of pages
        "700__m",  # email of contributor
        "520__9",
        "852__a",  # Physical Location
        "852__c",  # holdings will be taken separately
        "852__h",
        "8564_8",  # file id
        "8564_s",  # bibdoc id
        "8564_x",  # icon thumbnails sizes
        "8564_y",  # file description - done by files dump
        "913__t",  # Citation field
        "913__y",  # Citation field
        "913__v",  # Citation field
        "913__c",  # Citation field
        "916__y",  # year, redundant value
        "937__c",  # last modified by
        "937__s",  # last modification date
        "960__a",  # base number
        "961__c",  # CDS modification tag # TODO
        "961__h",  # CDS modification tag # TODO
        "961__l",  # CDS modification tag # TODO
        "961__x",  # CDS modification tag # TODO
        "981__a",  # duplicate record id
        "964__a",  # number of physical copies
    }

    _default_fields = {"custom_fields": {},}


technical_support_model = TechnicalSupportModel(
    bases=(rdm_base_publication_model,),
    entry_point_group="cds_migrator_kit.migrator.rules.technical_support",
)
