# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM Yellow report model."""
from cds_migrator_kit.rdm.records.transform.models.base_publication_record import (
    rdm_base_publication_model,
)
from cds_migrator_kit.transform.overdo import CdsOverdo


class BookModel(CdsOverdo):
    """Translation model for MoUs."""

    __query__ = '980__:BOOK OR 980__:BOOk 690C_:BOOK -690C:BOOKSUGGESTION -980__c:MIGRATED -690C_:"YELLOW REPORT" -690C_:"Yellow Report"'

    __ignore_keys__ = {
        "0248_a",
        "0248_p",
        "0248_q",
        "035__d",  # oai harvest tag
        "035__h",  # oai harvest tag # todo confirm
        "035__m",  # oai harvest tag
        "035__t",  # oai harvest tag
        "035__u",  # oai harvest tag
        "035__z",  # oai harvest tag
        "852__c",  # holdings will be taken separately
        "852__h",
        "037__c",  # arxiv subject
        "100__m",  # email of contributor
        "300__a",  # number of pages
        "700__m",  # email of contributor
        "7870_r",  # relation free text label
        "8564_8",  # file id
        "8564_s",  # bibdoc id
        "8564_x",  # icon thumbnails sizes
        "8564_y",  # file description - done by files dump
        "916__y",  # year, redundant value
        "937__c",  # last modified by
        "937__s",  # last modification date
        "960__a",  # base number
        "961__c",  # CDS modification tag # TODO
        "961__h",  # CDS modification tag # TODO
        "961__l",  # CDS modification tag # TODO
        "961__x",  # CDS modification tag # TODO
        "964__a",  # number of physical copies
        "981__a",  # duplicate record id
        # To be verified
        "020__u",
        "020__b",
        "340__a",
    }

    _default_fields = {
        "resource_type": {"id": "publication-book"},
        "custom_fields": {},
        # "creators": [{"person_or_org":  {"type": "organizational", "name": "CERN"}}]
    }


book_model = BookModel(
    bases=(rdm_base_publication_model,),
    entry_point_group="cds_migrator_kit.migrator.rules.books",
)
