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


class AnnualReportModel(CdsOverdo):
    """Translation model for MoUs."""

    __query__ = (
        "980__.a:CERN_Annual_Report_Issues OR 980__.a:CERN_Annual_Report_Contributions"
    )

    __ignore_keys__ = {
        "000tjt",  # seems like a hidden field, not expressed in MARC 1952785
        "0248_a",
        "0248_p",
        "0248_q",
        "100__m",  # email of contributor
        "111__g",  # used for tagging year+lang
        "145__a",  # duplication of title
        "145__b",  # duplication of subtitle
        "300__a",  # number of pages
        "310__a",  # one value, "annual"
        "340__a",  # only value "Online"
        "700__m",  # email of contributor
        "65017a",  # all values are incorrect - particle physics
        "650172",  # all values are incorrect - particle physics
        "8564_8",  # file id
        "8564_s",  # bibdoc id
        "8564_x",  # icon thumbnails sizes
        "8564_y",  # file description - done by files dump
        "932__a",  # corporate author
        "937__c",  # last modified by
        "937__s",  # last modification date
        "960__a",  # base number
        "961__c",  # CDS modification tag
        "961__h",  # CDS modification tag
        "961__l",  # CDS modification tag
        "961__x",  # CDS modification tag
        "970__d",  # duplicate record id
        "981__a",  # duplicate record id
    }

    _default_fields = {
        "resource_type": {"id": "publication-report"},
        "custom_fields": {},
        "creators": [
            {
                "person_or_org": {
                    "type": "organizational",
                    "name": "European Organization for Nuclear Research",
                }
            }
        ],
    }


annual_rep_model = AnnualReportModel(
    bases=(rdm_base_publication_model,),
    entry_point_group="cds_migrator_kit.migrator.rules.annual_rep",
)
