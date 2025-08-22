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
from cds_migrator_kit.rdm.records.transform.models.base_record import (
    rdm_base_record_model,
)
from cds_migrator_kit.rdm.records.transform.models.thesis import thesis_model
from cds_migrator_kit.transform.overdo import CdsOverdo


class YellowRepModel(CdsOverdo):
    """Translation model for MoUs."""

    __query__ = '690C_:YELLOWREPORT OR 690C_:"CERN Yellow Report" OR 690C_:"YELLOW REPORT" OR 690C_:Yellow Report OR 690C_:YELLOWREPCONTRIB -980__:THESIS'

    __ignore_keys__ = {
        "0248_a",
        "0248_p",
        "0248_q",
        "037__c",  # arxiv subject
        "852__c",  # todo holdings
        "852__h",
        "300__a",
        "340__a",  # ignore, spreadsheet
        "8564_8",  # Files system field
        "8564_s",  # Files system field
        "8564_x",  # Files system field
        "8564_y",  # Files / URLS label
        "916__y",  # year
        "960__a",  # collection id? usually value 14, to confirm if we ignore
        "961__c",  # CDS modification tag
        "961__h",  # CDS modification tag
        "961__l",  # CDS modification tag
        "961__x",  # CDS modification tag
        "964__a",  # TODO
        "981__a",  # duplicated record marker
        # "110__a",
        # "246_1a",
        # "690C_a",
        # "520__b",
        # "773__y",
        # "773__n",
        # "773__p",
        # "773__c",
        # "0248_q",
        # "8564_8",
        # "8564_s",
        # "8564_x",
        # "980__a",
    }

    _default_fields = {
        "resource_type": {"id": "publication-report"},
        "custom_fields": {},
        # "creators": [{"person_or_org":  {"type": "organizational", "name": "CERN"}}]
    }


yellow_issue_model = YellowRepModel(
    bases=(rdm_base_publication_model,),
    entry_point_group="cds_migrator_kit.migrator.rules.yellow_report",
)
