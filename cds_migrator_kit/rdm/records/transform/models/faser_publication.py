# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM FASER publication model."""

from cds_migrator_kit.rdm.records.transform.models.base_publication_record import (
    rdm_base_publication_model,
)
from cds_migrator_kit.transform.overdo import CdsOverdo


class FaserPublicationModel(CdsOverdo):
    """Translation model for FASER publication records."""

    __query__ = "693__:FASER -980__:DELETED -980__.c:MIGRATED -980__.a:DUMMY"

    __ignore_keys__ = {
        "0247_9",  # Checked with library, ignore. 20 records has 2 and 9 subfields.
        "0248_a",
        "0248_p",
        "0248_q",
        "035__d",  # oai harvest tag
        "035__h",  # oai harvest tag
        "035__m",  # oai harvest tag
        "035__t",  # oai harvest tag
        "035__u",  # oai harvest tag
        "037__c",  # arXiv subject
        "100__m",  # email of contributor
        "100__v",  # explanation of the affiliation? TODO: can we ignore? 2816452, 2924565
        "245__9",  # title source
        "270__m",  # contact person email
        "300__a",  # number of pages
        "500__9",
        "520__9",
        "540__3",
        "595_Ds",  # Checked with library, ignore
        "595_Dd",  # Checked with library, ignore
        "595_Da",  # Checked with library, ignore
        "542__3",
        "700__m",  # email of contributor
        "700__v",  # explanation of the affiliation? TODO: can we ignore? 2816452, 2924565
        "773__0",  # Checked with library, ignore
        "773__x",  # Checked with library, ignore
        "8564_8",
        "8564_s",
        "8564_x",
        "8564_y",  # file description - done by files dump
        "8564_z",  # automatic process with EP value:Stamped by WebSubmit
        "903__s",  # public: 2651328, 2642351, 2702868
        "916__y",  # year of publication, redundant
        "937__c",  # last modified by
        "937__s",  # last modification date
        "960__a",  # base number
        "961__c",  # CDS modification tag # TODO
        "961__h",  # CDS modification tag # TODO
        "961__l",  # CDS modification tag # TODO
        "961__x",  # CDS modification tag # TODO
        "981__a",  # duplicate record id
    }

    _default_fields = {
        "custom_fields": {},
    }


faser_publication_model = FaserPublicationModel(
    bases=(rdm_base_publication_model,),
    entry_point_group="cds_migrator_kit.migrator.rules.faser_publication",
)
