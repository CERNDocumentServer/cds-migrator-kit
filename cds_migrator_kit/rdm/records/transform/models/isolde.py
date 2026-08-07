# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM ISOLDE model."""

from cds_migrator_kit.rdm.records.transform.models.base_publication_record import (
    rdm_base_publication_model,
)
from cds_migrator_kit.transform.overdo import CdsOverdo


class ISOLDEModel(CdsOverdo):
    """Translation model for ISOLDE."""

    __query__ = '693__.a:"CERN ISOLDE" AND (980__:ARTICLE OR 980__:PREPRINT OR 980__:conferencepaper OR 980__:NOTE OR 980__:REPORT) -980__:DELETED -980__:DUMMY'

    __ignore_keys__ = {
        "0247_9",  # provenance of the DOI
        "0248_a",
        "0248_p",
        "0248_q",
        "035__d",  # oai harvest tag
        "035__h",  # oai harvest tag
        "035__m",  # oai harvest tag
        "035__t",  # oai harvest tag
        "035__u",  # oai harvest tag
        "035__z",  # oai harvest tag
        "030__a",  # CODEN journal code (e.g. "Phys. Lett., B") - obsolete identifier system, journal title already captured in 773__p
        "037__c",  # arxiv subject
        "100__m",  # email of contributor
        "245__9",  # title source
        "270__m",  # contact person email
        "300__a",  # number of pages
        "336__a",  # redundant field
        "500__9",  # provenance of the note
        "520__9",  # provenance of the description
        "520__h",  # provenance of the description
        "540__3",  # material of license
        "540__9",  # material of license
        "542__3",  # material of copyrights
        "595__i",
        "695__e",  # inspire tag
        "700__m",  # email of contributor
        "700__q",  # aliteration of the name, used for searching
        "700__v",
        "773__0",  # from SIS: can be ignored
        "773__o",  # from SIS: can be ignored
        "773__t",  # INSPIRE publication note
        "773__x",  # INSPIRE publication note
        "773__z",  # from SIS: can be ignored
        "8564_8",  # file id
        "8564_s",  # bibdoc id
        "8564_w",  # system field
        "8564_x",  # icon thumbnails sizes
        "8564_y",  # file description - done by files dump
        "8564_z",  # file comment, migrated via file metadata
        "913__a",  # citation
        "913__c",  # citation
        "913__t",  # citation
        "913__v",  # citation
        "913__y",  # citation
        "916__y",  # year, redundant value
        "937__c",  # last modified by
        "937__s",  # last modification date
        "960__a",  # base number
        "961__c",
        "961__h",
        "961__l",
        "961__x",
        "964__a",
        "980__b",  # additional article tag
        "981__a",  # duplicate record id
        "999C50",
        "999C52",
        "999C59",
        "999C5a",
        "999C5c",
        "999C5h",
        "999C5i",
        "999C5k",
        "999C5l",
        "999C5m",
        "999C5o",
        "999C5p",
        "999C5r",
        "999C5s",
        "999C5t",
        "999C5u",
        "999C5v",
        "999C5x",
        "999C5y",
        "999C5z",
        "999C6a",
        "999C6t",
        "999C6v",
    }

    _default_fields = {
        "custom_fields": {},
    }


isolde_model = ISOLDEModel(
    bases=(rdm_base_publication_model,),
    entry_point_group="cds_migrator_kit.migrator.rules.isolde",
)
