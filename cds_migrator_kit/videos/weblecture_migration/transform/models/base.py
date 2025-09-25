# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos base migration model."""

from cds_migrator_kit.transform.overdo import CdsOverdo


class CDSVideosBase(CdsOverdo):
    """Translation Index for CDS Weblectures."""

    __ignore_keys__ = {
        "003",
        # check with JY
        "961__h",  # Hour? TODO? check with JY
        "961__l",  # Library? TODO? check with JY
        "961__a",  # ? TODO? check with JY
        "961__b",  # ? TODO? check with JY
        # IGNORE
        "111__z",  # End date (indico)
        "518__h",  # Lectures: Starting time
        "518__e",  # Speaker (2 record has contributor in 518, they're also added in 511)
        "300__2",  # Imprint
        "300__b",  # Imprint
        "300__8",  # Imprint
        "300__a",  # Number of pages / duration
        "250__a",  # Edition
        "700__0",  # Author id (eg: AUTHOR|(CDS)2067852)
        "518__l",  # Lectures: length of speech
        "100__0",  # Author id (eg: AUTHOR|(CDS)2067852)
        "240__a",  # Decided to drop, (Streaming Video)
        "337__a",  # Decided to drop, (Video)
        "963__a",  # values: PUBLIC/RESTRICTED
        "8564_8",  # File: bibdoc id
        "8564_s",  # File: file size
        "916__d",  # period for books only one record
        "916__y",  # Status week year
        "916__e",  # spreadsheet no record
        "916__a",  # Status week only one record value:1 https://cds.cern.ch/record/423917
        "080__a",  # Subject code, only 4 record: 1206221, 225152, 225151, 254588
        "084__a",  # Other classification number
        "084__2",  # Other classification number
        "960__a",  # Base number
        "0248_a",  # oai identifier
        "0248_p",  # oai identifier
        "0248_q",  # oai
        "981__a",  # duplicate record id, checked with Jens
    }

model = CDSVideosBase(bases=(), entry_point_group="cds_migrator_kit.videos.rules.base")
