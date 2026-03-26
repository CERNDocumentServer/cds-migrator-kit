# -*- coding: utf-8 -*-
#
# This file is part of CERN Document Server.
# Copyright (C) 2026 CERN.
#
# Invenio is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the
# License, or (at your option) any later version.


"""CDS-Videos Digitized Video Lecture model."""

from cds_migrator_kit.transform.overdo import CdsOverdo

from .video_lecture import model as video_lecture_model


class DigitizedVideoLecture(CdsOverdo):
    """Translation Index for CERN Digitized Video Lectures."""

    __query__ = "8567_.x:'Absolute master path' 8567_.d:/mnt/master_share* -980__.C:MIGRATED -980__.c:DELETED 5831_.a:digitized"

    __ignore_keys__ = {
        "0248_a",  # oai identifier
        "0248_p",  # oai identifier
        "0248_q",  # oai
        "100__0",  # Author id (eg: AUTHOR|(CDS)2067852)
        "111__z",  # End date (indico)
        "250__a",  # Edition
        "337__a",  # Checked values only `Video`
        "511__0",  # Author id (eg: AUTHOR|(CDS)2067852)
        "5111_8",  # Video id for performer, multi video records.
        "518__l",  # Lectures: length of speech
        "518__h",  # Lectures: Starting time
        "700__0",  # Author id (eg: AUTHOR|(CDS)2067852)
        "518__e",  # Speaker (1 record has contributor in 518, it's also in 5111)
        "8564_8",  # File: bibdoc id
        "8564_s",  # File: file size
        "916__y",  # Status week year
        "960__a",  # Base number
        # CDS modification tag
        "961__h",
        "961__l",
    }

    _default_fields = {
        "language": "",
        "description": "",
        "performer": "",
        "url_files": [],
        "curated_copyright": {},
        "lecture_infos": [],
        "_curation": {
            "preservation_values": [],
        },
        "contributors": [],
        "alternate_identifiers": [],
        "additional_languages": [],
        "collections": [],
        "keywords": [],
    }


model = DigitizedVideoLecture(
    bases=(video_lecture_model,),
    entry_point_group="cds_migrator_kit.videos.rules.digitized_video_lecture",
)
