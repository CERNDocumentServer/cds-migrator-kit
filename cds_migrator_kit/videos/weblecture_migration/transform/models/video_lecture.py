# -*- coding: utf-8 -*-
#
# This file is part of CERN Document Server.
# Copyright (C) 2024 CERN.
#
# Invenio is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the
# License, or (at your option) any later version.
#
# Invenio is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Invenio; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.

"""CDS-Videos Video Lecture model."""

from cds_migrator_kit.rdm.migration.transform.models.overdo import CdsOverdo

from .base import model as base_model


class VideoLecture(CdsOverdo):
    """Translation Index for CERN Video Lectures."""

    __query__ = '8567_.x:"Absolute master path" 8567_.d:/mnt/master_share* -980__.C:MIGRATED -980__.c:DELETED -5831_.a:digitized'

    __ignore_keys__ = {
        "110__a",  # corporate author
        "8567_y",  # File description
        "111__c",  # Video location (indico location)
        "518__r",  # Video/meeting location
        "340__a",  # Physical medium -> curation field
        "340__d",  # Physical medium/recording technique -> curation field
        "961__x",  # Creation Date TODO? check with JY
        "961__c",  # modification Date TODO? check with JY
        "961__h",  # Hour? TODO? check with JY
        "961__l",  # Library? TODO? check with JY
        "964__a",  # Item owner TODO? check with JY
        "916__d",  # Status week? TODO? check with JY
        "901__u",  # Affiliation at Conversion? TODO? check with JY
        "583__a",  # Action note / curation TODO? check with JY
        "583__c",  # Action note / curation TODO? check with JY
        "583__z",  # Action note / curation TODO? check with JY
        "65027a",  # TODO Subject category = Event?
        "111__a",  # Title (indico)
        "111__9",  # Start date (indico)
        "111__g",  # Event id (indico)
        "111__z",  # End date (indico)
        "084__a",  # Indico?
        "084__2",  # Indico?
        "8567_2",  # File system? 'MediaArchive'
        "980__b",  # Secondary collection indicator
        "542__d",  # Copyright holder
        "542__g",  # Copyright date
        "490__a",  # TODO Series
        "8567_u",  # File url
        "962__n",  # `Presented at` note (conference/linked document)
        "962__b",  # `Presented at` record (conference/linked document)
        "518__g",  # Lectures: conference identification
        "490__v",  # Series: volume
        "269__b",  # Name of publ.
        "088__9",  # Report number (make it alternative identifier with cds reference?)
        "088__z",  # Report number (make it alternative identifier with cds reference?)
        # Files
        "8564_q",  # File type (digitized) # TODO this record has both lecturemedia and DM https://cds.cern.ch/record/589875
        "852__c",  # Location (Shelving/Library)
        "852__h",  # Location (Shelving) example: https://cds.cern.ch/record/254588/
        "852__a",  # Location (Shelving) example: https://cds.cern.ch/record/558348
        "852__x",  # Location (Shelving/ type? DVD) example: https://cds.cern.ch/record/690000/
        # IGNORE
        "518__h",  # Lectures: Starting time
        "300__2",  # Imprint
        "300__b",  # Imprint
        "300__a",  # Number of pages / duration
        "250__a",  # Edition
        "700__0",  # Author id (eg: AUTHOR|(CDS)2067852)
        "518__l",  # Lectures: length of speech
        # TODO copied from ssn
        "0248_a",  # oai identifier, not needed to migrate, TBD
        "0248_p",  # oai identifier, not needed to migrate, TBD
        "0248_q",  # full text tag 2778897
        "100__m",  # author's email <-- decided not to keep in RDM,
        "260__c",  # Redundant (more detailed value is in 269__c imprint.pub_date)
        "269__a",  # imprint place
        "270__m",  # document contact email
        "595__a",  # always value CERN EDS, not displayed, TODO: do we keep?
        "595__z",  # SOME RECORD HAVE UNCL as value, do we keep it? what does UNCL mean
        "700__m",  # author's email <-- decided not to keep in RDM,
        "710__5",  # department / organisation author
        "710__a",  # organisation author
        "8564_8",  # Files system field
        "8564_s",  # Files system field
        "8564_u",  # Files
        "8564_x",  # Files system field
        "8564_y",  # Files
        "937__c",  # modification date
        "937__s",  # modification person
        "960__a",  # collection id? usually value 12, to confirm if we ignore
        "980__a",  # collection tag
        "981__a",  # duplicate record id
        "003",
        "035__9",  # Inspire schema
        "035__a",  # Inspire id value
        "037__a",  # (Report number) alternative identifiers -> scheme "CDS REFERENCE"
        "088__a",  # RN (manual introduced?) second report number (so the identifiers schemas are not unique!)
        "246__a",
        "246__i",  # abbreviation
        "246__i",  # abbreviation tag, applies to value of 246__A
        "270__p",  # document contact person name
        "500__a",  # Note (-> description.type = other)
        "562__c",  # note
        "650172",  # subject provenance
        "65017a",  # subject value
        "6531_9",  # keyword provenance
        "6531_a",  # keyword
        "690C_a",  # collection name, not needed values(to drop: INTNOTE, CERN, otherwise parse PUBL to retrieve the department, if department not present in any other field)
        "6931_9",  # keyword
        "6931_a",  # keyword
        "693__a",  # accelerator, do we create a custom field?
        "693__b",  # beams recid: 2640381
        "693__e",  # custom_fields.cern:experiments
        "693__f",  # facility, do we create a custom field?
        "693__p",  # project, do we create a custom field?
        "693__s",  # study,  do we create a custom field?
        "710__g",  # Collaboration, OK to migrate as corporate contributor (not creator)?
        "859__f",  # creator's email, to be used to determine the owner
        "916__n",
        "916__s",
        "916__w",
        "963__a",
        "970__a",  # alternative identifier, scheme ALEPH
        # IMPLEMENTED
        # "520__a",  # Note (-> description.type = abstract
        # "001",
        # "041__a",  # languages
        # "906__p",  # names, is it supervisor?
        # "100__9",  # #BEARD# tag
        # "100__a",
        # "100__u",  # Author affiliation
        # "700__e", # Contributor/Speaker role
        # "700__0",  # Contributors (cds author id) - TBD if we keep, same with INSPIRE ID
        # "700__9",  # #BEARD# tag
        # "700__a",  # Contributors (full name)
        # "700__u",  # Contributors (affiliation)
        # "518__d", # Full date/time
        # "269__c", # Date (full date/year)
        # "518__a", # date?
    }


model = VideoLecture(
    bases=(base_model,),
    entry_point_group="cds_migrator_kit.videos.rules.video_lecture",
)
