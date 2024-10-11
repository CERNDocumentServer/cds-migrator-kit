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

"""CDS-RDM Summer student model."""

from .base import model as base_model
from .overdo import CdsOverdo


class CMSSummerStudent(CdsOverdo):
    """Translation Index for CDS Books."""

    # putting values like 980__ INTNOTEPHPUBL INTNOTEEPPUBL is an overkill since it maps
    # different departments
    # __query__ = "980__:INTNOTEEPPUBLL 980__:NOTE 037__:CERN-STUDENTS-Note-\"/(.*?)/\""
    __query__ = "980__:NOTE 037__:CERN-STUDENTS-Note-*"

    __ignore_keys__ = {

        # decided to ignore
        "0247_2",  # DOI, summer student notes do not have it
        "0247_a",  # DOI
        "0248_a",  # oai identifier, not needed to migrate, TBD
        "0248_p",  # oai identifier, not needed to migrate, TBD
        # "0248_q",  does appear in data, what is this field
        "100__m",  # author's email <-- decided not to keep in RDM,
        "260__c",  # Redundant (more detailed value is in 269__c imprint.pub_date)
        "270__m",  # document contact email
        "595__a",  # always value CERN EDS, not displayed, TODO: do we keep?
        "595__z",  # SOME RECORD HAVE UNCL as value, do we keep it? what does UNCL mean

        "710__5",  # department / organisation author
        "710__a",  # organisation author
        "700__m",  # Contributors (email)
        "700__m",  # author's email <-- decided not to keep in RDM,
        "8564_8",  # Files system field
        "8564_s",  # Files system field
        "8564_u",  # Files
        "8564_x",  # Files system field
        "8564_y",  # Files
        "937__c",  # modification date
        "937__s",  # modification person
        "960__a",  # collection id? usually value 12, to confirm if we ignore
        "980__a",  # collection tag
        # "980__c",  # MIGRATED/DELETED - it shouldn't even make it here


        # TO Implement (to remove from here)

        "690C_a",    # collection name, not needed values(to drop: INTNOTE, CERN, otherwise parse PUBL to retrieve the department, if department not present in any other field)
        # "970__a",  # alternative identifier, scheme ALEPH
        # "037__a",  # decided to ignore (Report number) alternative identifiers -> scheme "CDS REFERENCE"
        # "088__a",  # RN (manual introduced?) second report number (so the identifiers schemas are not unique!)
        # "035__9",  # Inspire schema
        # "035__a",  # Inspire id value
        # "100__9",  # #BEARD# tag
        # "100__9",  # Author, to check for BEARD
        # "700__9",  # #BEARD# tag
        # "270__p",  # document contact person name
        # "562__c",  # note
        "693__a",  # accelerator, do we create a custom field?
        # "693__b",  # beams
        # "693__f",  # facility, do we create a custom field?
        # "693__p",  # project, do we create a custom field?
        # "693__s",  # study,  do we create a custom field?
        # "700__0",  # Contributors (cds author id) - TBD if we keep, same with INSPIRE ID
        # "710__g",  # Collaboration, OK to migrate as corporate contributor (not creator)?
        "906__p",  # names, is it supervisor? # todo migrate as contributor?
        "963__a",
        # restriction # todo assert if any record is restricted -> to implement in collection specific rules

        # TO DECIDE


        # IMPLEMENTED

        # "001"
        # "003"
        # "041__a",  # languages
        # "100__a",
        # "100__u",  # Author affiliation
        # "246__a",
        # "246__i",  # abbreviation
        # "246__i",  # abbreviation tag, applies to value of 246__A
        # "500__a",  # Note (-> description.type = other)
        # "520__a",  # Note (-> description.type = abstract
        # "650172",  # subject provenance
        # "65017a",  # subject value
        # "6531_9",  # keyword provenance
        # "6531_a",  # keyword
        # "6931_9",  # keyword
        # "6931_a",  # keyword
        # "693__e",  # custom_fields.cern:experiment  # TODO this field is single value, do we have lists?
        # "700__a",  # Contributors (full name)
        # "700__u",  # Contributors (affiliation)
        # "859__f",  # creator's email, to be used to determine the owner
        # "916__n",
        # "916__s",
        # "916__w",

    }
    _default_fields = None


model = CMSSummerStudent(
    bases=(base_model,), entry_point_group="cds_migrator_kit.migrator.rules.ssn"
)
