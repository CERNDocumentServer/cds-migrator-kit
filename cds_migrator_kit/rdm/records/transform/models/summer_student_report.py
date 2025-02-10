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

from cds_migrator_kit.transform.overdo import CdsOverdo

from .base_record import rdm_base_record_model


class SummerStudentNotes(CdsOverdo):
    """Translation Index for CERN Summer Student Project Notes."""

    # __query__ = "980__:INTNOTEEPPUBLL 980__:NOTE 037__:CERN-STUDENTS-Note-\"/(.*?)/\""
    __query__ = "037__:CERN-STUDENTS-Note-* -980__c:DELETED"

    __ignore_keys__ = {
        # decided to ignore
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
        "300__a",  # number of pages - ignored for SSPN - 1 record (2913067) was found
        # IMPLEMENTED
        # "001",
        # "003",
        # "035__9",  # Inspire schema
        # "035__a",  # Inspire id value
        # "037__a",  # (Report number) alternative identifiers -> scheme "CDS REFERENCE"
        # "041__a",  # languages
        # "088__a",  # RN (manual introduced?) second report number (so the identifiers schemas are not unique!)
        # "100__9",  # #BEARD# tag
        # "100__a",
        # "100__u",  # Author affiliation
        # "246__a",
        # "246__i",  # abbreviation
        # "246__i",  # abbreviation tag, applies to value of 246__A
        # "270__p",  # document contact person name
        # "500__a",  # Note (-> description.type = other)
        # "520__a",  # Note (-> description.type = abstract
        # "562__c",  # note
        # "650172",  # subject provenance
        # "65017a",  # subject value
        # "6531_9",  # keyword provenance
        # "6531_a",  # keyword
        # "690C_a",  # collection name, not needed values(to drop: INTNOTE, CERN, otherwise parse PUBL to retrieve the department, if department not present in any other field)
        # "6931_9",  # keyword
        # "6931_a",  # keyword
        # "693__a",  # accelerator, do we create a custom field?
        # "693__b",  # beams recid: 2640381
        # "693__e",  # custom_fields.cern:experiments
        # "693__f",  # facility, do we create a custom field?
        # "693__p",  # project, do we create a custom field?
        # "693__s",  # study,  do we create a custom field?
        # "700__0",  # Contributors (cds author id) - TBD if we keep, same with INSPIRE ID
        # "700__9",  # #BEARD# tag
        # "700__a",  # Contributors (full name)
        # "700__u",  # Contributors (affiliation)
        # "710__g",  # Collaboration, OK to migrate as corporate contributor (not creator)?
        # "859__f",  # creator's email, to be used to determine the owner
        # "906__p",  # names, is it supervisor?
        # "916__n",
        # "916__s",
        # "916__w",
        # "963__a",
        # "970__a",  # alternative identifier, scheme ALEPH
    }
    _default_fields = {
        "resource_type": {"id": "publication-technicalnote"},
        "custom_fields": {},
    }


sspn_model = SummerStudentNotes(
    bases=(rdm_base_record_model,),
    entry_point_group="cds_migrator_kit.migrator.rules.ssn",
)
