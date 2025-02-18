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

"""CDS-RDM model for extracting affiliations from legacy records."""

from cds_migrator_kit.transform.overdo import CdsOverdo


class CDSAffiliations(CdsOverdo):
    """Translation Index for CDS Books."""

    def __init__(self, bases=None, entry_point_group=None):
        """Constructor."""
        super().__init__(bases, entry_point_group)

    __query__ = ""

    __ignore_keys__ = {
        # IGNORED
        "001",
        "003",
        "0247_2",  # DOI, summer student notes do not have it
        "0247_a",  # DOI
        "0248_a",  # oai identifier, not needed to migrate, TBD
        "0248_p",  # oai identifier, not needed to migrate, TBD
        "0248_q",  # does appear in data, what is this field recid 2778897
        "035__9",  # Inspire schema
        "035__a",  # Inspire id value
        "037__9",
        "037__a",  # (Report number) alternative identifiers -> scheme "CDS REFERENCE"
        "041__a",  # languages
        "088__a",  # RN (manual introduced?) second report number (so the identifiers schemas are not unique!)
        "100__0",
        "100__9",  # #BEARD# tag
        "100__m",  # author's email <-- decided not to keep in RDM,
        "245__a",
        "246__a",
        "246__i",  # abbreviation
        "246__i",  # abbreviation tag, applies to value of 246__A
        "260__c",  # Redundant (more detailed value is in 269__c imprint.pub_date)
        "269__a",
        "269__b",
        "269__c",
        "270__m",  # document contact email
        "270__p",  # document contact person name
        "300__a",
        "500__a",  # Note (-> description.type = other)
        "520__a",  # Note (-> description.type = abstract
        "562__c",  # note
        "562__c",  # note
        "595__a",  # always value CERN EDS, not displayed, TODO: do we keep?
        "595__z",  # SOME RECORD HAVE UNCL as value, do we keep it? what does UNCL mean
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
        "700__0",  # Contributors (cds author id) - TBD if we keep, same with INSPIRE ID
        "700__9",  # #BEARD# tag
        "700__m",  # Contributors (email)
        "700__m",  # author's email <-- decided not to keep in RDM,
        "710__5",  # department / organisation author
        "710__a",  # organisation author
        "710__g",
        "8564_8",  # Files system field
        "8564_s",  # Files system field
        "8564_u",  # Files
        "8564_x",  # Files system field
        "8564_y",  # Files
        "859__f",  # creator's email, to be used to determine the owner
        "906__p",  # names, is it supervisor?
        "916__n",
        "916__s",
        "916__w",
        "937__c",  # modification date
        "937__s",  # modification person
        "960__a",  # collection id? usually value 12, to confirm if we ignore
        "963__a",
        "970__a",  # alternative identifier, scheme ALEPH
        "980__a",  # collection tag
        "980__c",  # MIGRATED/DELETED - it shouldn't even make it here
        "981__a",
        # IMPLEMENTED
        # "100__a",
        # "100__u",  # Author affiliation
        # "700__a",  # Contributors (full name)
        # "700__u",  # Contributors (affiliation)
        # "710__g",  # Collaboration, OK to migrate as corporate contributor (not creator)?
    }
    _default_fields = None


affiliation_model = CDSAffiliations(
    entry_point_group="cds_migrator_kit.migrator.rules.affiliations",
)
