# -*- coding: utf-8 -*-
#
# This file is part of CERN Document Server.
# Copyright (C) 2025 CERN.
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

from ..models.base_record import rdm_base_record_model as base_model


class ThesisModel(CdsOverdo):
    """Translation Index for CERN Summer Student Project Notes."""

    __query__ = (
        "(980__:THESIS OR 980__:Thesis OR 980__:thesis) -980__:DUMMY -980__.c:HIDDEN"
    )

    __ignore_keys__ = {
        # decided to ignore
        "0248_a",  # oai identifier, not needed to migrate, TBD
        "0248_p",  # oai identifier, not needed to migrate, TBD
        "0248_q",  # full text tag 2778897
        "100__m",  # author's email <-- decided not to keep in RDM,
        "260__c",  # Redundant (more detailed value is in 269__c imprint.pub_date)
        "270__m",  # document contact email
        "595__a",  # always value CERN EDS, not displayed, TODO: do we keep?
        "700__m",  # author's email <-- decided not to keep in RDM,
        "8564_8",  # Files system field
        "8564_s",  # Files system field
        "8564_x",  # Files system field
        "8564_y",  # Files
        "960__a",  # collection id? usually value 14, to confirm if we ignore
        "980__a",  # collection tag
        # TO IMPLEMENT /decide
        # "035__9",  # Inspire schema
        # "035__a",  # Inspire id value, contains unknown identifiers, TBD what to do
        # "269__a",  # imprint place
        "300__a",  # number of pages (imprint?)
        # "340__a",  # resource type ?
        # "490__a",  # tag for experiment + thesis
        "502__a",  # seems to tag the type of thesis
        "502__b",  # seems to tag the university
        "502__c",  # seems to tag the defense date
        # "595__z",  # SOME RECORD HAVE UNCL as value, do we keep it? what does UNCL mean
        # "690C_a",  # collection name, values to be explored
        # "701__",   # contributors (implemented, to confirm this is the right field)
        # "710__5",  # department / organisation author
        # "710__a",  # organisation author
        "8564_u",  # exclude files but include links (filter by domain)
        "916__y",  # year
        # "937__c",  # modification date
        # "937__s",  # modification person
        # "961__c",  # seems like a set of identifiers, to be defined
        # "961__h",  # seems like a set of identifiers, to be defined
        # "961__l",  # seems like a set of identifiers, to be defined
        # "961__x",  # seems like a set of identifiers, to be defined
        # "981__a",  # duplicate record id
        # "999C6",   # to define what is the field
        # IMPLEMENTED
        # "001",
        # "003",
        # "037__a",  # (Report number) alternative identifiers -> scheme "CDS REFERENCE"
        # "041__a",  # languages
        # "088__a",  # RN (manual introduced?) second report number (so the identifiers schemas are not unique!)
        # "100__9",  # #BEARD# tag, checking if there is no other unexpected value, but discarding #BEARD# values
        # "100__a",  # author's name
        # "100__u",  # Author affiliation
        # "246__a",  # alt title
        # "246__i",  # abbreviation
        # "246__i",  # abbreviation tag, applies to value of 246__A
        # "270__p",  # document contact person name
        # "500__a",  # Note (-> description.type = other)
        # "520__a",  # Note (-> description.type = abstract
        # "562__c",  # note
        # "650172",  # subject provenance
        # "65017a",  # subject value
        # "6531_9",  # keyword provenance
        # "6531_a",  # keyword value
        # "693__a",  # accelerator, custom field
        # "693__b",  # beams recid: 2640381
        # "693__e",  # custom_fields.cern:experiments
        # "693__f",  # facility, custom field
        # "693__p",  # project, custom field
        # "693__s",  # study,  custom field
        # "700__0",  # Contributors (cds author id, inspire author id)
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
        "resource_type": {"id": "publication-thesis"},
        "custom_fields": {},
    }


thesis_model = ThesisModel(
    bases=(base_model,), entry_point_group="cds_migrator_kit.migrator.rules.thesis"
)
