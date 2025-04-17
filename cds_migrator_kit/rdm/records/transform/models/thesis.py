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
        # spreadsheet https://docs.google.com/spreadsheets/d/1S5x0pP1WnSIWPDZ_7WyFx87aHZ_Ni6y8v1nSC2TBNfg/edit?usp=sharing (ignored tab)
        # spreadsheet to be moved to cernbox when ready
        "020__b",  # material of ISBN, to ignore in a repository
        "020__u",  # material of ISBN, to ignore in a repository
        "0247_9",  # source of pid, INSPIRE data model
        "0248_a",  # oai identifier, not needed to migrate, TBD
        "0248_p",  # oai identifier, not needed to migrate, TBD
        "0248_q",  # full text tag 2778897
        "035__d",  # oai harvest tag
        "035__h",  # oai harvest tag # todo confirm
        "035__m",  # oai harvest tag
        "035__t",  # oai harvest tag
        "035__u",  # oai harvest tag
        "035__z",  # oai harvest tag
        "037__c",  # arxiv subject
        # "084__2",  # SORT ID, spreadsheet, also PACS and missing report numbers, requested curation
        # "084__a",  # SORT ID, spreadsheet
        "088__9",  # source of report number
        "100__j",  # alternative spelling of the name
        "100__m",  # author's email <-- decided not to keep in RDM,
        "100__q",  # alternative spelling of the name
        "242__9",  # source of TRANSLATED title, INSPIRE data model
        "245__9",  # source of title, INSPIRE data model
        "269__c",  # Redundant (more detailed value is in 260__c imprint.pub_date)
        "270__m",  # document contact email
        "340__a",  # ignore, spreadsheet
        "500__9",  # provenance of note # TODO
        "520__9",  # abstract provenance
        "540__b",  # material of license
        "540__3",  # all the copyrights seem not to need this qualifier
        "542__3",  # ignored, all the values were equal thesis
        "595__a",  # always value CERN EDS, not displayed
        "650272",  # scheme of subjects, other than CERN
        "6532_9",  # scheme of subjects, other than CERN
        "700__m",  # author's email <-- decided not to keep in RDM,
        "701__j",
        "701__m",  # supervisors's email <-- decided not to keep in RDM,
        "720__a",  # author's duplicate
        "773__o",  # spreadsheet
        "773__x",  # spreadsheet
        "8564_8",  # Files system field
        "8564_s",  # Files system field
        "8564_x",  # Files system field
        "8564_y",  # Files / URLS label
        "916__d",  # spreadsheet
        "916__e",  # spreadsheet
        "960__a",  # collection id? usually value 14, to confirm if we ignore
        "961__c",  # CDS modification tag # TODO
        "961__h",  # CDS modification tag # TODO
        "961__l",  # CDS modification tag # TODO
        "961__x",  # CDS modification tag # TODO
        "962__k",  # description of the related works
        "964__a",  # spreadsheet
        "970__b",  # spreadsheet
        # "980__a",  # collection tag
        "981__a",  # duplicated record marker # TODO -> decide how to handle these
        "999C50",  # https://cds.cern.ch/record/2284609/export/hm?ln=en CMS contributions
        "999C52",  # https://cds.cern.ch/record/2640188/export/hm?ln=en
        "999C59",  # https://cds.cern.ch/record/2284615/export/hm?ln=en
        "999C5a",  # https://cds.cern.ch/record/2678429/export/hm?ln=en
        "999C5c",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C5h",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C5i",  # https://cds.cern.ch/record/2284892/export/hm?ln=en
        "999C5k",  # https://cds.cern.ch/record/2671914/export/hm?ln=en
        "999C5l",  # https://cds.cern.ch/record/2283115/export/hm?ln=en
        "999C5m",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C5o",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C5p",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C5r",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C5s",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C5t",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C5u",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C5v",  # https://cds.cern.ch/record/2283088/export/hm?ln=en
        "999C5x",  # https://cds.cern.ch/record/2710809/export/hm?ln=en
        "999C5y",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C5z",  # https://cds.cern.ch/record/2710809/export/hm?ln=en
        "999C6a",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C6t",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C6v",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        # "100_v",
        # "701__v",  # raw affiliation string from INSPIRE
        "852__c",
        "852__h",
        "599__a",
        # "595__z", # hidden rn
        # "595__b",
        # "595__c",
        "100__v",
        "700__v",
        "701__v",
        # Fields which should not appear
        # "502__u",  # miscataloguing university, TODO check occurences
        # TO IMPLEMENT /decide
        # "035__9",  # Inspire schema
        # "035__a",  # Inspire id value, contains unknown identifiers, TBD what to do
        # "269__a",  # imprint place
        "300__a",  # number of pages (imprint?)
        # "340__a",  # resource type ?
        # "490__a",  # tag for experiment + thesis
        # "595__z",  # SOME RECORD HAVE UNCL as value, do we keep it? what does UNCL mean
        # "690C_a",  # collection name, values to be explored
        # "701__",   # contributors (implemented, to confirm this is the right field)
        # "710__5",  # department / organisation author
        # "710__a",  # organisation author
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
        # "502__a",  # seems to tag the type of thesis
        # "502__b",  # seems to tag the university
        # "502__c",  # seems to tag the submission date
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
        # "8564_u",  # exclude files but include links (filter by domain)
        # "8564_y",  # exclude files but include links (filter by domain)
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
