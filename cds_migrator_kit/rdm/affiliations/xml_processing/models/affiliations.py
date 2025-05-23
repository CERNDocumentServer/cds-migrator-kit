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
        "020__a",  # material of ISBN, to ignore in a repository
        "020__b",  # material of ISBN, to ignore in a repository
        "020__u",  # material of ISBN, to ignore in a repository
        "022__a",  # material of ISBN, to ignore in a repository
        "0247_2",  # DOI, summer student notes do not have it
        "0247_9",  # source of pid, INSPIRE data model
        "0247_a",  # DOI
        "0248_a",  # oai identifier, not needed to migrate, TBD
        "0248_a",  # oai identifier, not needed to migrate, TBD
        "0248_p",  # oai identifier, not needed to migrate, TBD
        "0248_p",  # oai identifier, not needed to migrate, TBD
        "0248_q",  # does appear in data, what is this field recid 2778897
        "0248_q",  # full text tag 2778897
        "035__9",  # Inspire schema
        "035__a",  # Inspire id value
        "035__d",
        "035__d",  # oai harvest tag
        "035__h",  # oai harvest tag # todo confirm
        "035__m",  # oai harvest tag
        "035__t",  # oai harvest tag
        "035__u",  # oai harvest tag
        "035__z",  # oai harvest tag
        "037__9",
        "037__a",  # (Report number) alternative identifiers -> scheme "CDS REFERENCE"
        "037__c",  # arxiv subject
        "041__a",  # languages
        "084__q",  # other ids qualifiers
        "084__a",  # other ids qualifiers
        "084__2",  # other ids qualifiers
        "088__9",  # source of report number
        "088__a",  # RN (manual introduced?) second report number (so the identifiers schemas are not unique!)
        "100__0",
        "100__9",  # #BEARD# tag
        "100__i",
        "100__j",  # alternative spelling of the name
        "100__k",
        "100__m",  # author's email <-- decided not to keep in RDM,
        "100__m",  # author's email <-- decided not to keep in RDM,
        "100__q",  # alternative spelling of the name
        "100__v",
        "210__a",
        "242__9",  # source of TRANSLATED title, INSPIRE data model
        "242__a",
        "242__b",
        "245__9",
        "245__9",  # source of title, INSPIRE data model
        "245__a",
        "245__b",
        "246__9",
        "246__a",
        "242__b",
        "246__i",  # abbreviation
        "246__i",  # abbreviation tag, applies to value of 246__A
        "246__n",  # to be migrated manually 968697, 2927034
        "246__p",  # to be migrated manually
        "260__a",
        "260__b",
        "260__c",  # Redundant (more detailed value is in 269__c imprint.pub_date)
        "269__a",
        "269__a",  # Redundant (more detailed value is in 260__c imprint.pub_date)
        "269__b",
        "269__b",  # Redundant (more detailed value is in 260__c imprint.pub_date)
        "269__c",
        "270__m",  # document contact email
        "270__m",  # document contact email
        "270__p",  # document contact person name
        "300__a",
        "340__a",  # ignore, spreadsheet
        "490__a",
        "490__v",
        "500__9",  # provenance of note # TODO
        "500__a",  # Note (-> description.type = other)
        "502__a",
        "502__b",
        "502__c",
        "520__9",
        "520__9",  # abstract provenance
        "520__a",  # Note (-> description.type = abstract
        "536__a",
        "536__f",
        "536__c",
        "536__r",
        "540__3",  # all the copyrights seem not to need this qualifier
        "540__a",  # material of license
        "540__b",  # material of license
        "540__f",  # funder of OA, to be manually migrated, 2845118
        "540__u",  # material of license
        "542__3",  # ignored, all the values were equal thesis
        "562__c",  # note
        "594__a",  # PUB 466504, 455788
        "595__a",  # always value CERN EDS, not displayed
        "595__b",  # always value CERN EDS, not displayed
        "595__c",  # always value CERN EDS, not displayed
        "595__a",  # always value CERN EDS, not displayed, TODO: do we keep?
        "595__z",  # SOME RECORD HAVE UNCL as value, do we keep it? what does UNCL mean
        "599__a",
        "650172",  # subject provenance
        "65017a",  # subject value
        "65017b",  # subject value
        "65027a",
        "650272",  # scheme of subjects, other than CERN
        "6531_9",  # keyword provenance
        "6531_9",  # looks like relevance or order of subjects (bibclassify, 1198695)
        "6531_a",  # keyword
        "6531_n",  # looks like relevance or order of subjects (bibclassify, 1198695)
        "6532_9",  # scheme of subjects, other than CERN
        "690C_a",  # collection name, not needed values(to drop: INTNOTE, CERN, otherwise parse PUBL to retrieve the department, if department not present in any other field)
        "690C_e",  # 878811  fermilab
        "6931_9",  # keyword
        "6931_a",  # keyword
        "693__a",  # accelerator, do we create a custom field?
        "693__b",  # beams recid: 2640381
        "693__e",  # custom_fields.cern:experiments
        "693__f",  # facility, do we create a custom field?
        "693__p",  # project, do we create a custom field?
        "693__s",  # study,  do we create a custom field?
        "694__9",  # note
        "694__a",  # note
        "695__9",  # study,  do we create a custom field?
        "695__a",  # study,  do we create a custom field?
        "700__0",  # Contributors (cds author id) - TBD if we keep, same with INSPIRE ID
        "700__9",  # #BEARD# tag
        "700__m",  # Contributors (email)
        "700__e",  # author's email <-- decided not to keep in RDM,
        "700__v",
        "701__0",
        "701__9",
        "701__e",
        "701__i",
        "701__j",
        "701__m",  # supervisors's email <-- decided not to keep in RDM,
        "701__v",
        "710__5",  # department / organisation author
        "710__a",  # organisation author
        "710__g",
        "720__a",  # author's duplicate
        "773__o",  # spreadsheet
        "773__x",  # spreadsheet
        "773__c",  # spreadsheet
        "773__v",  # spreadsheet
        "773__p",  # spreadsheet
        "773__y",  # spreadsheet
        "852__c",
        "852__h",
        "8564_8",  # Files system field
        "8564_8",  # Files system field
        "8564_s",  # Files system field
        "8564_s",  # Files system field
        "8564_u",  # Files
        "8564_x",  # Files system field
        "8564_x",  # Files system field
        "8564_y",  # Files
        "8564_y",  # Files / URLS label
        "859__f",  # creator's email, to be used to determine the owner
        "906__p",  # names, is it supervisor?
        "916__d",  # spreadsheet
        "916__e",  # spreadsheet
        "916__n",
        "916__s",
        "916__w",
        "916__y",  # year
        "937__c",  # modification date
        "937__s",  # modification person
        "960__a",  # collection id? usually value 12, to confirm if we ignore
        "960__a",  # collection id? usually value 14, to confirm if we ignore
        "961__c",  # CDS modification tag # TODO
        "961__h",  # CDS modification tag # TODO
        "961__l",  # CDS modification tag # TODO
        "961__x",  # CDS modification tag # TODO
        "962__k",  # description of the related works
        "962__b",  # description of the related works
        "962__n",  # description of the related works
        "963__a",
        "964__a",  # spreadsheet
        "970__a",  # alternative identifier, scheme ALEPH
        "970__b",  # spreadsheet
        "980__a",  # collection tag
        "980__b",
        "980__c",  # MIGRATED/DELETED - it shouldn't even make it here
        "981__a",
        "981__a",  # duplicated record marker # TODO -> decide how to handle these
        "999C50",
        "999C52",  # https://cds.cern.ch/record/2640188/export/hm?ln=en
        "999C59",
        "999C59",  # https://cds.cern.ch/record/2284615/export/hm?ln=en
        "999C5a",  # https://cds.cern.ch/record/2678429/export/hm?ln=en
        "999C5c",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C5h",
        "999C5h",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C5i",  # https://cds.cern.ch/record/2284892/export/hm?ln=en
        "999C5k",  # https://cds.cern.ch/record/2671914/export/hm?ln=en
        "999C5l",  # https://cds.cern.ch/record/2283115/export/hm?ln=en
        "999C5m",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C5o",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C5p",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C5r",
        "999C5r",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C5s",
        "999C5s",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C5t",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C5u",
        "999C5u",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C5v",  # https://cds.cern.ch/record/2283088/export/hm?ln=en
        "999C5x",
        "999C5x",  # https://cds.cern.ch/record/2710809/export/hm?ln=en
        "999C5y",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C5z",
        "999C5z",  # https://cds.cern.ch/record/2710809/export/hm?ln=en
        "999C6a",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C6t",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "999C6v",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        # https://cds.cern.ch/record/2284609/export/hm?ln=en CMS contributions
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
