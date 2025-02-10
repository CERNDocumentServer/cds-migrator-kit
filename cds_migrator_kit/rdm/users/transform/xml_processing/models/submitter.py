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

from cds_migrator_kit.transform.xml_processing.models.base import model as base_model
from cds_migrator_kit.transform.overdo import CdsOverdo


class SubmitterModel(CdsOverdo):
    """Translation Model for submitters."""

    __query__ = (
        # meant to match any record
        ""
    )

    __ignore_keys__ = {
        # decided to ignore
        "001",
        "003",

        # TODO add to report
        # https://cds.cern.ch/record/1569831/export/hm?ln=en
        '695__a',
        '037__c',
        '500__9',
        '037__9',
        '695__9',
        '520__9',
        '720__a',
        '100__i',
        '701__j',
        '701__i',
        '242__a',
        '100__j',
        # https://cds.cern.ch/record/2284606/export/hm?ln=en
        '999C5p', '999C5y', '999C6t', '999C5s', '999C6v', '999C5u', '999C5m', '999C6a',  #
         '999C5t', '999C5c', '035__d', '035__h', '035__m', '999C5h', '999C5r', '999C5o',
        # https://cds.cern.ch/record/2284607/export/hm?ln=en
        '246__9', '210__a',
        # https://cds.cern.ch/record/2284609/export/hm?ln=en
        '020__a', '999C50',
        # https://cds.cern.ch/record/2284615/export/hm?ln=en
        '242__9', '245__9', '999C59', '0247_2', # 242 -> subtitle translation
        # https://cds.cern.ch/record/1242493/export/hm?ln=en
        '260__a', '260__b',
        # https://cds.cern.ch/record/180824/export/hm?ln=en -----> thesis in depot
        '852__c', '964__a', '852__h',
        # https://cds.cern.ch/record/1636892/export/hm?ln=en technical student program - another community?
        '536__a',
        # https://cds.cern.ch/record/318683/export/hm?ln=en
        '773__p', '084__a', '084__2', '773__y', '773__c', '773__v',
        # https://cds.cern.ch/record/2710809/export/hm?ln=en
        '999C5x', '999C5z',
        # https://cds.cern.ch/record/548140/export/hm?ln=en
        '8564_w',
        # https://cds.cern.ch/record/220489/export/hm?ln=en
        "088__9",
        # https://cds.cern.ch/record/2284888/export/hm?ln=en
        "245__b", # subtitle ?
        # https://cds.cern.ch/record/2284892/export/hm?ln=en
        "999C5i",
        # https://cds.cern.ch/record/2022781/export/hm?ln=en
        '65027b', '650272', '540__u', '65027a', '100__v', '540__b', '540__a',
        # https://cds.cern.ch/record/744924/export/hm?ln=en
        '694__a', '242__b', '694__9',
        # https://cds.cern.ch/record/2630426/export/hm?ln=en
        "0247_9",
        # https://cds.cern.ch/record/1498702/export/hm?ln=en
        '536__r', '536__c', '65017b', '980__b', '536__f', '595__c', '595__b',
        # https://cds.cern.ch/record/2285198/export/hm?ln=en needs curation, duplicated author, also 2285221
        "100__q",
        # https://cds.cern.ch/record/2285212/export/hm?ln=en
        '542__f', '542__u',
        # https://cds.cern.ch/record/2678429/export/hm?ln=en
        "999C5a",
        # https://cds.cern.ch/record/1236728/export/hm?ln=en
        "916__a",
        # https://cds.cern.ch/record/1367848/export/hm?ln=en
        "595__s",
        # https://cds.cern.ch/record/2711388/export/hm?ln=en
        '520__\\', '701__v', '0247_9',
        # https://cds.cern.ch/record/2645860/export/hm?ln=en
        '0247_q', '035__u', '0247_9', '035__t', '502__d', '540__3',
        # https://cds.cern.ch/record/1138741/export/hm?ln=en
        "022__a",
        # https://cds.cern.ch/record/2744546/export/hm?ln=en CDS LINK
        "599__a",
        # https://cds.cern.ch/record/2671914/export/hm?ln=en
        "999C5k",
        # https://cds.cern.ch/record/450847/export/hm?ln=en
        '962__n', '962__b', '962__k',
        # https://cds.cern.ch/record/2318731/export/hm?ln=en
        "773__x",
        # https://cds.cern.ch/record/2843737/export/hm?ln=en
        "520__Z", # <--------- this one is interesting, apparently $$ breaks XML!!!
        # https://cds.cern.ch/record/550152/export/hm?ln=en
        "020__z",
        # https://cds.cern.ch/record/1566113/export/hm?ln=en
        '695__e', '695__2',
        # https://cds.cern.ch/record/1473435/export/hm?ln=en
        "490__v",
        # https://cds.cern.ch/record/1566121/export/hm?ln=en
        '020__b', '246__y',
        # https://cds.cern.ch/record/2057663/export/hm?ln=en
        '246__b', '020__u',
        # https://cds.cern.ch/record/2041435/export/hm?ln=en
        '020__9',
        # https://cds.cern.ch/record/2713231/export/hm?ln=en
        '084__9',
        # https://cds.cern.ch/record/831034/export/hm?ln=en
        "901__u",
        # https://cds.cern.ch/record/2895598/export/hm?ln=en
        '542__d', '542__c',
        # https://cds.cern.ch/record/176062/export/hm?ln=en
        "080__a",
        # https://cds.cern.ch/record/733805/export/hm?ln=en
        "710__b",
        # https://cds.cern.ch/record/2636618/export/hm?ln=en
        "710__u", # looks like funder ? hosting institution?

        '269__b', '542__g',  # 2897660
        # 2283115
        "999C5l",
        # 2918710
        "502__u",
        # 1488923
        "300__s",
        # 2898037
        '542__g',
        # 2898054
        '520__p',
        # 2898541
        '520__b',
        # 1751219
        "690C_e",
        # 2839604
        '7870_i', '7870_w',
        # 1390829
        "595__d",
        # 2636105
        "505__c",
        # 1063371
        "269__b",
        # 1096410
        "542__3",
        "037__z", # https://cds.cern.ch/record/2094394/export/hm?ln=en to fix, should be in a subfield, same:2008723, 2239318, 2299967
        "520__y", # 2244617 ? 2102935
        "595__u", # 2244659 looks like url, why not in url field? 1483038
        "035__z", # 1032351 not clear why it is duplicated
        '925__a', '925__b', '852__a', '541__a', # restricted record ! 1655788 (location?) check with IT secretary?
        "520__H", # 2812400 $$ breaking the field
        "320__a", # looks like abstract, should be moved there?
        "773__u", # https://cds.cern.ch/record/2846123/export/hm?ln=en does not seem like a thesis ? indico event, also 2918566
        "260__u", # 1175646
        "7870_r", # a note? shouldn't be in a note field? https://cds.cern.ch/record/2879802/export/hm?ln=en
        "520__W", # $$ breaking
        "100__t", # TODO ROR
        "916__e", # source ? 1536507
        '856__y', '856__a', # seems like an url, but missing ind=4
        "690C_9", # founder? 1295514
        "250__a", # 383483 edition? 401681
        "773__n", # 317991
        "594__a", # 466504, 455788
        "859__a", # 1613874 wrong submitter subfield, should be f, 1442305
        "970__b", # ? note ? internal system tag? 139394
        "710__e", # 2914058 looks like contributors, should be in 700?
        "245__W", # $$ broken, 2684465
        '6351_9', '6351_a', # 1375850  subjects TODO
        "916__d", # 325944 not sure the meaning of the number
        '6532_a', '6532_9', # look like subjects but seems a wrong field, 1341864
        "037__z", # 1538525 report number in a wrong subfield
        '540__g', '540__f', # looks like license and funding info?
        "269__u", # imprint date ? 2915293, 2915298, 2915333
        "773__n", # 1670134
        "269__0", # 2894345
        '775__c', '775__b', '775__w', # 108178
        "500__i", # 797742
        "037__2", # TODO other identifiers schemes HAL NNT 2883727
        "037__b",  # 2883908 should be in 2? scheme id
        '701__t', # todo ror 2883993
        '8564_b', # 2883993
        '100__t', # todo ror 2883993
        '8564_c', # what kind of date is it? 2883993
        "035__z", # 852862
        "340__e", # ebook marker # TODO 1606787
        '520__F', '520__ ', # $$ breaking 1607078, 2790972
        "590__a", # abstract translation TODO 1476067
        "520__y", # 2053769 $$ broken, 1969601, 2102935
        '653__a', '653__9', # 2263131 strange keyword scheme 2283139, 1464084
        "300_c", # 2689347 should be in 300_a, 2693068, 2690382
        "037__n", # should be in schema field 2886140
        "773__o", # 363288
        "6531aa", # 2690627
        '701_aa', '701_ae', '701_au', # 2690834 wrong subfield ind
        "852__a", # locatoin, cern libary # TODO 366331, 366497
        "8564_a", "8546_a", # 2802982
        "035__p" , # 2859334 should be ID scheme, not provenance
        "246__n", "246__p", # check ILS fields 968697 # TODO
        "65917a", "659172", # subjects, TODO check 1198225
        "999C52", # 2640188
        "6531_n", # TODO cehck 1198695
        '024472', '02447a', # 2280171
        "269__2", # 2892457 no subfield?
        '775__w', '775__b', # 1528167 # TODO related document
        "999C5v", # 2283088
        "697C_a", # 120467 ILS record
        '560172', '56017a', # 383486 wrong keyword subfield
        '8565_y', '8565_u', # 2284218 wrong url field
        '520__^', '520___', # 2710229 $$ broken


        "6531_2", # subject scheme
        "65017e",  # keywords scheme # todo if == SzGeCERN
        '701__m',
        "0247_a",  # oai identifier, not needed to migrate, TBD
        "0248_a",  # oai identifier, not needed to migrate, TBD
        "0248_p",  # oai identifier, not needed to migrate, TBD
        "0248_q",  # full text tag 2778897
        "035__9",  # Inspire schema
        "035__a",  # Inspire id value, contains unknown identifiers, TBD what to do
        "037__a",  # (Report number) alternative identifiers -> scheme "CDS REFERENCE"
        "041__a",  # languages
        "088__a",
        # RN (manual introduced?) second report number (so the identifiers schemas are not unique!)
        "100__9",
        # #BEARD# tag, checking if there is no other unexpected value, but discarding #BEARD# values
        "100__a",  # author's name
        "100__m",  # author's email <-- decided not to keep in RDM,
        "100__u",  # Author affiliation
        "246__a",  # alt title
        "246__i",  # abbreviation
        "246__i",  # abbreviation tag, applies to value of 246__A
        "260__c",  # Redundant (more detailed value is in 269__c imprint.pub_date)
        "269__a",  # imprint place
        "269__c",  # imprint place
        "270__m",  # document contact email
        "270__p",  # document contact person name
        "300__a",  # number of pages (imprint?)
        "340__a",  # resource type ?
        "490__a",  # tag for experiment + thesis
        "500__a",  # Note (-> description.type = other)
        "502__a",  # seems to tag the type of thesis
        "502__b",  # seems to tag the university
        "502__c",  # seems to tag the defense date
        "520__a",  # Note (-> description.type = abstract
        "562__c",  # note
        "595__a",  # always value CERN EDS, not displayed, TODO: do we keep?
        "595__z",  # SOME RECORD HAVE UNCL as value, do we keep it? what does UNCL mean
        "650172",  # subject provenance
        "65017a",  # subject value
        "6531_9",  # keyword provenance
        "6531_a",  # keyword value
        "690C_a",  # collection name, values to be explored
        "693__a",  # accelerator, custom field
        "693__b",  # beams recid: 2640381
        "693__e",  # custom_fields.cern:experiments
        "693__f",  # facility, custom field
        "693__p",  # project, custom field
        "693__s",  # study,  custom field
        "700__0",  # Contributors (cds author id, inspire author id)
        "700__9",  # #BEARD# tag
        "700__a",  # Contributors (full name)
        "700__m",  # author's email <-- decided not to keep in RDM,
        "700__u",  # Contributors (affiliation)
        "701__a",  # contributors (implemented, to confirm this is the right field)
        "701__e",  # contributors (implemented, to confirm this is the right field)
        "701__u",  # affiliation
        "701__0",  # contributors ids
        "701__9",  # BEARD
        "710__5",  # department / organisation author
        "710__a",  # organisation author
        "710__g",
        # Collaboration, OK to migrate as corporate contributor (not creator)?
        "8564_8",  # Files system field
        "8564_s",  # Files system field
        "8564_u",  # exclude files but include links (filter by domain)
        "8564_x",  # Files system field
        "8564_y",  # Files
        "906__p",  # names, is it supervisor?
        "916__n",
        "916__s",
        "916__w",
        "916__y",  # year
        "937__c",  # modification date
        "937__s",  # modification person
        "960__a",  # collection id? usually value 14, to confirm if we ignore
        "961__c",  # seems like a set of identifiers, to be defined
        "961__h",  # seems like a set of identifiers, to be defined
        "961__l",  # seems like a set of identifiers, to be defined
        "961__x",  # seems like a set of identifiers, to be defined
        "963__a",
        "970__a",  # alternative identifier, scheme ALEPH
        "980__a",  # collection tag
        "981__a",  # duplicate record id
        "999C6",  # to define what is the field

        # "859__f",  # creator's email, to be used to determine the owner
    }


submitter_model = SubmitterModel(
    # use base rules - we don't need any other rule than 859
    bases=(base_model,), entry_point_group="cds_migrator_kit.migrator.rules.base"
)
