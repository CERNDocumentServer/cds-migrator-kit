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
from cds_migrator_kit.transform.xml_processing.models.base import model as base_model


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
        "020__9",  # https://cds.cern.ch/record/2041435/export/hm?ln=en
        "020__a",  # https://cds.cern.ch/record/2284609/export/hm?ln=en
        "020__b",  # https://cds.cern.ch/record/1566121/export/hm?ln=en
        "020__u",  # https://cds.cern.ch/record/2057663/export/hm?ln=en
        "020__z",  # https://cds.cern.ch/record/550152/export/hm?ln=en
        "022__a",  # https://cds.cern.ch/record/1138741/export/hm?ln=en
        "024472",  # https://cds.cern.ch/record/2280171/export/hm?ln=en
        "02447a",  # https://cds.cern.ch/record/2280171/export/hm?ln=en
        "0247_2",  # https://cds.cern.ch/record/2284615/export/hm?ln=en 242 -> subtitle translation
        "0247_9",  # https://cds.cern.ch/record/2630426/export/hm?ln=en
        "0247_9",  # https://cds.cern.ch/record/2645860/export/hm?ln=en
        "0247_9",  # https://cds.cern.ch/record/2711388/export/hm?ln=en
        "0247_q",  # https://cds.cern.ch/record/2645860/export/hm?ln=en
        "035__d",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "035__h",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "035__m",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
        "035__p",  # https://cds.cern.ch/record/2859334/export/hm?ln=en should be ID scheme, not provenance
        "035__t",  # https://cds.cern.ch/record/2645860/export/hm?ln=en
        "035__u",  # https://cds.cern.ch/record/2645860/export/hm?ln=en
        "035__z",  # https://cds.cern.ch/record/1032351/export/hm?ln=en not clear why it is duplicated
        "035__z",  # https://cds.cern.ch/record/852862/export/hm?ln=en
        "037__2",  # TODO other identifiers schemes HAL NNT https://cds.cern.ch/record/2883727/export/hm?ln=en
        "037__9",  # https://cds.cern.ch/record/1569831/export/hm?ln=en
        "037__b",  # https://cds.cern.ch/record/2883908/export/hm?ln=en should be in 2? scheme id
        "037__c",  # https://cds.cern.ch/record/1569831/export/hm?ln=en
        "037__n",  # should be in schema field https://cds.cern.ch/record/2886140/export/hm?ln=en
        "037__z",  # https://cds.cern.ch/record/1538525/export/hm?ln=en report number in a wrong subfield
        "037__z",  # https://cds.cern.ch/record/2094394/export/hm?ln=en to fix, should be in a subfield, same:2008723, 2239318, 2299967
        "080__a",  # https://cds.cern.ch/record/176062/export/hm?ln=en
        "084__2",  # https://cds.cern.ch/record/318683/export/hm?ln=en
        "084__9",  # https://cds.cern.ch/record/2713231/export/hm?ln=en
        "084__a",  # https://cds.cern.ch/record/318683/export/hm?ln=en
        "088__9",  # https://cds.cern.ch/record/220489/export/hm?ln=en
        "100__i",  # https://cds.cern.ch/record/1569831/export/hm?ln=en
        "100__j",  # https://cds.cern.ch/record/1569831/export/hm?ln=en
        "100__q",  # https://cds.cern.ch/record/2285198/export/hm?ln=en needs curation, duplicated author, also 2285221
        "100__t",  # TODO ROR https://cds.cern.ch/record/2879802/export/hm?ln=en
        "100__t",  # todo ror https://cds.cern.ch/record/2883993/export/hm?ln=en
        "100__v",  # https://cds.cern.ch/record/2022781/export/hm?ln=en
        "210__a",  # https://cds.cern.ch/record/2284607/export/hm?ln=en
        "242__9",  # https://cds.cern.ch/record/2284615/export/hm?ln=en
        "242__a",  # https://cds.cern.ch/record/1569831/export/hm?ln=en
        "242__b",  # https://cds.cern.ch/record/744924/export/hm?ln=en
        "245__9",  # https://cds.cern.ch/record/2284615/export/hm?ln=en
        "245__W",  # $$ broken, https://cds.cern.ch/record/2684465/export/hm?ln=en
        "245__b",  # https://cds.cern.ch/record/2284888/export/hm?ln=en # subtitle ?
        "246__9",  # https://cds.cern.ch/record/2284607/export/hm?ln=en
        "246__b",  # https://cds.cern.ch/record/2057663/export/hm?ln=en
        "246__n",  # https://cds.cern.ch/record/968697/export/hm?ln=en
        "246__p",  # check ILS fields https://cds.cern.ch/record/968697/export/hm?ln=en # TODO
        "246__y",  # https://cds.cern.ch/record/1566121/export/hm?ln=en
        "250__a",  # https://cds.cern.ch/record/383483/export/hm?ln=en edition? 401681
        "260__a",  # https://cds.cern.ch/record/1242493/export/hm?ln=en
        "260__b",  # https://cds.cern.ch/record/1242493/export/hm?ln=en
        "260__u",  # https://cds.cern.ch/record/1175646/export/hm?ln=en
        "269__0",  # https://cds.cern.ch/record/2894345/export/hm?ln=en
        "269__2",  # https://cds.cern.ch/record/2892457/export/hm?ln=en no subfield?
        "269__b",  # https://cds.cern.ch/record/1063371/export/hm?ln=en
        "269__b",  # https://cds.cern.ch/record/2636618/export/hm?ln=en
        "269__u",  # imprint date ? https://cds.cern.ch/record/2915293/export/hm?ln=en, 2915298, 2915333
        "300__s",  # https://cds.cern.ch/record/1488923/export/hm?ln=en
        "300_c",  # https://cds.cern.ch/record/2689347/export/hm?ln=en should be in 300_a, 2693068, 2690382
        "320__a",  # https://cds.cern.ch/record/2812400/export/hm?ln=en looks like abstract, should be moved there?
        "340__e",  # ebook marker # TODO https://cds.cern.ch/record/1606787/export/hm?ln=en
        "490__v",  # https://cds.cern.ch/record/1473435/export/hm?ln=en
        "500__9",  # https://cds.cern.ch/record/1569831/export/hm?ln=en
        "500__i",  # https://cds.cern.ch/record/797742/export/hm?ln=en
        "502__d",  # https://cds.cern.ch/record/2645860/export/hm?ln=en
        "502__u",  # https://cds.cern.ch/record/2918710/export/hm?ln=en
        "505__c",  # https://cds.cern.ch/record/2636105/export/hm?ln=en
        "520__ ",  # $$ breaking https://cds.cern.ch/record/1607078/export/hm?ln=en, 2790972
        "520__9",  # https://cds.cern.ch/record/1569831/export/hm?ln=en
        "520__F",  # $$ breaking https://cds.cern.ch/record/1607078/export/hm?ln=en
        "520__H",  # https://cds.cern.ch/record/2812400/export/hm?ln=en $$ breaking the field
        "520__W",  # https://cds.cern.ch/record/2879802/export/hm?ln=en $$ breaking
        "520__Z",  # https://cds.cern.ch/record/2843737/export/hm?ln=en # <--------- this one is interesting, apparently $$ breaks XML!!!
        "520__\\",  # https://cds.cern.ch/record/2711388/export/hm?ln=en
        "520__^",  # https://cds.cern.ch/record/2710229/export/hm?ln=en
        "520___",  # https://cds.cern.ch/record/2710229/export/hm?ln=en $$ broken
        "520__b",  # https://cds.cern.ch/record/2898541/export/hm?ln=en
        "520__p",  # https://cds.cern.ch/record/2898054/export/hm?ln=en
        "520__y",  # https://cds.cern.ch/record/2053769/export/hm?ln=en $$ broken, 1969601, 2102935
        "520__y",  # https://cds.cern.ch/record/2244617/export/hm?ln=en ? https://cds.cern.ch/record/2102935/export/hm?ln=en
        "536__a",  # https://cds.cern.ch/record/1636892/export/hm?ln=en technical student program - another community?
        "536__c",  # https://cds.cern.ch/record/1498702/export/hm?ln=en
        "536__f",  # https://cds.cern.ch/record/1498702/export/hm?ln=en
        "536__r",  # https://cds.cern.ch/record/1498702/export/hm?ln=en
        "540__3",  # https://cds.cern.ch/record/2645860/export/hm?ln=en
        "540__a",  # https://cds.cern.ch/record/2022781/export/hm?ln=en
        "540__b",  # https://cds.cern.ch/record/2022781/export/hm?ln=en
        "540__f",  # https://cds.cern.ch/record/1538525/export/hm?ln=en looks like license and funding info?
        "540__g",  # https://cds.cern.ch/record/1538525/export/hm?ln=en
        "540__u",  # https://cds.cern.ch/record/2022781/export/hm?ln=en
        "541__a",  # restricted record ! https://cds.cern.ch/record/1655788/export/hm?ln=en (location?) check with IT secretary?
        "542__3",  # https://cds.cern.ch/record/1096410/export/hm?ln=en
        "542__c",  # https://cds.cern.ch/record/2895598/export/hm?ln=en
        "542__d",  # https://cds.cern.ch/record/2895598/export/hm?ln=en
        "542__f",  # https://cds.cern.ch/record/2285212/export/hm?ln=en
        "542__g",  # https://cds.cern.ch/record/2897660/export/hm?ln=en
        "542__g",  # https://cds.cern.ch/record/2898037/export/hm?ln=en
        "542__u",  # https://cds.cern.ch/record/2285212/export/hm?ln=en
        "560172",  # https://cds.cern.ch/record/383486/export/hm?ln=en
        "56017a",  # https://cds.cern.ch/record/383486/export/hm?ln=en wrong keyword subfield
        "590__a",  # abstract translation TODO https://cds.cern.ch/record/1476067/export/hm?ln=en
        "594__a",  # https://cds.cern.ch/record/466504/export/hm?ln=en, 455788
        "595__b",  # https://cds.cern.ch/record/1498702/export/hm?ln=en
        "595__c",  # https://cds.cern.ch/record/1498702/export/hm?ln=en
        "595__d",  # https://cds.cern.ch/record/1390829/export/hm?ln=en
        "595__s",  # https://cds.cern.ch/record/1367848/export/hm?ln=en
        "595__u",  # https://cds.cern.ch/record/2244659/export/hm?ln=en looks like url, why not in url field? 1483038
        "599__a",  # https://cds.cern.ch/record/2744546/export/hm?ln=en CDS LINK
        "6351_9",  # https://cds.cern.ch/record/1375850/export/hm?ln=en
        "6351_a",  # https://cds.cern.ch/record/1375850/export/hm?ln=en  subjects TODO
        "65017b",  # https://cds.cern.ch/record/1498702/export/hm?ln=en
        "65017e",  # https://cds.cern.ch/record/2710229/export/hm?ln=en keywords scheme # todo if == SzGeCERN
        "650272",  # https://cds.cern.ch/record/2022781/export/hm?ln=en
        "65027a",  # https://cds.cern.ch/record/2022781/export/hm?ln=en
        "65027b",  # https://cds.cern.ch/record/2022781/export/hm?ln=en
        "6531_2",  # https://cds.cern.ch/record/2710229/export/hm?ln=en subject scheme
        "6531_n",  # TODO cehck https://cds.cern.ch/record/1198695/export/hm?ln=en
        "6531aa",  # https://cds.cern.ch/record/2690627/export/hm?ln=en
        "6532_9",  # https://cds.cern.ch/record/1341864/export/hm?ln=en looks like subjects but seems a wrong field
        "6532_a",  # https://cds.cern.ch/record/1341864/export/hm?ln=en looks like subjects but seems a wrong field
        "653__9",  # https://cds.cern.ch/record/2263131/export/hm?ln=en strange keyword scheme 2283139, 1464084
        "653__a",  # https://cds.cern.ch/record/2263131/export/hm?ln=en
        "659172",  # https://cds.cern.ch/record/1198225/export/hm?ln=en subjects, TODO check 1198225
        "65917a",  # https://cds.cern.ch/record/1198225/export/hm?ln=en
        "690C_9",  # founder? https://cds.cern.ch/record/1295514/export/hm?ln=en
        "690C_e",  # https://cds.cern.ch/record/1751219/export/hm?ln=en
        "694__9",  # https://cds.cern.ch/record/744924/export/hm?ln=en
        "694__a",  # https://cds.cern.ch/record/744924/export/hm?ln=en
        "695__2",  # https://cds.cern.ch/record/1566113/export/hm?ln=en
        "695__9",  # https://cds.cern.ch/record/1569831/export/hm?ln=en
        "695__a",  # https://cds.cern.ch/record/1569831/export/hm?ln=en
        "695__e",  # https://cds.cern.ch/record/1566113/export/hm?ln=en
        "697C_a",  # https://cds.cern.ch/record/120467/export/hm?ln=en ILS record
        "701__i",  # https://cds.cern.ch/record/1569831/export/hm?ln=en
        "701__j",  # https://cds.cern.ch/record/1569831/export/hm?ln=en
        "701__t",  # todo ror https://cds.cern.ch/record/2883993/export/hm?ln=en
        "701__v",  # https://cds.cern.ch/record/2711388/export/hm?ln=en
        "701_aa",  # https://cds.cern.ch/record/2690834/export/hm?ln=en
        "701_ae",  # https://cds.cern.ch/record/2690834/export/hm?ln=en
        "701_au",  # https://cds.cern.ch/record/2690834/export/hm?ln=en wrong subfield ind
        "710__b",  # https://cds.cern.ch/record/733805/export/hm?ln=en
        "710__e",  # https://cds.cern.ch/record/2914058/export/hm?ln=en looks like contributors, should be in 700?
        "710__u",  # https://cds.cern.ch/record/2636618/export/hm?ln=en # looks like funder ? hosting institution?
        "720__a",  # https://cds.cern.ch/record/1569831/export/hm?ln=en
        "773__c",  # https://cds.cern.ch/record/318683/export/hm?ln=en
        "773__n",  # https://cds.cern.ch/record/1670134/export/hm?ln=en
        "773__n",  # https://cds.cern.ch/record/317991/export/hm?ln=en
        "773__o",  # https://cds.cern.ch/record/363288/export/hm?ln=en
        "773__p",  # https://cds.cern.ch/record/318683/export/hm?ln=en
        "773__u",  # https://cds.cern.ch/record/2846123/export/hm?ln=en does not seem like a thesis ? indico event, also 2918566
        "773__v",  # https://cds.cern.ch/record/318683/export/hm?ln=en
        "773__x",  # https://cds.cern.ch/record/2318731/export/hm?ln=en
        "773__y",  # https://cds.cern.ch/record/318683/export/hm?ln=en
        "775__b",  # https://cds.cern.ch/record/108178/export/hm?ln=en
        "775__b",  # https://cds.cern.ch/record/1528167/export/hm?ln=en # TODO related document
        "775__c",  # https://cds.cern.ch/record/108178/export/hm?ln=en
        "775__w",  # https://cds.cern.ch/record/108178/export/hm?ln=en
        "775__w",  # https://cds.cern.ch/record/1528167/export/hm?ln=en # TODO related document
        "7870_i",  # https://cds.cern.ch/record/2839604/export/hm?ln=en
        "7870_r",  # https://cds.cern.ch/record/2879802/export/hm?ln=en a note? shouldn't be in a note field?
        "7870_w",  # https://cds.cern.ch/record/2839604/export/hm?ln=en
        "852__a",  # https://cds.cern.ch/record/1032351/export/hm?ln=en
        "852__a",  # locatoin, cern libary # TODO https://cds.cern.ch/record/366331/export/hm?ln=en, 366497
        "852__c",  # https://cds.cern.ch/record/180824/export/hm?ln=en -----> thesis in depot
        "852__h",  # https://cds.cern.ch/record/180824/export/hm?ln=en
        "8546_a",  # https://cds.cern.ch/record/2802982/export/hm?ln=en
        "8564_a",  # https://cds.cern.ch/record/2802982/export/hm?ln=en
        "8564_b",  # https://cds.cern.ch/record/2883993/export/hm?ln=en
        "8564_c",  # what kind of date is it? https://cds.cern.ch/record/2883993/export/hm?ln=en
        "8564_w",  # https://cds.cern.ch/record/548140/export/hm?ln=en
        "8565_u",  # https://cds.cern.ch/record/2284218/export/hm?ln=en wrong url field
        "8565_y",  # https://cds.cern.ch/record/2284218/export/hm?ln=en
        "856__a",  # https://cds.cern.ch/record/1536507/export/hm?ln=en seems like an url, but missing ind=4
        "856__y",  # https://cds.cern.ch/record/1536507/export/hm?ln=en
        "859__a",  # https://cds.cern.ch/record/1613874/export/hm?ln=en wrong submitter subfield, should be f, 1442305
        "901__u",  # https://cds.cern.ch/record/831034/export/hm?ln=en
        "916__a",  # https://cds.cern.ch/record/1236728/export/hm?ln=en
        "916__d",  # https://cds.cern.ch/record/325944/export/hm?ln=en not sure the meaning of the number
        "916__e",  # source ? https://cds.cern.ch/record/1536507/export/hm?ln=en
        "925__a",  # https://cds.cern.ch/record/1032351/export/hm?ln=en
        "925__b",  # https://cds.cern.ch/record/1032351/export/hm?ln=en
        "962__b",  # https://cds.cern.ch/record/450847/export/hm?ln=en
        "962__k",  # https://cds.cern.ch/record/450847/export/hm?ln=en
        "962__n",  # https://cds.cern.ch/record/450847/export/hm?ln=en
        "964__a",  # https://cds.cern.ch/record/180824/export/hm?ln=en
        "970__b",  # ? note ? internal system tag? https://cds.cern.ch/record/139394/export/hm?ln=en
        "980__b",  # https://cds.cern.ch/record/1498702/export/hm?ln=en
        "999C50",  # https://cds.cern.ch/record/2284609/export/hm?ln=en
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
        "701__m",
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
    bases=(base_model,),
    entry_point_group="cds_migrator_kit.migrator.rules.base",
)
