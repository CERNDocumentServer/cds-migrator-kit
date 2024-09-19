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
        # decided
        "690C_a",   # collection name, not needed values(INTNOTE, CERN, PUBLIT)
        "8564_8",  # Files system field
        "8564_s",  # Files system field
        "8564_x",  # Files system field
        "8564_y",  # Files
        "8564_u",  # Files
        "0247_2",  # DOI, summer student notes do not have it
        "0247_a",  # DOI
        "100__m",  # author's email <-- decided not to keep in RDM,


        # TO Implement (to remove from here)
        "710__5",  # department / organisation author
        "710__a",  # organisation author
        "859__f",  # creator's email, to be used to determine the owner ???
        "700__m",  # author's email <-- decided not to keep in RDM,
        "035__9",  # Inspire schema
        "035__a",  # Inspire id value
        "246__a",  # explanation of abrreviations, TODO: shall we keep it in notes?
        # "916__s",  # creation date

        # TO DECIDE
        "0248_a",  # oai identifier, not needed to migrate, TBD
        "0248_p",  # oai identifier, not needed to migrate, TBD
        "970__a",  # some kind of identifier? "000732636CER"
        "906__p",  # name, is it supervisor?
        "650172",  # TODO TBD
        "65017a",  # TODO TBD
        "595__a",  # always value CERN EDS, not displayed, TODO: do we keep?
        "595__z",  # SOME RECORD HAVE UNCL as value, do we keep it?

        # "0248_q",  does appear

        "246__i",  # abbreviation tag, applies to value of 246__A
        "088__a",  # RN (manual introduced?)
        "100__0",
        "100__9",  # Author to check
        # "100__a",

        # "100__u",  # Author affiliation
        "246__a",  # Abbreviation
        "246__i",  # Abbreviation
        "260__c",  # Redundant (more detailed value is in 269__c)
        # "270__m",
        # "270__p",
        # "500__a",  # Note
        # "562__c",  # note
        "6531_a",
        # "693__a", # accelerator
        # "693__b", # value 'H4' in 1 record: 2640381
        # "693__e", # experiment
        # "693__f", # facility
        # "693__p",  # project
        # "693__s",  # study
        # "700__u",  # Contributors (affiliation?)
        # "700__0",  # Contributors (cds author id) - TBD if we keep
        # "700__a",  # Contributors (full name)
        # "700__9",  # Contributors (?) - value '#BEARD#' in some records - to ignore
        # "700__m",  # Contributors (email)
        # "700__u",  # Contributors (affiliation)
        # "710__g",  # Collaboration


        # "906__p", # probably supervisor TODO: check

        "960__a",  # collection id? usuall y valu 12
        "963__a",  # restriction
        "980__a",
        "980__c",  # TODO: remove this one, it should not appear
    }
    _default_fields = None


model = CMSSummerStudent(
    bases=(base_model,), entry_point_group="cds_migrator_kit.migrator.rules.ssn"
)
