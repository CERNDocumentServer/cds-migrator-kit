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


class PeopleAuthority(CdsOverdo):
    """Translation Index for CDS Books."""

    __query__ = "980__:AUTHORITY 980__:PEOPLE"
    __ignore_keys__ = {
        "001",
        "005",
        "372__0",  # affiliations
        "371__1",  # affiliation abbreviation
        "371__0",  # affiliation name
        "371__j",  # TODO ??? ex 2872846
        "371__k",  # phone number
        "371__f",  # phone number
        "371__l",  # phone number
        "371__h",  # looks like duplicate
        "371__d",  # duplicate  department info
        "371__g",  # group
        "371__v",  # source = CERN LDAP
        # "371__d",  # department
        "1001_a",  # surname
        "1000_a",  # given names
        "690C_a",  # always CERN
        "595__c",  # internal note
        "595__a",  # internal note
        "980__a",  # collection
    }

model = PeopleAuthority(
    bases=(base_model,), entry_point_group="cds_migrator_kit.migrator.rules.people"
)
