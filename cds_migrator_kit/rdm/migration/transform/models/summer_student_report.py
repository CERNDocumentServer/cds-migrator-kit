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

from .overdo import CdsOverdo
from .base import model as base_model


class CMSSummerStudent(CdsOverdo):
    """Translation Index for CDS Books."""

    # putting values like 980__ INTNOTEPHPUBL INTNOTEEPPUBL is an overkill since it maps
    # different departments
    # __query__ = "980__:INTNOTEEPPUBLL 980__:NOTE 037__:CERN-STUDENTS-Note-\"/(.*?)/\""
    __query__ = "980__:NOTE 037__:CERN-STUDENTS-Note-*"

    __ignore_keys__ = {
        '65017a',
        '937__c',
        '960__a',
        '269__b',
        '6531_a',
        '906__p',
        '269__a',
        '100__0',
        '100__u', # Author affiliation
        '100__9', # Author to check
        '970__a', # TODO: check it
        '980__a',
        '980__c', # TODO: remove this one, it should not appear
        # '260__c',
        '8564_s', # Files
        '8564_y', # Files
        '8564_x', # Files
        '8564_8', # Files
        '500__a', # Note
        '246__i', # Abbreviation
        '246__a', # Abbreviation
        '595__z', # TODO: check it
        '0248_q', # TODO: check it
        '700__9', # Contributors (?)
        '700__0', # Contributors (cds author)
        '700__u', # Contributors (affiliation?)
        '700__m', # Contributors (email)
        '693__s', # study
        '693__p', # project
        '693__b', # TODO: check it
        '088__a', # RN (manual introduced?)
        '0248_a',
        '0248_p',
        '0247_a', # DOI
        '0247_2', # DOI
        '710__g', # Collaboration
        '981__a', # TODO: check it
        '562__c', # TODO: check it
        '970__d', # TODO: check it
        '270__p', # TODO: check it
        '270__m', # TODO: check it
        '035__a', # Inspire ref
        '035__9', # Inspire ref
        '693__a',
        '710__5',
        '916__w',
        '690C_a',
        '595__a',
        '693__e',
        '650172',
        '916__s',
        '041__a',
        '937__s',
        '859__f',
        '6531_9',
        '520__a',
        '963__a',
        '710__a',
        '100__a',
        '100__m',
    }
    _default_fields = None


model = CMSSummerStudent(bases=(base_model,), entry_point_group="cds_migrator_kit.migrator.rules.ssn")
