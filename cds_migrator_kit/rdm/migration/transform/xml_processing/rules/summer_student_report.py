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
"""Common RDM fields."""

from dojson.errors import IgnoreKey

from cds_dojson.marc21.fields.utils import clean_val, out_strip
from ...models.summer_student_report import model


# TODO: not working
@model.over('report_number', '^037__')
@out_strip
def report_number(self, key, value):
    """Translates report_number fields."""
    report_number = clean_val('a', value, str)
    if report_number:
        return report_number
    else:
        raise IgnoreKey('preprint_date')
