# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos dates migration module."""


from datetime import datetime


def parse_date(date_str):
    """Parses a date string into 'YYYY-MM-DD' format."""
    if not date_str or not isinstance(date_str, str):
        return

    valid_formats = [
        "%Y-%m-%dT%H:%M:%SZ",  # 2008-03-11T11:00:00Z
        "%Y-%m-%dT%H:%M:%S",  # 2008-03-11T11:00:00
        "%Y-%m-%d",  # 1993-08-09
        "%d %b %Y",  # 27 Nov 1998
        "%d %B %Y",  # 27 November 1998
        "%Y%m%d",  # 20030512
    ]

    for format in valid_formats:
        try:
            parsed_date = datetime.strptime(date_str, format)
            return parsed_date.strftime("%Y-%m-%d")
        except ValueError:
            continue  # Try the next format

    return
