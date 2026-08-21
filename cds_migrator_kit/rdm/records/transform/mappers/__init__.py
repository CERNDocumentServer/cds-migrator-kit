# -*- coding: utf-8 -*-
#
# Copyright (C) 2022-2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM record field mappers.

Each mapper owns the derivation of a single ``metadata`` or
``custom_fields`` value from the legacy record entry, composed together by
``RecordEntry`` in ``entities/record.py``.
"""
