# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.
"""Minter configuration for CDS Migrator Kit."""

import warnings

from cds_migrator_kit.import_utils import import_module

# Default: No minter
selected_minter = None

# Check if `rdm` is installed and set the minter
if import_module("cds_rdm.__init__"):
    from cds_rdm.minters import legacy_recid_minter as selected_minter
else:
    warnings.warn(
        "No valid PID minter found. Ensure `rdm` is installed.", RuntimeWarning
    )

# Expose the minter function
legacy = selected_minter
