# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.
"""Migration configuration for CDS Migrator Kit."""

from cds_migrator_kit.import_utils import import_module

selected_config = None

# Check for `rdm` dependencies
if import_module("cds_rdm.__init__"):
    from cds_migrator_kit.rdm import migration_config as selected_config

# Check for `videos` dependencies
elif import_module("cds.version"):
    from cds_migrator_kit.videos import migration_config as selected_config

# If no valid module is found, use default one
if selected_config is None:
    from cds_migrator_kit import config as selected_config

# Set the selected config module
globals().update(vars(selected_config))
