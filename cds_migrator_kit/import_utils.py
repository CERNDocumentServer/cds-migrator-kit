# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.
"""Utility function for dynamically checking module availability."""

import importlib


def import_module(module_name):
    """Try to import a module, return True if successful, otherwise False."""
    try:
        importlib.import_module(module_name)
        return True
    except ImportError:
        return False
