# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM transform config module."""

# filters out PIDs which we don't migrate
PIDS_SCHEMES_TO_DROP = ["HAL"]
# validates allowed schemes
PIDS_SCHEMES_ALLOWED = ["DOI"]

# stores the identifiers found in PIDs field in the alternative identifiers instead
PID_SCHEMES_TO_STORE_IN_IDENTIFIERS = ["ARXIV", "HDL", "HANDLE", "URN", "INIS"]

IDENTIFIERS_SCHEMES_TO_DROP = ["SPIRES", "HAL", "OSTI", "SLAC", ]
IDENTIFIERS_VALUES_TO_DROP = "oai:arXiv.org"

CONTROLLED_SUBJECTS_SCHEMES = ["szgecern", "cern", "cds"]

RECOGNISED_KEYWORD_SCHEMES = ["author", "cms", "arxiv", "inspire", "spr", "inis", "lanl eds", "in2p3"]
KEYWORD_SCHEMES_TO_DROP = ["proquest"]

ALLOWED_THESIS_COLLECTIONS = ["thesis", "publcms", "book"]
IGNORED_THESIS_COLLECTIONS = ["cern"]



udc_pattern = r'\b\d+(?:\.\d+)*-?\d*(?:\.\d+)*\b'


FILE_SUBFORMATS_TO_DROP = ["pdfa", "unstamped"]
