# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM transform config module."""

# filters out PIDs which we don't migrate
PIDS_SCHEMES_TO_DROP = []
# validates allowed schemes
PIDS_SCHEMES_ALLOWED = ["DOI"]

# stores the identifiers found in PIDs field in the alternative identifiers instead
PID_SCHEMES_TO_STORE_IN_IDENTIFIERS = [
    "ARXIV",
    "HDL",
    "HAL" "HANDLE",
    "URN",
    "INIS",
    "CERCER",
    "CDSRN",
]

IDENTIFIERS_SCHEMES_TO_DROP = [
    "SPIRES",
    "OSTI",
    "SLAC",
    "PROQUEST",
    "INSPEC",
    "NNT",
    "DESY",
    "WAI01",
    "KEK",
    "ATLATL",
]
IDENTIFIERS_VALUES_TO_DROP = "oai:arXiv.org"

CONTROLLED_SUBJECTS_SCHEMES = ["szgecern", "cern", "cds"]

RECOGNISED_KEYWORD_SCHEMES = [
    "author",
    "cms",
    "arxiv",
    "inspire",
    "spr",
    "inis",
    "lanl eds",
    "in2p3",
    "eucard",
    "inspec",
    "desy",
]
KEYWORD_SCHEMES_TO_DROP = ["proquest", "disxa"]

ALLOWED_THESIS_COLLECTIONS = [
    "thesis",
    "publcms",
    "book",
    "aida",
    "eucard",
    "eucard2",
    "article",
    "preprint",  #
    "core",  # coming from inspire
    "clinot",
    "clicnote",
    "intnote",
    "aidathesis",
    "bookchapter",
    "eucardboo",
    "eucarddis",
    "eucard2thesis",
    "eucardacad",
    "eucard2mon",
    "cern internal note",
    "publsl",
    "yellow report",
    "report",
    "publats",
    "fermilab",
    "publit",
    "publab",
    "intnotelhcbpubl",
    "intnoteatlaspubl",
    "publlhcproj",
    "lhcprojnote",
    "aida-2020",
    "publatlas",
    "publlhcb",
    "aida-2020thesis",
]

ALLOWED_DOCUMENT_TAGS = [
    "thesis",
    "aida",
    "eucard2",
    "clinot",
    "book",
    "eucardacad",
    "cern",
    "clicnote",
    "note",
    "bookchapter",
    "legserlib",
    "e-learning",
    "indico",
    "intnotelhcbpubl",
    "report",  # these are all yellow reports in thesis
    "preprint",
    "slintnote",  # only 733805
    "aida-2020",
    "article",
    "intnoteitpubl",
    "intnoteatlaspubl",  # 455788
    # "arc012301",  # 1655788
    # "cernitarchive",  # 1655788
]

FORMER_COLLECTION_TAGS_TO_KEEP = [
    "aida",
    "eucard2",
    "eucard",
    "clinot",
    "book",
    "eucardacad",
    "clicnote",
    "note",
    "intnotelhcbpubl",
    "report",
    "slintnote",
    "aida-2020",
    "intnoteitpubl",
    "intnoteatlaspubl",
    # "arc012301",  # 1655788
    # "cernitarchive",  # 1655788
]

IGNORED_THESIS_COLLECTIONS = ["cern", "preprint", "clinot"]


udc_pattern = r"\b\d+(?:\.\d+)*-?\d*(?:\.\d+)*\b"


FILE_SUBFORMATS_TO_DROP = ["pdfa", "unstamped"]
