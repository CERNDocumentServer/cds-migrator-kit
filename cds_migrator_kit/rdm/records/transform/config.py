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
    "CDSRN",
    "APPRN",  # EP committee approval report numbers e.g. CERN-EP-2026-001
]

PID_SCHEMES_TO_STORE_IN_RELATED_IDENTIFIERS = [
    "ARXIV",
    "HDL",
    "HAL",
    "HANDLE",
    "URN",
    "INIS",
    "CERCER",
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
    "HOLALE",
    "UNCOVER",
    "INTINT",
    "KEKSCAN",
    "ALIALI",
    "AIS.FOUNDATION.EXPERIMENTS",
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
    "medline",
    "aip",
    "jacow",
]
KEYWORD_SCHEMES_TO_DROP = ["proquest", "disxa", "inspeq", "jinr"]

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

# Experiment IDs that live in the experiments MARC field but should be
# stored as studies (cern:studies) rather than experiments (cern:experiments).
EXPERIMENTS_AS_STUDIES = {
    "IS01",
    "IS01-1",
    "IS01-2",
    "IS01-3",
    "IS01-4",
    "IS01-5",
    "IS01-6",
    "IS01-7",
    "IS01-8",
    "IS01-9",
    "IS01-10",
    "IS01-11",
    "IS01-12",
    "IS01-13",
    "IS01-15",
    "IS01-16",
    "IS01-18",
    "IS01-20",
    "IS01-21",
    "IS01-22",
    "IS01-23",
    "IS01-24",
    "IS01-26",
    "IS01-28",
    "IS01-29",
    "IS01-31",
    "IS01-32",
    "IS01-35",
    "IS01-37",
    "IS01-38",
    "IS01-39",
    "IS01-40",
    "IS01-42",
    "IS01-43",
    "IS01-44",
    "IS01-45",
    "IS01-49",
    "IS01-51",
    "IS01-B",
    "IS01-C",
    "IS01-D",
    "IS01-E",
    "IS250",
    # "IS",
    # "IS02",
    "IS142",
}

# Legacy experiment names remapped to vocabulary ids before lookup
EXPERIMENT_ALIASES = {
    "t2k": "RE13",
    "antares": "RE6",
    "dirac": "PS212",
    "dirac ps212": "PS212",
    "harp ps214": "PS214",
    "harp": "PS214",
    "dampe": "RE29",
    "compass": "NA58",
    "compass na58": "NA58",
    "cern isolde": "ISOLDE",
    "mistral is346": "IS346",
    "mistral is373": "IS373",
    "witch is433": "IS433",
    "isoltrap": "IS394",
    "is485 (rex-isolde)": "IS485",
}

# 693__e values that are keywords rather than real experiments (curated as
# "mots clef"), mapped to subjects instead of cern:experiments
EXPERIMENTS_AS_SUBJECTS = ["d3", "r104", "r105a"]

# Public research publication resource types that are auto-included in the CERN Research community.
CERN_SCIENTIFIC_RESOURCE_TYPES = {
    "publication-dissertation",  # Already included by the migrator for thesis records.
    "publication-book",
    "publication-section",
    "publication-conferencepaper",
    "publication-conferenceproceeding",
    "publication-conferencenote",
    "publication-journal",
    "publication-article",
    "publication-preprint",
    "publication-report",
    "publication-technicalnote",
    "publication-note",
    "publication",
}
