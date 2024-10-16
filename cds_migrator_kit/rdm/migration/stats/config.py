import os


# Collection ids
RECID_LIST_FILE = os.path.join(
    "<path>",
    "ssn_recids.json",
)


######## Logging ###########
ROOT_PATH = os.path.join(
    "<path>",
    "stats",
)

####### Search ##############
SRC_SEARCH_URL = "https://os-cds-legacy.cern.ch:443/os"
SRC_SEARCH_AUTH = ("", "")

SRC_SEARCH_SIZE = 5000
SRC_SEARCH_SCROLL = "1h"
DEST_SEARCH_INDEX_PREFIX = "cds-rdm-events-stats"
DEST_SEARCH_URL = "http://127.0.0.1:9200/"
DEST_SEARCH_AUTH = ("", "")


######## Statistics ###########

_QUERY_VIEWS = {
    "query": {
        "bool": {
            "must": [
                {"match": {"id_bibrec": "<recid>"}},
                {"match": {"event_type": "<type>"}},
            ]
        }
    }
}

LEGACY_TO_RDM_EVENTS_MAP = {
    "events.pageviews": {
        "type": "record-view",
        "query": _QUERY_VIEWS,
    },
    "events.downloads": {
        "type": "file-download",
        "query": _QUERY_VIEWS,
    },
}

EVENT_TYPES = ["events.pageviews", "events.downloads"]

LEGACY_INDICES = [
    "cds-2004",
    "cds-2005",
    "cds-2006",
    "cds-2007",
    "cds-2008",
    "cds-2009",
    "cds-2010",
    "cds-2011",
    "cds-2012",
    "cds-2013",
    "cds-2014",
    "cds-2015",
    "cds-2016",
    "cds-2017",
    "cds-2018",
    "cds-2019",
    "cds-2020",
    "cds-2021",
    "cds-2022",
    "cds-2023",
    "cds-2024",
]
