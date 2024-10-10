import time

from copy import deepcopy
from opensearchpy import OpenSearch
from opensearchpy.exceptions import OpenSearchException

from .config import (
    SRC_SEARCH_URL,
    SRC_SEARCH_AUTH,
    SRC_SEARCH_SCROLL,
    SRC_SEARCH_SIZE,
    DEST_SEARCH_URL,
    DEST_SEARCH_AUTH,
    LEGACY_TO_RDM_EVENTS_MAP,
)

src_os_client = dest_os_client = OpenSearch(
    SRC_SEARCH_URL,
    http_auth=SRC_SEARCH_AUTH,
    use_ssl=True,  # set to True if your cluster is using HTTPS
    verify_certs=False,  # set to False if you do not want to verify SSL certificates
    ssl_show_warn=False,  # set to False to suppress SSL warnings)
)


# Initialize the OpenSearch client
dest_os_client = OpenSearch(
    DEST_SEARCH_URL,
    http_auth=DEST_SEARCH_AUTH,
    use_ssl=(
        True if "https" in DEST_SEARCH_URL else False
    ),  # set to True if your cluster is using HTTPS
    verify_certs=False,  # set to False if you do not want to verify SSL certificates
    ssl_show_warn=False,  # set to False to suppress SSL warnings)
)


def generate_query(doc_type, identifier):
    q = deepcopy(LEGACY_TO_RDM_EVENTS_MAP[doc_type]["query"])
    q["query"]["bool"]["must"][0]["match"]["id_bibrec"] = identifier
    q["query"]["bool"]["must"][1]["match"]["event_type"] = doc_type

    return q


def os_search(index, doc_type, identifier):
    ex = None
    i = 0
    q = generate_query(doc_type, identifier)
    while i < 10:
        try:
            return src_os_client.search(
                index=index,
                size=SRC_SEARCH_SIZE,
                scroll=SRC_SEARCH_SCROLL,
                body=q,
            )
        except OpenSearchException as _ex:
            i += 1
            ex = _ex
            time.sleep(10)
    raise ex


def os_scroll(scroll_id):
    ex = None
    i = 0
    while i < 10:
        try:
            return src_os_client.scroll(scroll_id=scroll_id, scroll=SRC_SEARCH_SCROLL)
        except OpenSearchException as _ex:
            i += 1
            ex = _ex
            time.sleep(10)
    raise ex
