# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos identifiers migration module."""

import json
import os
import re

import requests
from flask import current_app

_legacy_id_map_cache = None


def load_legacy_ids(legacy_ids_path):
    """Load legacy ID mappings."""
    global _legacy_id_map_cache
    if _legacy_id_map_cache is None:
        if os.path.exists(legacy_ids_path):
            with open(legacy_ids_path, "r") as f:
                _legacy_id_map_cache = json.load(f)
        else:
            _legacy_id_map_cache = {}
    return _legacy_id_map_cache


def get_new_indico_id(legacy_id):
    """Return the new Indico ID from the legacy mapping."""
    path = current_app.config.get("CDS_MIGRATOR_KIT_INDICO_LEGACY_IDS")
    if legacy_id.startswith("0") or legacy_id.startswith("a"):
        legacy_mapping = load_legacy_ids(path)
        return legacy_mapping.get(legacy_id)
    return None


def follow_redirect(u, timeout=2):
    """Follow HTTP redirects and return the final URL."""
    try:
        response = requests.head(u, allow_redirects=True, timeout=timeout)
        if response.status_code in (405, 403):
            raise requests.RequestException("HEAD not supported, try GET")
        return response.url if response.url != u else u
    except requests.RequestException:
        try:
            response = requests.get(u, allow_redirects=True, timeout=timeout)
            return response.url if response.url != u else u
        except requests.RequestException:
            return u  # Fall back to original


def transform_legacy_urls(url, type=""):
    """
    Transform legacy Indico and CDS URLs.

    - Legacy CDS links:
      Replace cdsweb with cds

    - Legacy Indico links:
      - If in this format: http://agenda.cern.ch/tools/SSLPdisplay.php?... => drop
      - If in this format:
        http://indico.cern.ch/contributionDisplay.py?confId=a035925&contribId=s11t1
        or
        http://indico.cern.ch/conferenceDisplay.py?confId=a035925
        Then:
          * extract confId
          * get the new id
          * return transformed URL: https://indico.cern.ch/event/new_id
      - If not a legacy URL, return the same URL.
    """

    def transform_cds(u):
        return u.replace("cdsweb.cern.ch", "cds.cern.ch")

    def transform_indico(u):
        # Drop agenda.cern.ch format
        if re.match(r"http://agenda\.cern\.ch/tools/SSLPdisplay\.php\?", u):
            return ""
        # Get the new indico id
        match = re.search(r"(?:confId|ida)=([a-zA-Z0-9]+)", u)
        if match:
            legacy_id = match.group(1)
            new_id = get_new_indico_id(legacy_id)
            if new_id:
                return f"https://indico.cern.ch/event/{new_id}"
            else:
                # Try to get redirection
                return follow_redirect(u)
        return u

    # Apply transformation based on type
    if type == "cds" or "cds" in url:
        return transform_cds(url)
    elif type == "indico" or "indico" in url or "agenda" in url:
        return transform_indico(url)
    else:
        # Try both
        url = transform_cds(url)
        return transform_indico(url)


def is_doi(identifier):
    """Returns identifier is a valid DOI."""
    doi_pattern = r"^10\.\d{4,9}/\S+$"
    return bool(re.match(doi_pattern, identifier))
