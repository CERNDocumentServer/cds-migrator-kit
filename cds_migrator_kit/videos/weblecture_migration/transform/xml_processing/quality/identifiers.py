# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos identifiers migration module."""

import json
import re


def load_json(file):
    """Load legacy ID mappings."""
    with open(file, "r") as f:
        return json.load(f)


def get_new_indico_id(legacy_id):
    """Return the new Indico ID from the legacy mapping."""
    if legacy_id.startswith("0") or legacy_id.startswith("a"):
        legacy_mapping = load_json(
            "cds_migrator_kit/videos/weblecture_migration/data/indico/indico-legacy-ids.json"
        )
        return legacy_mapping.get(legacy_id)
    return None


def get_redirection(url):
    """Return the new Indico ID from the legacy mapping."""
    redirections = load_json(
        "cds_migrator_kit/videos/weblecture_migration/data/indico/redirected_links.json"
    )
    return redirections.get(url)


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
                # Get the redirection
                redirection = get_redirection(u)
                if redirection:
                    return redirection
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
