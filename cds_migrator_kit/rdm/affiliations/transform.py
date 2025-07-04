# -*- coding: utf-8 -*-
#
# Copyright (C) 2022-2025 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM transform step module."""
import logging
from copy import deepcopy

import requests
from invenio_rdm_migrator.streams.records.transform import RDMRecordTransform

from cds_migrator_kit.transform.dumper import CDSRecordDump

from . import affiliations_migrator_marc21
from .log import AffiliationsLogger

cli_logger = logging.getLogger("migrator")


def affiliations_search(affiliation_name):
    """Query ROR organizations API to normalize affiliations."""

    def get_ror_affiliation(affiliation):
        """Query ROR organizations API to normalize affiliations."""
        assert affiliation

        url = "https://api.ror.org/organizations"
        params = {"affiliation": affiliation}

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            items = response.json().get("items")
            if items:
                for item in items:
                    if item["chosen"] is True:
                        return (True, item)
            return (False, items)
        except requests.exceptions.HTTPError as http_err:
            cli_logger.exception(http_err)
        except Exception as err:
            cli_logger.exception(err)

    (chosen, affiliation) = get_ror_affiliation(affiliation_name)

    return (chosen, affiliation)


class CDSToRDMAffiliationTransform(RDMRecordTransform):
    """CDSToRDMAffiliationTransform."""

    def __init__(
        self,
        dry_run=False,
    ):
        """Constructor."""
        self.dry_run = dry_run
        super().__init__()

    def _affiliations(self, json_entry, key):
        _creators = deepcopy(json_entry.get(key, []))
        _creators = list(filter(lambda x: x is not None, _creators))
        _affiliations = []

        for creator in _creators:
            affiliations = creator.get("affiliations", [])

            for affiliation_name in affiliations:
                if not affiliation_name:
                    continue

                _affiliation = {
                    "original_input": affiliation_name,
                }

                (chosen, match_or_suggestions) = affiliations_search(affiliation_name)

                if chosen:
                    _affiliation.update(
                        {
                            "ror_exact_match": match_or_suggestions["organization"][
                                "id"
                            ],
                            "ror_match_info": match_or_suggestions,
                        }
                    )
                else:
                    if match_or_suggestions:
                        for not_exact_match in match_or_suggestions:
                            if not_exact_match["score"] >= 0.9:
                                _affiliation.update(
                                    {
                                        "ror_not_exact_match": not_exact_match[
                                            "organization"
                                        ]["id"],
                                        "ror_match_info": not_exact_match,
                                    }
                                )
                                break
                _affiliations.append(_affiliation)

        return _affiliations

    def _transform(self, entry):
        """Transform a single entry."""
        # creates the output structure for load step
        try:
            record_dump = CDSRecordDump(
                entry,
                dojson_model=affiliations_migrator_marc21,
                raise_on_missing_rules=False,
            )
            record_dump.prepare_revisions()
        except Exception as e:
            logger = AffiliationsLogger.get_logger()
            logger.error(str(e))

        timestamp, json_data = record_dump.latest_revision
        try:
            return {
                "creators_affiliations": self._affiliations(json_data, "creators"),
                "contributors_affiliations": self._affiliations(
                    json_data, "contributors"
                ),
            }
        except Exception as e:
            cli_logger.exception(e)

    def _draft(self, entry):
        return None

    def _parent(self, entry):
        return None

    def _record(self, entry):
        return None
