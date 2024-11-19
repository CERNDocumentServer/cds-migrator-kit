# -*- coding: utf-8 -*-
#
# Copyright (C) 2024 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM migration load module."""
import logging
import os
import json
import psycopg2

from invenio_db import db
from invenio_rdm_migrator.load.base import Load
from sqlalchemy.exc import IntegrityError

from cds_rdm.legacy.models import CDSMigrationAffiliationMapping

from .log import AffiliationsLogger

logger = AffiliationsLogger.get_logger()


class CDSAffiliationsLoad(Load):
    """CDSAffiliationsLoad."""

    def __init__(
        self,
        dry_run=False,
    ):
        """Constructor."""
        self.dry_run = dry_run

    def _prepare(self, entry):
        """Prepare the record."""
        pass

    def _save_affiliation(self, affiliations):
        """."""

        for affiliation in affiliations:
            _affiliation_model = None
            _original_input = affiliation.pop("original_input")
            try:
                if affiliation.get("ror_exact_match"):
                    _affiliation_model = CDSMigrationAffiliationMapping(
                        legacy_affiliation_input=_original_input,
                        ror_exact_match=affiliation["ror_exact_match"],
                        ror_match_info=affiliation["ror_match_info"],
                    )
                elif affiliation.get("ror_not_exact_match"):
                    _affiliation_model = CDSMigrationAffiliationMapping(
                        legacy_affiliation_input=_original_input,
                        ror_not_exact_match=affiliation["ror_not_exact_match"],
                        ror_match_info=affiliation["ror_match_info"],
                    )
                else:
                    _affiliation_model = CDSMigrationAffiliationMapping(
                        legacy_affiliation_input=_original_input,
                    )
                db.session.add(_affiliation_model)
                db.session.commit()
            except IntegrityError as e:
                db.session.rollback()
                # We continue when the legacy affiliation input is already in the db
                if isinstance(e.orig, psycopg2.errors.UniqueViolation):
                    continue

    def _load(self, entry):
        """Use the services to load the entries."""
        if entry:
            creators_affiliations = entry["creators_affiliations"]
            contributors_affiliations = entry["contributors_affiliations"]
            try:
                self._save_affiliation(creators_affiliations)
                self._save_affiliation(contributors_affiliations)
            except Exception as ex:
                logger.error(ex)

    def _cleanup(self, *args, **kwargs):
        """Cleanup the entries."""
        pass
