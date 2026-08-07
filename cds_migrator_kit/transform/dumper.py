# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM MARC XML dumper module."""
import logging

import arrow
from cds_dojson.exceptions import ModelMissingException, MultipleModelsException
from cds_dojson.marc21.utils import create_record
from cds_dojson.matcher import _load_models

from cds_migrator_kit.errors import MultipleModelsMatched
from cds_migrator_kit.transform import migrator_marc21
from cds_migrator_kit.transform.errors import LossyConversion

cli_logger = logging.getLogger("migrator")


class CDSRecordDump:
    """CDS record dump class."""

    def __init__(
        self,
        data,
        source_type="marcxml",
        latest_only=True,
        dojson_model=migrator_marc21,
        raise_on_missing_rules=True,
        preferred_model=None,
    ):
        """Initialize."""
        self.data = data
        self.source_type = source_type
        self.latest_only = latest_only
        self.dojson_model = dojson_model
        self.latest_revision = None
        self.files = None
        self.raise_on_missing_rules = raise_on_missing_rules
        self.preferred_model = preferred_model
        self.multiple_models_warning = None

    @property
    def first_created(self):
        """Get first record creation date."""
        # modification datetime of first revision is the creation date of the whole record
        # this assumption is based on the hstRECORD dump from invenio-migrator module
        # for older records first revision is not the creation of record
        # so we added creation_date field to dump and it's getting it from bibrec
        # https://github.com/inveniosoftware/invenio-migrator/blob/master/invenio_migrator/legacy/records.py#L216
        return self.data["creation_date"]

    def prepare_revisions(self):
        """Prepare revisions."""
        self.latest_revision = self._prepare_revision(self.data["record"][-1])

    def prepare_files(self):
        """Get files from data dump."""
        # Prepare files
        files = {}
        for f in self.data["files"]:
            k = f["full_name"]
            if k not in files:
                files[k] = []
            files[k].append(f)

        # Sort versions
        for k in files.keys():
            files[k].sort(key=lambda x: x["version"])

        self.files = files

    def _resolve_preferred_model(self, exc_message):
        """Resolve to the preferred model by name when multiple models match.

        Returns the model instance if found, otherwise None.
        """
        models = _load_models(self.dojson_model.entry_point_models)
        for name, model, _ in models:
            if name == self.preferred_model:
                return model
        return None

    def _prepare_revision(self, data):
        timestamp = arrow.get(data["modification_datetime"]).datetime

        marc_record = create_record(data["marcxml"])

        resolved_model = None
        try:
            json_converted_record = self.dojson_model.do(marc_record)
        except MultipleModelsException as e:
            if self.preferred_model:
                resolved_model = self._resolve_preferred_model(str(e))
                if resolved_model is None:
                    raise MultipleModelsMatched(
                        message=f"preferred_model '{self.preferred_model}' not found among matched models. {e}"
                    )
                json_converted_record = resolved_model.do(marc_record)
                self.multiple_models_warning = MultipleModelsMatched(
                    message=str(e),
                    priority="warning",
                )
            else:
                raise MultipleModelsMatched(str(e))
        except ModelMissingException as e:
            raise MultipleModelsMatched(str(e))

        # Use the resolved model for missing() to avoid re-triggering the matcher
        missing_checker = resolved_model if resolved_model else self.dojson_model
        missing = missing_checker.missing(marc_record)
        if missing and self.raise_on_missing_rules:
            cli_logger.warning(missing)
            raise LossyConversion(missing=missing)
        return timestamp, json_converted_record
