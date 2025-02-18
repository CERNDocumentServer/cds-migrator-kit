# -*- coding: utf-8 -*-
#
# Copyright (C) 2024 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos migration load module."""
from invenio_rdm_migrator.load.base import Load


class CDSVideosLoad(Load):
    """CDS-Videos Load."""

    def __init__(
        self,
        db_uri,
        data_dir,
        tmp_dir,
        entries=None,
        dry_run=False,
    ):
        """Constructor."""
        self.dry_run = dry_run

    def _prepare(self, entry):
        """Prepare the record."""
        pass

    def _load(self, entry):
        """Use the services to load the entries."""
        pass

    def _cleanup(self, *args, **kwargs):
        """Cleanup the entries."""
        pass
