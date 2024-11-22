# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2023 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Permission policy."""

from invenio_communities.permissions import CommunityPermissionPolicy
from invenio_preservation_sync.services.permissions import (
    DefaultPreservationInfoPermissionPolicy,
)
from invenio_rdm_records.services.generators import IfNewRecord, IfRecordDeleted
from invenio_rdm_records.services.permissions import RDMRecordPermissionPolicy
from invenio_records_permissions.generators import IfConfig, SystemProcess

from cds_rdm.permissions import CDSRDMRecordPermissionPolicy


class CDSRDMMigrationRecordPermissionPolicy(CDSRDMRecordPermissionPolicy):
    """Record permission policy for records migration.

    We need to override this so we can allow system process to manage files.
    """

    can_manage_files = [
        IfConfig(
            "RDM_ALLOW_METADATA_ONLY_RECORDS",
            then_=[
                IfNewRecord(
                    then_=CDSRDMRecordPermissionPolicy.can_authenticated,
                    else_=CDSRDMRecordPermissionPolicy.can_review,
                )
            ],
            else_=[
                SystemProcess()
            ],  # needed for migrating records with no files as metadata-only
        ),
    ]
