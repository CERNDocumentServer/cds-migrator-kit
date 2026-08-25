# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Creates and approves a migrated EP committee approval request."""
from cds_rdm.requests.committee_approval import APPRN_PID_TYPE, CommitteeApprovalRequest
from flask import current_app
from invenio_access.permissions import system_identity
from invenio_db.uow import UnitOfWork
from invenio_pidstore.errors import PIDAlreadyExists
from invenio_pidstore.models import PersistentIdentifier, PIDStatus
from invenio_rdm_records.records.api import RDMParent
from invenio_records_resources.services.uow import RecordCommitOp
from invenio_requests.customizations.event_types import (
    LogEventType,
    ReviewersUpdatedType,
)
from invenio_requests.proxies import current_events_service, current_requests_service
from invenio_requests.resolvers.registry import ResolverRegistry

from cds_migrator_kit.errors import ManualImportRequired, UnexpectedValue

from .approval_request import ApprovalRequest


class ApprovalRequestLoad:
    """Creates and approves a migrated EP committee approval request.

    The "persist" counterpart of ``ApprovalRequest`` - takes the already-
    validated entity and drives the request-service calls, committee-
    approval PID minting, and timeline events that materialize it. Mirrors
    ``RequestLoad``.
    """

    def __init__(self, approval_request: ApprovalRequest):
        """Constructor.

        :param approval_request: the ``ApprovalRequest`` for this entry,
            already ``.validate()``-d.
        """
        self.approval_request = approval_request

    def create(self, restricted_record_state, uow=None):
        """Create and approve EP approval request after restricted record exists.

        If ``uow`` is provided, the request is registered on it without
        committing, so the caller can group this atomically with other
        operations (e.g. the public record creation and linking).
        """
        approval_request = self.approval_request
        if approval_request.dry_run:
            return

        if not restricted_record_state:
            raise UnexpectedValue(
                message="Restricted record is required for EP approval.",
                stage="load",
                recid=approval_request.legacy_recid,
                priority="critical",
            )

        restricted_recid = restricted_record_state["latest_version"]
        restricted_parent = RDMParent.get_record(
            restricted_record_state["parent_object_uuid"]
        )
        self._create_request(
            restricted_recid,
            restricted_parent,
            uow=uow,
        )
        self._mint_apprn_pid(restricted_record_state["latest_version_object_uuid"])

    def _get_referee_group(self, restricted_parent):
        """Get the EP approval referee group from the restricted record."""
        default_community_id = restricted_parent.get("communities", {}).get("default")
        if not default_community_id:
            raise UnexpectedValue(
                message="Restricted record has no default community for EP approval",
                stage="load",
                priority="critical",
            )
        ep_config = current_app.config.get(
            "CDS_COMMITTEE_APPROVAL_COMMUNITIES", {}
        ).get(default_community_id)
        if not ep_config:
            raise UnexpectedValue(
                message=(
                    f"Community {default_community_id} is not enrolled in "
                    "CDS_COMMITTEE_APPROVAL_COMMUNITIES"
                ),
                stage="load",
                priority="critical",
            )
        return ep_config["referee_group"]

    def _mint_apprn_pid(self, restricted_version_uuid):
        """Mint the APPRN PID."""
        report_number = self.approval_request.report_number
        try:
            PersistentIdentifier.create(
                pid_type=APPRN_PID_TYPE,
                pid_value=report_number,
                object_type="rec",
                object_uuid=str(restricted_version_uuid),
                status=PIDStatus.REGISTERED,
            )
        except PIDAlreadyExists:
            raise ManualImportRequired(
                message=f"APPRN PID {report_number} already exists",
                stage="load",
                priority="critical",
            )

    def _create_accept_log_event(self, request, uow):
        """Create the accept timeline event with the legacy approver as created_by."""
        approval_request = self.approval_request
        approver_ref = approval_request.resolve_user_by_email(
            approval_request.approved_entry.get("submitted_by"),
            "approver",
        )

        event = current_events_service.record_cls.create(
            {},
            request=request.model,
            request_id=str(request.id),
            type=LogEventType,
        )
        event.update({"payload": {"event": "accepted"}})
        event.created_by = ResolverRegistry.resolve_entity_proxy(
            approver_ref, raise_=True
        )

        approved_at = approval_request.parse_legacy_datetime(
            approval_request.approved_entry.get("date")
        )
        if approved_at:
            event.model.created = approved_at

        uow.register(RecordCommitOp(event, indexer=current_events_service.indexer))

    def _create_reviewing_log_event(self, request, uow):
        """Create the reviewers-updated timeline event with the legacy reviewer as created_by."""
        approval_request = self.approval_request
        if not approval_request.reviewing_entry:
            return

        reviewer_ref = approval_request.resolve_user_by_email(
            approval_request.reviewing_entry.get("submitted_by"),
            "reviewer",
        )
        request.reviewers = [reviewer_ref]

        event = current_events_service.record_cls.create(
            {},
            request=request.model,
            request_id=str(request.id),
            type=ReviewersUpdatedType,
        )
        event.update(
            {
                "payload": {
                    "event": "reviewers_updated",
                    "content": approval_request.reviewing_entry.get("description", ""),
                    "reviewers": [reviewer_ref],
                }
            }
        )
        event.created_by = ResolverRegistry.resolve_entity_proxy(
            reviewer_ref, raise_=True
        )

        reviewing_at = approval_request.parse_legacy_datetime(
            approval_request.reviewing_entry.get("date")
        )
        if reviewing_at:
            event.model.created = reviewing_at

        uow.register(RecordCommitOp(event, indexer=current_events_service.indexer))

    def _apply_approved_entry(self, request, uow):
        """Update an existing request to accepted using the legacy approved entry."""
        approval_request = self.approval_request
        payload = dict(request.get("payload") or {})
        payload["approved_report_number"] = approval_request.report_number
        request["payload"] = payload
        request.status = "accepted"

        approved_at = approval_request.parse_legacy_datetime(
            approval_request.approved_entry.get("date")
        )
        if approved_at:
            request.model.updated = approved_at

        self._create_accept_log_event(request, uow)

    def _create_request(self, restricted_recid, restricted_parent, uow=None):
        """Create request from waiting entry, then update it with approved entry.

        If ``uow`` is provided, it is used as-is and left uncommitted for the
        caller to commit; otherwise a unit of work is created and committed
        here.
        """
        if uow is not None:
            self._build_request(restricted_recid, restricted_parent, uow)
            return

        with UnitOfWork() as inner_uow:
            self._build_request(restricted_recid, restricted_parent, inner_uow)
            inner_uow.commit()

    def _build_request(self, restricted_recid, restricted_parent, uow):
        """Register the request creation and its updates on the given uow."""
        approval_request = self.approval_request
        expires_at = approval_request.parse_legacy_datetime(
            approval_request.waiting_entry.get("deadline")
        )
        referee_group = self._get_referee_group(restricted_parent)

        request_item = current_requests_service.create(
            system_identity,
            data={
                "title": f'EP approval for "{approval_request.title}"',
                "payload": {},
            },
            request_type=CommitteeApprovalRequest,
            receiver={"group": referee_group},
            creator=approval_request.resolve_user_by_email(
                approval_request.waiting_entry.get("submitted_by"), "submitter"
            ),
            topic={"record": restricted_recid},
            expires_at=expires_at,
            uow=uow,
        )
        request = request_item._record
        request.number = f"lrecid:{approval_request.legacy_recid}:ep-approval"
        request.status = "submitted"

        submitted_at = approval_request.parse_legacy_datetime(
            approval_request.waiting_entry.get("date")
        )
        if submitted_at:
            request.model.created = submitted_at

        self._create_reviewing_log_event(request, uow)
        self._apply_approved_entry(request, uow)

        uow.register(
            RecordCommitOp(request, indexer=current_requests_service.indexer)
        )
