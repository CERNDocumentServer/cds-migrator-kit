# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM EP approval request validation and creation."""

from datetime import datetime, timezone

from cds_rdm.requests.committee_approval import APPRN_PID_TYPE, CommitteeApprovalRequest
from flask import current_app
from invenio_access.permissions import system_identity
from invenio_accounts.models import User
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

EP_APPROVAL_WAITING_STATUS = "waiting"
EP_APPROVAL_APPROVED_STATUS = "approved"
EP_APPROVAL_REVIEWING_STATUS = "reviewing"


class ApprovalRequest:
    """Validate and create a migrated EP committee approval request."""

    def __init__(
        self,
        ep_approval,
        legacy_recid,
        title=None,
        resource_type=None,
        dry_run=False,
    ):
        self.ep_approval = ep_approval
        self.legacy_recid = legacy_recid
        self.title = title
        self.resource_type = resource_type
        self.dry_run = dry_run
        self.waiting_entry = None
        self.reviewing_entry = None
        self.approved_entry = None
        self.report_number = None
        self.approved_at = None

    def validate(self):
        """Validate EP approval data before creating any records."""
        waiting_entry, approved_entry, reviewing, report_number = self._parse_history()

        existing = self._existing_request()
        if existing:
            raise ManualImportRequired(
                message=f"EP approval request {existing['id']} already exists",
                stage="load",
                priority="critical",
            )
        if self._exists_apprn_pid(report_number):
            raise ManualImportRequired(
                message=f"APPRN PID {report_number} already exists",
                stage="load",
                priority="critical",
            )

        self.waiting_entry = waiting_entry
        self.approved_entry = approved_entry
        self.reviewing_entry = reviewing
        self.report_number = report_number

    def create(self, restricted_record_state, uow=None):
        """Create and approve EP approval request after restricted record exists.

        If ``uow`` is provided, the request is registered on it without
        committing, so the caller can group this atomically with other
        operations (e.g. the public record creation and linking).
        """
        if self.dry_run:
            return

        if not restricted_record_state:
            raise UnexpectedValue(
                message="Restricted record is required for EP approval.",
                stage="load",
                recid=self.legacy_recid,
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

    def _parse_history(self):
        """Return waiting/approved history entries and the report number."""
        if len(self.ep_approval) > 3:
            raise UnexpectedValue(
                message="EP approval history has more/less than 3 entries",
                stage="load",
                priority="critical",
            )
        history = self.ep_approval or []
        waiting = next(
            (
                item
                for item in history
                if item.get("status") == EP_APPROVAL_WAITING_STATUS
            ),
            None,
        )
        reviewing = next(
            (
                item
                for item in history
                if item.get("status") == EP_APPROVAL_REVIEWING_STATUS
            ),
            None,
        )
        approved = next(
            (
                item
                for item in history
                if item.get("status") == EP_APPROVAL_APPROVED_STATUS
            ),
            None,
        )
        if not waiting:
            raise UnexpectedValue(
                message="EP approval history has no waiting entry",
                stage="load",
                priority="critical",
            )
        if not approved:
            raise UnexpectedValue(
                message="EP approval history has no approved entry",
                stage="load",
                priority="critical",
            )

        report_number = approved.get("ep_report_number")
        if not report_number:
            raise UnexpectedValue(
                message="EP approval approved entry is missing ep_report_number",
                stage="load",
                priority="critical",
            )
        if waiting.get("ep_report_number") != report_number:
            raise UnexpectedValue(
                message=(
                    "EP approval waiting entry has different ep_report_number "
                    "than approved entry"
                ),
                stage="load",
                priority="critical",
            )

        self._resolve_user_by_email(waiting.get("submitted_by"), "submitter")
        self._resolve_user_by_email(approved.get("submitted_by"), "approver")

        waiting_deadline = self.parse_legacy_datetime(waiting.get("deadline"))
        approved_date = self.parse_legacy_datetime(approved.get("date"))
        self.approved_at = approved_date
        created_at = self.parse_legacy_datetime(waiting.get("date"))
        if not created_at or not approved_date or not waiting_deadline:
            raise UnexpectedValue(
                message="EP approval history has missing timestamps",
                stage="load",
                priority="critical",
            )

        return waiting, approved, reviewing, report_number

    @staticmethod
    def parse_legacy_datetime(value):
        """Parse legacy EP approval timestamps into timezone-aware datetimes."""
        if not value:
            return None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        return None

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

    @staticmethod
    def _resolve_user_by_email(email, role):
        """Resolve the user by email."""
        if not email:
            raise UnexpectedValue(
                message=f"EP approval {role} email is missing",
                stage="load",
                priority="critical",
            )
        user = User.query.filter_by(email=email).one_or_none()
        if not user:
            raise UnexpectedValue(
                message=f"EP approval {role} user not found: {email}",
                stage="load",
                priority="critical",
            )
        return {"user": str(user.id)}

    def _existing_request(self):
        """Check if the EP approval request already exists."""
        number = f"lrecid:{self.legacy_recid}:ep-approval"
        results = current_requests_service.search(
            system_identity,
            params={"q": f'number:"{number}"', "size": 1},
        )
        hits = list(results.hits)
        return hits[0] if hits else None

    @staticmethod
    def _exists_apprn_pid(report_number):
        """Check if the APPRN PID already exists."""
        existing = PersistentIdentifier.query.filter_by(
            pid_type=APPRN_PID_TYPE,
            pid_value=report_number,
        ).one_or_none()
        return bool(existing)

    def _mint_apprn_pid(self, restricted_version_uuid):
        """Mint the APPRN PID."""
        try:
            PersistentIdentifier.create(
                pid_type=APPRN_PID_TYPE,
                pid_value=self.report_number,
                object_type="rec",
                object_uuid=str(restricted_version_uuid),
                status=PIDStatus.REGISTERED,
            )
        except PIDAlreadyExists:
            raise ManualImportRequired(
                message=f"APPRN PID {self.report_number} already exists",
                stage="load",
                priority="critical",
            )

    def _create_accept_log_event(self, request, uow):
        """Create the accept timeline event with the legacy approver as created_by."""
        approver_ref = self._resolve_user_by_email(
            self.approved_entry.get("submitted_by"),
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

        approved_at = self.parse_legacy_datetime(self.approved_entry.get("date"))
        if approved_at:
            event.model.created = approved_at

        uow.register(RecordCommitOp(event, indexer=current_events_service.indexer))


    def _create_reviewing_log_event(self, request, uow):
        """Create the reviewers-updated timeline event with the legacy reviewer as created_by."""
        if not self.reviewing_entry:
            return

        reviewer_ref = self._resolve_user_by_email(
            self.reviewing_entry.get("submitted_by"),
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
                    "content": self.reviewing_entry.get("description", ""),
                    "reviewers": [reviewer_ref],
                }
            }
        )
        event.created_by = ResolverRegistry.resolve_entity_proxy(
            reviewer_ref, raise_=True
        )

        reviewing_at = self.parse_legacy_datetime(self.reviewing_entry.get("date"))
        if reviewing_at:
            event.model.created = reviewing_at

        uow.register(RecordCommitOp(event, indexer=current_events_service.indexer))

    def _apply_approved_entry(self, request, uow):
        """Update an existing request to accepted using the legacy approved entry."""
        payload = dict(request.get("payload") or {})
        payload["approved_report_number"] = self.report_number
        request["payload"] = payload
        request.status = "accepted"

        approved_at = self.parse_legacy_datetime(self.approved_entry.get("date"))
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
        expires_at = self.parse_legacy_datetime(self.waiting_entry.get("deadline"))
        referee_group = self._get_referee_group(restricted_parent)

        request_item = current_requests_service.create(
            system_identity,
            data={
                "title": f'EP approval for "{self.title}"',
                "payload": {},
            },
            request_type=CommitteeApprovalRequest,
            receiver={"group": referee_group},
            creator=self._resolve_user_by_email(
                self.waiting_entry.get("submitted_by"), "submitter"
            ),
            topic={"record": restricted_recid},
            expires_at=expires_at,
            uow=uow,
        )
        request = request_item._record
        request.number = f"lrecid:{self.legacy_recid}:ep-approval"
        request.status = "submitted"

        submitted_at = self.parse_legacy_datetime(self.waiting_entry.get("date"))
        if submitted_at:
            request.model.created = submitted_at

        self._create_reviewing_log_event(request, uow)
        self._apply_approved_entry(request, uow)

        uow.register(
            RecordCommitOp(request, indexer=current_requests_service.indexer)
        )
