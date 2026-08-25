# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Validates an EP committee approval request's legacy history."""
from datetime import datetime, timezone

from cds_rdm.requests.committee_approval import APPRN_PID_TYPE
from invenio_access.permissions import system_identity
from invenio_accounts.models import User
from invenio_pidstore.models import PersistentIdentifier
from invenio_requests.proxies import current_requests_service

from cds_migrator_kit.errors import ManualImportRequired, UnexpectedValue

EP_APPROVAL_WAITING_STATUS = "waiting"
EP_APPROVAL_APPROVED_STATUS = "approved"
EP_APPROVAL_REVIEWING_STATUS = "reviewing"


class ApprovalRequest:
    """Validates an EP committee approval request's legacy history.

    The "build" counterpart of the EP-approval split - parses and validates
    the ``ep_approval`` MARC history (waiting/reviewing/approved entries)
    and checks for a pre-existing request/PID, computing everything
    ``ApprovalRequestLoad`` needs before it creates anything. Mirrors
    ``RecordParent``/``RecordRequest`` on the transform side: computation
    and read-only DB lookups only, no writes - see ``ApprovalRequestLoad``
    for the persistence half.
    """

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

        self.resolve_user_by_email(waiting.get("submitted_by"), "submitter")
        self.resolve_user_by_email(approved.get("submitted_by"), "approver")

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

    @staticmethod
    def resolve_user_by_email(email, role):
        """Resolve the user by email.

        Public (not ``_``-prefixed) since ``ApprovalRequestLoad`` also calls
        it, the way ``RecordParent.resolve_grants()`` is called from
        ``ParentLoad``.
        """
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
