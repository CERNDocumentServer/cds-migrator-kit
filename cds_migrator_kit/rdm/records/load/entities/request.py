# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Creates a community-inclusion request from a ``RecordRequest``."""
import datetime

from invenio_access.permissions import system_identity
from invenio_rdm_records.requests import CommunitySubmission
from invenio_records_resources.services.uow import RecordCommitOp
from invenio_requests.customizations.event_types import LogEventType
from invenio_requests.proxies import current_events_service, current_requests_service
from invenio_requests.records.models import RequestMetadata

from cds_migrator_kit.errors import ManualImportRequired
from cds_migrator_kit.rdm.records.transform.entities.migration import MigrationEntry
from cds_migrator_kit.rdm.records.transform.entities.request import RecordRequest


class RequestLoad:
    """Creates a community-inclusion request from a ``RecordRequest``.

    The load-side counterpart of ``RecordRequest`` - takes the already-built
    transform-side entity (reviewers resolved, ``request_data`` popped off
    ``dojson_entry``) and drives the request-service calls that materialize
    it once the record has been published - mirrors ``RecordLoad``/
    ``ParentLoad``.
    """

    def __init__(self, entry:MigrationEntry):
        """Constructor.

        :param record_request: the built ``RecordRequest`` for this entry
            (``MigrationEntry["_request_data"]``).
        :param legacy_recid: this record's legacy recid, used to build the
            request's idempotency ``number``.
        """
        self.record_request = entry.get("_request_data")
        self.legacy_recid = entry["record"].recid

    def load(self, records, create_inclusion_request, uow):
        """Create the community-inclusion request against the latest published record.

        :param records: this entry's published records in version order
            (``RecordLoad.load()``'s return value - service-level wrapper
            objects, as returned by ``current_rdm_records_service.publish()``);
            only the last (latest) one is used.
        :param create_inclusion_request: whether the current load run is
            configured to create community-inclusion requests.
        """
        if self.record_request and create_inclusion_request:
            self.create_submission_request(records[-1], uow)
        elif self.record_request.data and not create_inclusion_request:
            raise ManualImportRequired(
                message="Detected request data, enable the requests",
                field="validation",
                stage="load",
                recid=self.legacy_recid,
                priority="warning",
                subfield=None,
            )

    def create_submission_request(self, record, uow):
        """Create community inclusion request after publish.

        :param record: the published record - the service-level wrapper
            (e.g. the last item of ``load()``'s ``records``, as returned by
            ``current_rdm_records_service.publish()``), hence
            ``record._record.parent``/``record.id`` (not a bare
            ``record.parent``/``record.pid``).
        """
        request_data = self.record_request.data
        request_number = f"lrecid:{self.legacy_recid}"

        # Defensive/idempotency guard: skip if a request for this record was
        # already committed by a previous (partial) load attempt, otherwise
        # re-creating it would violate the unique constraint on `number`.
        if RequestMetadata.query.filter_by(number=request_number).first() is not None:
            return

        status = request_data.get("status", "accepted")
        reviewers = request_data.get("reviewers", [])

        created_at = datetime.datetime.fromisoformat(record["created"])

        parent = record._record.parent
        owner_id = parent.access.owned_by.owner_id
        community = parent.communities.default

        creator = {"user": str(owner_id)}
        receiver = {"community": str(community.id)}

        def create_event(request_model, payload, event_type, user):
            """Create and register a request event."""
            event = current_events_service.record_cls.create(
                {},
                request=request_model,
                request_id=str(request_model.id),
                type=event_type,
            )
            event.update(payload=payload)
            event.model.created = created_at
            event.created_by = {"user": str(user)}

            uow.register(RecordCommitOp(event, indexer=current_events_service.indexer))

        request_item = current_requests_service.create(
            system_identity,
            data={"title": record["metadata"]["title"]},
            request_type=CommunitySubmission,
            receiver=receiver,
            creator=creator,
            topic={"record": record.id},
            uow=uow,
        )

        request = request_item._record
        request.status = "submitted"
        request.number = request_number
        request.model.created = created_at

        if reviewers:
            request.reviewers = reviewers

        if status:
            request.status = status
            if status == "accepted":
                parent_to_request_relation = (
                    parent.communities._m2m_model_cls.query.filter_by(
                        record_id=parent.id, community_id=community.id
                    ).one()
                )
                parent_to_request_relation.request_id = request.id

            create_event(
                request.model,
                {"event": status},
                event_type=LogEventType,
                user="system",
            )

        uow.register(RecordCommitOp(request, indexer=current_requests_service.indexer))
