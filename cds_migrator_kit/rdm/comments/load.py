# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM migration load module."""

import os

import arrow
from cds_rdm.legacy.resolver import get_pid_by_legacy_recid
from flask import current_app
from invenio_access.permissions import system_identity
from invenio_accounts.models import User
from invenio_db.uow import UnitOfWork
from invenio_rdm_migrator.load.base import Load
from invenio_rdm_records.proxies import current_rdm_records_service
from invenio_rdm_records.records.api import RDMParent
from invenio_rdm_records.requests import CommunitySubmission
from invenio_records_resources.services.uow import RecordCommitOp
from invenio_requests.customizations.event_types import CommentEventType, LogEventType
from invenio_requests.proxies import current_events_service, current_requests_service
from invenio_requests.records.api import RequestEventFormat
from invenio_requests.resolvers.registry import ResolverRegistry

from cds_migrator_kit.errors import ManualImportRequired
from cds_migrator_kit.rdm.comments.log import CommentsLogger

logger = CommentsLogger.get_logger()


class CDSCommentsLoad(Load):
    """CDSCommentsLoad."""

    LEGACY_REPLY_LINK_MAP = {}
    """Map of legacy reply ids to RDM comment ids."""

    def __init__(
        self,
        dirpath,
        dry_run=False,
    ):
        """Constructor."""
        self.dirpath = dirpath  # The directory path where the attached files are stored
        self.dry_run = dry_run
        self.all_record_versions = {}

    def get_attached_files_for_comment(self, recid, comment_id):
        """Get the attached files for the comment."""
        attached_files_directory = os.path.join(
            self.dirpath, str(recid), str(comment_id)
        )
        if os.path.exists(attached_files_directory):
            return os.listdir(attached_files_directory)
        return []

    def get_oldest_record(self, parent_pid_value):
        latest_record = current_rdm_records_service.read_latest(
            identity=system_identity, id_=parent_pid_value
        )
        search_result = current_rdm_records_service.scan_versions(
            system_identity,
            latest_record["id"],
        )
        for hit in search_result.hits:
            self.all_record_versions[hit["versions"]["index"]] = hit
        oldest_version_index = min(self.all_record_versions.keys())
        return self.all_record_versions[oldest_version_index]

    def create_event(
        self,
        request,
        data,
        community,
        uow,
        legacy_recid,
        parent_comment_id=None,
    ):
        logger.info(
            "Creating event for legacy recid ID<{}> request ID<{}> comment ID<{}> parent_comment ID<{}>".format(
                legacy_recid,
                request.id,
                data.get("comment_id"),
                "self" if not parent_comment_id else parent_comment_id,
            )
        )

        # Create comment event
        comment_payload = {
            "payload": {
                "content": data.get("content"),
                "format": RequestEventFormat.HTML.value,
            }
        }

        comment_status = data.get("status")
        event_type = CommentEventType
        if comment_status == "da":
            comment_payload["payload"]["content"] = "comment was deleted by the author."
            comment_payload["payload"]["event"] = "comment_deleted"
            event_type = LogEventType
        elif comment_status == "dm":
            comment_payload["payload"][
                "content"
            ] = "comment was deleted by the moderator."
            comment_payload["payload"]["event"] = "comment_deleted"
            event_type = LogEventType

        event = current_events_service.record_cls.create(
            {}, request=request.model, request_id=str(request.id), type=event_type
        )

        # If the comment is attached to a record file, add a link to the file in the content
        if data.get("file_relation"):
            file_relation = data.get("file_relation")
            file_id = file_relation.get("file_id")
            version = file_relation.get("version")
            record_version = self.all_record_versions.get(version, None)
            if record_version:
                record_url = (
                    current_app.config["CDS_MIGRATOR_KIT_SITE_UI_URL"]
                    + "/records/"
                    + record_version["id"]
                    + f"?preview_file={file_id}"
                )
                version_link_html = f"<p><a href='{record_url}'>See related record version {version}</a></p>"
                comment_payload["payload"]["content"] = (
                    version_link_html + "\n" + comment_payload["payload"]["content"]
                )

        if parent_comment_id:
            logger.info(
                "Found parent event<{}> for reply event<{}>. Setting parent_id.".format(
                    event.id,
                    parent_comment_id,
                )
            )
            # If it's a reply, 1. set parent comment id, 2. and if a nested reply, in the content add mentioned reply event's deep link
            event.parent_id = str(parent_comment_id)
            mentioned_event_id = self.LEGACY_REPLY_LINK_MAP.get(
                data.get("reply_to_id"), None
            )
            if mentioned_event_id:
                logger.info(
                    "Adding deep link to the content for the deeply nested reply event<{}>.".format(
                        mentioned_event_id,
                        event.id,
                    )
                )
                deep_link = (
                    current_app.config["CDS_MIGRATOR_KIT_SITE_UI_URL"]
                    + f"/communities/{community.slug}/requests/{request.id}#commentevent-{parent_comment_id}_{mentioned_event_id}"
                )
                deep_link_html = f"<p><a href='{deep_link}'>Link to the reply</a></p>"
                comment_payload["payload"]["content"] = (
                    deep_link_html + "\n" + comment_payload["payload"]["content"]
                )

        # TODO: Add attached files to the event
        # https://github.com/CERNDocumentServer/cds-migrator-kit/issues/381
        # For now, if attached files are found, raise ManualImportRequired error
        attached_files = self.get_attached_files_for_comment(
            legacy_recid, data.get("comment_id")
        )
        if attached_files:
            raise ManualImportRequired(
                message=f"Attached files found.",
                field=data.get("comment_id"),
                value=attached_files,
                stage="load",
                recid=legacy_recid,
                priority="critical",
            )

        event.update(comment_payload)
        user = User.query.filter_by(email=data.get("user_email")).one_or_none()
        if user:
            event.created_by = ResolverRegistry.resolve_entity_proxy(
                {"user": str(user.id)}, raise_=True
            )
        else:
            raise ManualImportRequired(
                message=f"User not found.",
                field=data.get("comment_id"),
                value=data.get("user_email"),
                stage="load",
                recid=legacy_recid,
                priority="critical",
            )
        created_at = arrow.get(data.get("created_at")).datetime.replace(tzinfo=None)
        event.model.created = created_at
        event.model.version_id = 0

        # Since we are not using the services to create the event, we need to register the commit operation manually for indexing
        uow.register(RecordCommitOp(event, indexer=current_events_service.indexer))

        return event

    def create_accepted_community_submission_request(
        self,
        legacy_recid,
        record,
        community,
        creator_user_id,
        comments=None,
    ):
        """Create an accepted community submission request."""
        if not comments:
            logger.warning(
                f"No comments found for record<{record['id']}>. Skipping request creation."
            )
            return None

        # Resolve entities for references
        creator_ref = ResolverRegistry.reference_entity(
            {"user": str(creator_user_id)}, raise_=True
        )
        receiver_ref = ResolverRegistry.reference_entity(
            {"community": str(community.id)}, raise_=True
        )
        topic_ref = ResolverRegistry.reference_entity(
            {"record": record["id"]}, raise_=True
        )

        with UnitOfWork() as uow:
            request_item = current_requests_service.create(
                system_identity,
                data={
                    "title": record["metadata"]["title"],
                },
                request_type=CommunitySubmission,
                receiver=receiver_ref,
                creator=creator_ref,
                topic=topic_ref,
                uow=uow,
            )
            request = request_item._record
            request.status = "accepted"
            request.number = f"lrecid:{legacy_recid}"
            created_at = arrow.get(record["created"]).datetime.replace(tzinfo=None)
            request.model.created = created_at

            logger.info(
                f"Created accepted community submission request<{request.id}> for record<{record['id']}>."
            )

            for comment_data in comments:
                comment_event = self.create_event(
                    request, comment_data, community, uow, legacy_recid
                )
                for reply in comment_data.get("replies", []):
                    reply_event = self.create_event(
                        request,
                        reply,
                        community,
                        uow,
                        legacy_recid,
                        parent_comment_id=comment_event.id,
                    )
                    self.LEGACY_REPLY_LINK_MAP[reply.get("comment_id")] = reply_event.id

            # Commit at the end so that rollback can be done if any error occurs not only for the request but also for the comments in the middle
            uow.commit()

        return request

    def _process_legacy_comments_for_recid(self, recid, comments):
        """Process the legacy comments for the record."""
        logger.info(f"Processing legacy comments for recid: {recid}")
        parent_pid = get_pid_by_legacy_recid(recid)
        oldest_record = self.get_oldest_record(parent_pid.pid_value)
        parent = RDMParent.pid.resolve(parent_pid.pid_value)
        community = parent.communities.default
        record_owner_id = parent.access.owned_by.owner_id
        # Skip if it is already migrated
        search_result = current_requests_service.search(
            identity=system_identity,
            q=f'number:"lrecid:{recid}"',
        )
        if search_result.total > 0:
            logger.info(
                f"Skipping recid: {recid} because the request comments are already migrated"
            )
            return None
        if self.dry_run:
            logger.info(f"Dry loading legacy comments for recid: {recid}")
            return None
        request = self.create_accepted_community_submission_request(
            recid, oldest_record, community, record_owner_id, comments
        )
        return request

    def _load(self, entry):
        """Use the services to load the entries."""
        if entry:
            recid, comments = entry
            try:
                self._process_legacy_comments_for_recid(recid, comments)
            except ManualImportRequired as ex:
                error_message = (
                    f"Error: {ex.message} | "
                    f"Value: {ex.value} | "
                    f"Recid: {recid} | "
                    f"Comment ID: {ex.field}"
                )
                logger.error(error_message)
            except Exception as ex:
                logger.error(f"Error: {ex} | Recid: {recid}")

    def _cleanup(self, *args, **kwargs):
        """Cleanup the entries."""
        pass
