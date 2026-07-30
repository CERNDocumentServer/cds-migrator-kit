# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM migration load module."""

import os
from datetime import datetime, timezone

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
from invenio_requests.records.models import RequestEventModel, RequestMetadata
from invenio_requests.resolvers.registry import ResolverRegistry
from invenio_users_resources.proxies import current_users_service
from invenio_users_resources.records.api import UserAggregate
from sqlalchemy.exc import NoResultFound

from cds_migrator_kit.errors import ManualImportRequired
from cds_migrator_kit.users.load import CDSSubmitterLoad


class CDSCommentsLoad(Load):
    """CDSCommentsLoad."""

    LEGACY_REPLY_LINK_MAP = {}
    """Map of legacy reply ids to RDM comment ids."""

    def __init__(
        self,
        dirpath,
        logger,
        dry_run=False,
        collection=None,
    ):
        """Constructor."""
        self.dirpath = dirpath  # The directory path where the attached files are stored
        self.dry_run = dry_run
        self.collection = collection
        self.logger = logger.get_logger()
        self.report_logger = logger
        self.all_record_versions = {}

    def get_attached_files_for_comment(self, recid, comment_id):
        """Get the attached files for the comment."""
        attached_files_directory = os.path.join(
            self.dirpath, str(recid), str(comment_id)
        )
        if os.path.exists(attached_files_directory):
            return [
                os.path.join(attached_files_directory, file)
                for file in os.listdir(attached_files_directory)
            ]
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
        count,
        parent_comment_id=None,
    ):
        """Create a comment event."""
        legacy_comment_id = data.get("comment_id")
        self.logger.info(
            "Creating event for legacy recid ID<{}> request ID<{}> comment ID<{}>".format(
                legacy_recid,
                request.id,
                legacy_comment_id,
            )
            + (
                " parent comment ID<{}>".format(parent_comment_id)
                if parent_comment_id
                else ""
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
        else:
            # Normal comment, add other links and files according to the data
            event_type = CommentEventType
            if data.get("file_relation"):
                file_relation = data.get("file_relation")
                file_id = file_relation.get("file_id")
                version = file_relation.get("version")
                record_version = self.all_record_versions.get(version, None)
                # If the comment is attached to a record file, add a link to the file in the content
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

            attached_files = self.get_attached_files_for_comment(
                legacy_recid, legacy_comment_id
            )
            if attached_files:
                request.files.enabled = True
                if request.files.bucket is None:
                    request.files.create_bucket()
                comment_payload["payload"]["files"] = []

                for file in attached_files:
                    filename = os.path.basename(file)
                    unique_key = f"{legacy_comment_id}_{filename}"
                    self.logger.info("Adding file<{}> to the event.".format(unique_key))
                    try:
                        with open(file, "rb") as fp:
                            request.files[unique_key] = (
                                fp,
                                {"metadata": {"description": "MIGRATED"}},
                            )
                            request.files[unique_key].model.data = {
                                "original_filename": filename
                            }
                        comment_payload["payload"]["files"].append(
                            {"file_id": str(request.files[unique_key].file.id)}
                        )
                    except Exception as e:
                        self.logger.error(
                            "Error adding file<{}> to the event: {}".format(
                                unique_key, e
                            )
                        )
                        raise ManualImportRequired(
                            message=f"Error adding file<{unique_key}> to the event: {e}",
                            field=legacy_comment_id,
                            subfield="files",
                            value=file,
                            stage="load",
                            recid=legacy_recid,
                            priority="critical",
                        )

        event = current_events_service.record_cls.create(
            {}, request=request.model, request_id=str(request.id), type=event_type
        )
        event.update(comment_payload)

        if parent_comment_id:
            self.logger.info(
                "Found parent event<{}> for reply event. Setting parent_id.".format(
                    parent_comment_id,
                )
            )
            # If it's a reply, 1. set parent comment id, 2. and if a nested reply, in the content add mentioned reply event's deep link
            event.parent_id = str(parent_comment_id)
            mentioned_event_id = self.LEGACY_REPLY_LINK_MAP.get(
                data.get("reply_to_id"), None
            )
            if mentioned_event_id:
                self.logger.info(
                    "Adding deep link to the content for the deeply nested reply event<{}>.".format(
                        mentioned_event_id,
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

        user_email = data.get("user_email")
        user = User.query.filter_by(email=user_email).one_or_none()
        if user:
            event.created_by = ResolverRegistry.resolve_entity_proxy(
                {"user": str(user.id)}, raise_=True
            )
        else:
            raise ManualImportRequired(
                message=f"User not found.",
                field=legacy_comment_id,
                subfield="user_email",
                value=user_email,
                stage="load",
                recid=legacy_recid,
                priority="critical",
            )
        created_at = datetime.strptime(
            data.get("created_at"), "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=timezone.utc)
        event.model.created = created_at
        event.model.version_id = 0

        # Since we are not using the services to create the event, we need to register the commit operation manually for indexing
        uow.register(RecordCommitOp(event, indexer=current_events_service.indexer))

        self.report_logger.add_comment_log(
            id=count
            + 1,  # For each request, the report logger will have id 1, 2, .. for the comments
            legacy_recid=legacy_recid,
            legacy_comment_id=legacy_comment_id,
            status="success",
            new_comment_id=event.id,
            parent_comment_id=parent_comment_id,
            community_slug=community.slug,
            request_id=request.id,
        )

        return event

    def _configured_reviewers(self):
        """Return configured reviewers for the current collection, if any."""
        return (
            current_app.config.get("CDS_MIGRATOR_KIT_COMMENTS_REVIEWERS", {}).get(
                self.collection
            )
            or []
        )

    def _get_existing_request(self, legacy_recid):
        """Return the request with number ``lrecid:{legacy_recid}``, if any."""
        model = RequestMetadata.query.filter_by(
            number=f"lrecid:{legacy_recid}"
        ).one_or_none()
        if model is None:
            return None
        return current_requests_service.record_cls.get_record(model.id)

    def _request_has_migrated_comments(self, request):
        """Return True if migrated comment events already exist on the request."""
        if RequestEventModel.query.filter_by(
            request_id=request.id, type=CommentEventType.type_id
        ).first():
            return True

        # Deleted legacy comments are stored as log events
        for row in RequestEventModel.query.filter_by(
            request_id=request.id, type=LogEventType.type_id
        ):
            payload = (row.json or {}).get("payload") or {}
            if payload.get("event") == "comment_deleted":
                return True
        return False

    def _apply_reviewers(self, request):
        """Add configured reviewers that are not already on the request."""
        configured = self._configured_reviewers()
        if not configured:
            return

        existing = list(request.get("reviewers") or [])
        added = []
        for reviewer in configured:
            if reviewer not in existing:
                existing.append(reviewer)
                added.append(reviewer)

        if not added:
            self.logger.info(
                f"Configured reviewers already present on request<{request.id}>."
            )
            return

        request.reviewers = existing
        self.logger.info(
            f"Added reviewers {added} on request<{request.id}> "
            f"for collection<{self.collection}>."
        )

    def _add_comments_to_request(
        self, request, parent, comments, legacy_recid, uow, link_parent_relation=False
    ):
        """Create comment/reply events on an existing request."""
        community = parent.communities.default
        self.LEGACY_REPLY_LINK_MAP = {}
        count = 0

        for comment_data in comments:
            comment_event = self.create_event(
                request, comment_data, community, uow, legacy_recid, count
            )
            count += 1
            for reply in comment_data.get("replies", []):
                reply_event = self.create_event(
                    request,
                    reply,
                    community,
                    uow,
                    legacy_recid,
                    count=count,
                    parent_comment_id=comment_event.id,
                )
                self.LEGACY_REPLY_LINK_MAP[reply.get("comment_id")] = reply_event.id
                count += 1

        if link_parent_relation:
            # Normally executed by the accept action in the request service
            parent_to_request_relation = (
                parent.communities._m2m_model_cls.query.filter_by(
                    record_id=parent.id, community_id=community.id
                ).one()
            )
            parent_to_request_relation.request_id = request.id

        return count

    def create_accepted_community_submission_request(
        self,
        legacy_recid,
        record,
        parent,
        comments=None,
    ):
        """Create an accepted community submission request."""
        if not comments:
            self.logger.warning(
                f"No comments found for record<{record['id']}>. Skipping request creation."
            )
            return None

        community = parent.communities.default
        record_owner_id = parent.access.owned_by.owner_id

        # Resolve entities for references
        creator_ref = ResolverRegistry.reference_entity(
            {"user": str(record_owner_id)}, raise_=True
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
            created_at = datetime.fromisoformat(record["created"])
            request.model.created = created_at

            self._apply_reviewers(request)

            self.logger.info(
                f"Created accepted community submission request<{request.id}> for record<{record['id']}>."
            )

            count = self._add_comments_to_request(
                request,
                parent,
                comments,
                legacy_recid,
                uow,
                link_parent_relation=True,
            )

            # Commit at the end so that rollback can be done if any error occurs not only for the request but also for the comments in the middle
            uow.register(
                RecordCommitOp(request, indexer=current_requests_service.indexer)
            )
            uow.commit()
        self.logger.info(
            f"Successfully migrated {count} comment(s) for request: {request.id} from recid: {legacy_recid}"
        )

        return request

    def migrate_comments_to_existing_request(
        self, request, parent, comments, legacy_recid
    ):
        """Attach migrated comments to an existing community inclusion request."""
        if not comments:
            self.logger.warning(
                f"No comments found for request<{request.id}>. Skipping."
            )
            return None

        with UnitOfWork() as uow:
            self._apply_reviewers(request)
            self.logger.info(
                f"Migrating comments onto existing request<{request.id}> "
                f"for recid: {legacy_recid}."
            )
            count = self._add_comments_to_request(
                request, parent, comments, legacy_recid, uow
            )
            uow.register(
                RecordCommitOp(request, indexer=current_requests_service.indexer)
            )
            uow.commit()

        self.logger.info(
            f"Successfully migrated {count} comment(s) for request: {request.id} from recid: {legacy_recid}"
        )
        return request

    def _resolve_record_and_parent(self, recid):
        """Resolve the RDM parent and oldest record version for comments.

        Legacy recid PIDs are minted on the public parent. For EP-approval
        migrations, comments must instead target the restricted (internal)
        parent, linked via ``permission_flags.committee_approval.source_internal_version``.
        Non-EP records keep the legacy-recid → parent → oldest version path.
        """
        parent_pid = get_pid_by_legacy_recid(recid)
        parent = RDMParent.pid.resolve(parent_pid.pid_value)
        parent_pid_value = parent_pid.pid_value

        internal_version = (
            (parent.get("permission_flags") or {})
            .get("committee_approval", {})
            .get("source_internal_version")
        )
        if internal_version:
            self.logger.info(
                f"EP approval record detected for recid: {recid}. "
                f"Resolving comments target via internal version: {internal_version}"
            )
            internal_record = current_rdm_records_service.read(
                identity=system_identity, id_=internal_version
            )
            parent_pid_value = internal_record["parent"]["id"]
            parent = RDMParent.pid.resolve(parent_pid_value)

        self.all_record_versions = {}
        oldest_record = self.get_oldest_record(parent_pid_value)
        return oldest_record, parent

    def _process_legacy_comments_for_recid(self, recid, comments):
        """Process the legacy comments for the record."""
        self.logger.info(f"Processing legacy comments for recid: {recid}")
        oldest_record, parent = self._resolve_record_and_parent(recid)

        existing_request = self._get_existing_request(recid)
        if existing_request and self._request_has_migrated_comments(existing_request):
            self.logger.info(
                f"Skipping recid: {recid} because the request comments are already migrated"
            )
            return None

        if self.dry_run:
            target = (
                f"existing request<{existing_request.id}>"
                if existing_request
                else "a new community submission request"
            )
            self.logger.info(
                f"Dry loading legacy comments for recid: {recid} onto {target}"
            )
            return None

        if existing_request:
            return self.migrate_comments_to_existing_request(
                existing_request, parent, comments, recid
            )

        return self.create_accepted_community_submission_request(
            recid, oldest_record, parent, comments
        )

    def _load(self, entry):
        """Use the services to load the entries."""
        if entry:
            recid, comments = entry
            try:
                self._process_legacy_comments_for_recid(recid, comments)
            except ManualImportRequired as ex:
                error_message = (
                    f"Error: {ex.message} | "
                    f"Field: {ex.subfield} | "
                    f"Value: {ex.value} | "
                    f"Recid: {recid} | "
                    f"Comment ID: {ex.field}"
                )
                self.report_logger.add_comment_log(
                    id=None,
                    legacy_recid=recid,
                    legacy_comment_id=ex.field,
                    status="error",
                    error_message=error_message,
                )
                self.logger.error(error_message)
            except Exception as ex:
                self.logger.error(f"Error: {ex} | Recid: {recid}", exc_info=1)

    def _cleanup(self, *args, **kwargs):
        """Cleanup the entries."""
        pass


class CDSCommentersLoad(CDSSubmitterLoad):
    """CDSCommentersLoad."""

    def _owner(self, json_entry):
        """Fetch or create user commenter, overrids the parent method."""
        email = json_entry.get("submitter")
        if not email:
            return
        try:
            user = User.query.filter_by(email=email).one()
            self.logger.info(f"User commenter already exists: {user.id}")
        except NoResultFound:
            if self.dry_run:
                self.logger.info(f"Dry running user commenter creation: {email}")
                return
            user_id = self._create_owner(email)
            if not user_id:
                self.logger.error(f"Failed to create user commenter: {email}")
                return
            user_record = UserAggregate.get_record(user_id)
            current_users_service.indexer.index(user_record)
            self.logger.info(
                f"Successfully created and indexed user commenter: {user_id}"
            )
