# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM migration load module for records with EP approval."""
import re
from collections import OrderedDict
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

from cds_rdm.requests.committee_approval import APPRN_PID_TYPE, CommitteeApprovalRequest
from flask import current_app
from invenio_access.permissions import system_identity
from invenio_accounts.models import User
from invenio_db import db
from invenio_db.uow import UnitOfWork
from invenio_drafts_resources.services.records.uow import ParentRecordCommitOp
from invenio_pidstore.errors import PIDAlreadyExists
from invenio_pidstore.models import PersistentIdentifier, PIDStatus
from invenio_rdm_records.proxies import current_rdm_records_service
from invenio_rdm_records.records.api import RDMParent
from invenio_records_resources.services.uow import RecordCommitOp
from invenio_requests.customizations.event_types import LogEventType
from invenio_requests.proxies import current_events_service, current_requests_service
from invenio_requests.resolvers.registry import ResolverRegistry

from cds_migrator_kit.errors import ManualImportRequired, UnexpectedValue
from cds_migrator_kit.rdm.migration_config import CDS_CERN_SCIENTIFIC_COMMUNITY_ID

from .load import CDSRecordServiceLoad

EPPHAPP_FILE_TYPE = "EPPHAPP_FILE"
EP_APPROVAL_WAITING_STATUS = "waiting"
EP_APPROVAL_APPROVED_STATUS = "approved"
EP_APPROVAL_REPORT_NUMBER_PREFIX = "CERN-EP"
EP_APPROVAL_REPORT_NUMBER_RE = re.compile(r"^CERN-EP-\d{4}-\d{3}$")


class CDSEPApprovalRecordServiceLoad(CDSRecordServiceLoad):
    """Load records with EP approval.

    Splits a legacy record into two RDM records before load:
    - a public record with non-EPPHAPP files
    - a restricted record with restricted EPPHAPP files
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ep_approval_metadata = {
            "title": None,
            "experiment": None,
            "resource_type": None,
            "report_number": None,
        }
        self._ep_approval_parsed = None
        self._load_flags = self._public_load_flags()

    def _public_load_flags(self):
        """Load flags for the public migrated record."""
        return {
            "mint_pids": True,
            "mint_legacy_recid": True,
            "save_original_dump": True,
            "clc_sync": True,
            "record_state": True,
        }

    def _restricted_load_flags(self):
        """Load flags for the restricted EPPHAPP snapshot record."""
        return {
            "mint_pids": False,
            "mint_legacy_recid": False,
            "save_original_dump": False,
            "clc_sync": False,
            "record_state": False,
        }

    def _should_log_record_state(self):
        return self._load_flags["record_state"]

    def _after_publish_mint_recid(self, record, entry, version):
        if self._load_flags["mint_legacy_recid"]:
            super()._after_publish_mint_recid(record, entry, version)

    def _after_publish_update_dois(self, identity, record, entry, uow):
        if self._load_flags["mint_pids"]:
            return super()._after_publish_update_dois(identity, record, entry, uow)

    def _assign_rep_numbers(self, draft):
        if self._load_flags["mint_pids"]:
            super()._assign_rep_numbers(draft)

    def _save_original_dumped_record(self, entry, recid_state):
        if self._load_flags["save_original_dump"]:
            super()._save_original_dumped_record(entry, recid_state)

    def _after_load_clc_sync(self, record_state):
        if self._load_flags["clc_sync"]:
            super()._after_load_clc_sync(record_state)

    def _load(self, entry):
        """
        Load the record with EP approval.
        Configure the 2 records by separating the files, then:
        1. create the restricted record
        2. create and approve the EP approval request
        3. create the public record and link both with related_identifiers
        """
        if not entry:
            return
        try:
            recid = entry.get("record", {}).get("recid")
            ep_approval = entry.get("record", {}).get("ep_approval")
            if not ep_approval:
                raise UnexpectedValue(
                    message="EP approval request not found",
                    stage="load",
                    recid=recid,
                    priority="critical",
                )
            record_json = entry.get("record", {}).get("json", {})
            metadata = record_json.get("metadata", {})

            # Set the EP approval metadata
            self.ep_approval_metadata["resource_type"] = metadata.get("resource_type")
            self.ep_approval_metadata["title"] = metadata.get("title")

            # Validate the EP approval data
            self._validate_ep_approval(ep_approval, recid)

            # Split the metadata and files
            public_entry = self._split_entry(entry, include_epphapp=False)
            restricted_entry = self._split_entry(entry, include_epphapp=True)

            # 1. Create restricted record
            restricted_record_state = self._load_split_record(
                restricted_entry, self._restricted_load_flags(), finalise=False
            )

            # 2. Create and approve EP approval request
            self._create_ep_approval(restricted_record_state, legacy_recid=recid)

            # 3. Create public record and link both records
            public_record_state = self._load_split_record(
                public_entry,
                self._public_load_flags(),
                finalise=True,
            )
            self._link_ep_approval_records(
                restricted_record_state, public_record_state, legacy_recid=recid
            )
        except (UnexpectedValue, ManualImportRequired) as e:
            self.migration_logger.add_log(e, record=entry)
        except Exception as e:
            exc = ManualImportRequired(
                message=str(e),
                field="validation",
                stage="load",
                recid=recid,
                priority="warning",
            )
            self.migration_logger.add_log(exc, record=entry)

    def _load_split_record(self, entry, load_flags, finalise):
        """Load the record."""
        self._load_flags = load_flags
        self._finalise_on_load = finalise
        return super()._load(entry)

    def _validate_ep_approval(self, ep_approval, legacy_recid):
        """Validate EP approval data before creating any records."""
        waiting_entry, approved_entry, report_number = self._parse_ep_approval_history(
            ep_approval
        )

        existing = self._existing_ep_approval_request(legacy_recid)
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

        self._ep_approval_parsed = (waiting_entry, approved_entry, report_number)

    def _create_ep_approval(self, restricted_record_state, legacy_recid):
        """Create and approve EP approval request after restricted record exists."""
        waiting_entry, approved_entry, report_number = self._ep_approval_parsed
        publication_title = self.ep_approval_metadata["title"]

        if not self.dry_run:
            if not restricted_record_state:
                raise UnexpectedValue(
                    message="Restricted record is required for EP approval.",
                    stage="load",
                    recid=legacy_recid,
                    priority="critical",
                )
            restricted_recid = restricted_record_state["latest_version"]
            restricted_parent = RDMParent.get_record(
                restricted_record_state["parent_object_uuid"]
            )
            self._create_ep_approval_request(
                legacy_recid,
                restricted_recid,
                restricted_parent,
                waiting_entry,
                approved_entry,
                report_number,
                publication_title,
            )
            self._mint_apprn_pid(
                report_number, restricted_record_state["latest_version_object_uuid"]
            )

    def _link_ep_approval_records(
        self, restricted_record_state, public_record_state, legacy_recid
    ):
        """Write parent metadata and link public/restricted records."""
        _, approved_entry, report_number = self._ep_approval_parsed
        approval_iso = self._parse_legacy_datetime(
            approved_entry.get("date")
        ).isoformat()

        if not self.dry_run:
            if not restricted_record_state or not public_record_state:
                raise UnexpectedValue(
                    message="Both public and restricted records are required for EP approval.",
                    stage="load",
                    recid=legacy_recid,
                    priority="critical",
                )
            restricted_recid = restricted_record_state["latest_version"]
            public_recid = public_record_state["latest_version"]
            restricted_parent = RDMParent.get_record(
                restricted_record_state["parent_object_uuid"]
            )
            public_parent = RDMParent.get_record(
                public_record_state["parent_object_uuid"]
            )

            with UnitOfWork() as uow:
                self._write_parent_ep_approval(
                    restricted_parent,
                    {
                        "reportnumber": report_number,
                        "datetime": approval_iso,
                        "approved_internal_version": restricted_recid,
                        "approved_public_version": public_recid,
                        "source_public_version": restricted_recid,
                    },
                    uow,
                )
                self._write_parent_ep_approval(
                    public_parent,
                    {
                        "reportnumber": report_number,
                        "source_internal_version": restricted_recid,
                    },
                    uow,
                )
                uow.commit()
            # Link the records with related_identifiers
            self._append_related_identifier(
                public_recid,
                restricted_recid,
                "isversionof",
                self.ep_approval_metadata["resource_type"],
            )
            self._append_related_identifier(
                restricted_recid,
                public_recid,
                "isvariantformof",
                self.ep_approval_metadata["resource_type"],
            )

    def _parse_ep_approval_history(self, ep_approval):
        """Return waiting/approved history entries and the report number."""
        if len(ep_approval) != 2:
            raise UnexpectedValue(
                message="EP approval history has more/less than 2 entries",
                stage="load",
                priority="critical",
            )
        history = ep_approval or []
        waiting = next(
            (
                item
                for item in history
                if item.get("status") == EP_APPROVAL_WAITING_STATUS
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
                message="EP approval waiting entry has different ep_report_number than approved entry",
                stage="load",
                priority="critical",
            )
        self.ep_approval_metadata["report_number"] = report_number
        # Check if the submitters are exists
        self._resolve_user_by_email(waiting.get("submitted_by"), "submitter")
        self._resolve_user_by_email(approved.get("submitted_by"), "approver")

        # Check if record approved after the deadline
        waiting_deadline = self._parse_legacy_datetime(waiting.get("deadline"))
        approved_date = self._parse_legacy_datetime(approved.get("date"))
        created_at = self._parse_legacy_datetime(waiting.get("date"))
        if not created_at or not approved_date or not waiting_deadline:
            raise UnexpectedValue(
                message="EP approval history has missing timestamps",
                stage="load",
                priority="critical",
            )
        if waiting_deadline and approved_date > waiting_deadline:
            raise UnexpectedValue(
                message="Record approved after the deadline",
                stage="load",
                priority="critical",
            )
        return waiting, approved, report_number

    @staticmethod
    def _parse_legacy_datetime(value):
        """Parse legacy EP approval timestamps into timezone-aware datetimes."""
        if not value:
            return None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        return None

    def _get_ep_referee_group(self, restricted_parent):
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

    def _resolve_user_by_email(self, email, role):
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

    def _existing_ep_approval_request(self, legacy_recid):
        """Check if the EP approval request already exists."""
        number = f"lrecid:{legacy_recid}:ep-approval"
        results = current_requests_service.search(
            system_identity,
            params={"q": f'number:"{number}"', "size": 1},
        )
        hits = list(results.hits)
        return hits[0] if hits else None

    def _exists_apprn_pid(self, report_number):
        """Check if the APPRN PID already exists."""
        existing = PersistentIdentifier.query.filter_by(
            pid_type=APPRN_PID_TYPE,
            pid_value=report_number,
        ).one_or_none()
        if existing:
            return True
        return False

    def _mint_apprn_pid(self, report_number, restricted_version_uuid):
        """Mint the APPRN PID."""
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

    def _write_parent_ep_approval(
        self,
        parent,
        ep_approval,
        uow,
    ):
        """Write the EP approval metadata to the parent record."""
        pf = parent.get("permission_flags") or {}
        pf["committee_approval"] = ep_approval
        parent["permission_flags"] = pf
        uow.register(ParentRecordCommitOp(parent))

    def _create_accept_log_event(self, request, approved_entry, uow):
        """Create the accept timeline event with the legacy approver as created_by."""
        approver_ref = self._resolve_user_by_email(
            approved_entry.get("submitted_by"),
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

        approved_at = self._parse_legacy_datetime(approved_entry.get("date"))
        if approved_at:
            event.model.created = approved_at

        uow.register(RecordCommitOp(event, indexer=current_events_service.indexer))

    def _apply_approved_entry_to_request(
        self, request, approved_entry, report_number, uow
    ):
        """Update an existing request to accepted using the legacy approved entry."""
        payload = dict(request.get("payload") or {})
        payload["approved_report_number"] = report_number
        request["payload"] = payload
        request.status = "accepted"

        approved_at = self._parse_legacy_datetime(approved_entry.get("date"))
        if approved_at:
            request.model.updated = approved_at

        self._create_accept_log_event(request, approved_entry, uow)

    def _create_ep_approval_request(
        self,
        legacy_recid,
        restricted_recid,
        restricted_parent,
        waiting_entry,
        approved_entry,
        report_number,
        publication_title,
    ):
        """Create request from waiting entry, then update it with approved entry."""
        expires_at = self._parse_legacy_datetime(waiting_entry.get("deadline"))

        referee_group = self._get_ep_referee_group(restricted_parent)
        with UnitOfWork() as uow:
            request_item = current_requests_service.create(
                system_identity,
                data={
                    "title": f'EP approval for "{publication_title}"',
                    # Use the default
                    "payload": {},
                },
                request_type=CommitteeApprovalRequest,
                receiver={"group": referee_group},
                creator=self._resolve_user_by_email(
                    waiting_entry.get("submitted_by"), "submitter"
                ),
                topic={"record": restricted_recid},
                expires_at=expires_at,
                uow=uow,
            )
            request = request_item._record
            request.number = f"lrecid:{legacy_recid}:ep-approval"
            request.status = "submitted"

            submitted_at = self._parse_legacy_datetime(waiting_entry.get("date"))
            if submitted_at:
                request.model.created = submitted_at

            self._apply_approved_entry_to_request(
                request, approved_entry, report_number, uow
            )

            uow.register(
                RecordCommitOp(request, indexer=current_requests_service.indexer)
            )
            uow.commit()

    def _append_related_identifier(
        self, record_id, target_id, relation_id, resource_type
    ):
        """Append the related identifier to the record."""
        draft = current_rdm_records_service.edit(system_identity, id_=record_id)
        data = draft.data
        related = list(data.get("metadata", {}).get("related_identifiers", []))

        entry = {
            "identifier": target_id,
            "scheme": "cds",
            "relation_type": {"id": relation_id},
        }
        if resource_type:
            entry["resource_type"] = resource_type
        related.append(entry)
        data.setdefault("metadata", {})["related_identifiers"] = related
        current_rdm_records_service.update_draft(
            system_identity, id_=draft.id, data=data
        )
        current_rdm_records_service.publish(system_identity, id_=draft.id)
        return True

    def _add_cern_scientific_community(self, entry):
        """Add the CERN Scientific community to the public record parent."""
        communities = entry.get("parent", {}).get("json", {}).get("communities", {})
        ids = list(communities.get("ids", []))
        if CDS_CERN_SCIENTIFIC_COMMUNITY_ID not in ids:
            ids.append(CDS_CERN_SCIENTIFIC_COMMUNITY_ID)
        communities["ids"] = ids
        entry.setdefault("parent", {}).setdefault("json", {})[
            "communities"
        ] = communities

    def _should_remove_ep_report_number(self, identifier, public_split):
        """Return whether an EP report number should be stripped from metadata."""
        if not identifier.startswith(EP_APPROVAL_REPORT_NUMBER_PREFIX):
            return False
        if public_split:
            return True
        if EP_APPROVAL_REPORT_NUMBER_RE.match(identifier):
            if identifier != self.ep_approval_metadata["report_number"]:
                raise UnexpectedValue(
                    message="EP report number is not the same as the approved entry",
                    stage="load",
                    priority="critical",
                )
            return True
        return False

    def _remove_ep_report_numbers_from_metadata(self, entry, include_epphapp):
        """Strip EP report numbers from split record metadata before load."""
        recid = entry.get("record", {}).get("recid")
        metadata = entry.get("record", {}).get("json", {}).get("metadata", {})
        identifiers = metadata.get("identifiers", [])
        if not identifiers:
            return

        kept = []
        removed = []
        for id_entry in identifiers:
            if id_entry.get("scheme") != "cdsrn":
                kept.append(id_entry)
                continue
            identifier = id_entry.get("identifier", "")
            if self._should_remove_ep_report_number(identifier, not include_epphapp):
                removed.append(identifier)
            else:
                kept.append(id_entry)

        if not removed:
            return

        metadata["identifiers"] = kept
        split_type = "restricted" if include_epphapp else "public"
        self.migration_logger.add_information(
            recid,
            {
                "message": (
                    f"Removed EP approval report number(s) from {split_type} " "record."
                ),
                "value": removed,
            },
        )

    def _remove_doi_pid_from_metadata(self, entry, include_epphapp):
        """Strip DOI PID from restricted EPPHAPP split record metadata before load."""
        if not include_epphapp:
            return

        recid = entry.get("record", {}).get("recid")
        record_json = entry.get("record", {}).get("json", {})
        pids = record_json.get("pids")

        if not pids or "doi" not in pids:
            return

        removed = pids.pop("doi")

        self.migration_logger.add_information(
            recid,
            {
                "message": "Removed DOI PID from restricted record.",
                "value": removed,
            },
        )

    def _split_entry(self, entry, include_epphapp):
        """Return a load entry for the public or restricted EP approval split."""
        split = deepcopy(entry)
        split["record"].pop("ep_approval", None)

        new_versions = OrderedDict()
        versioned_files = OrderedDict()
        previous_signature = None

        for _, version_data in split.get("versions", {}).items():
            current_version_files = OrderedDict()

            for key, file_data in version_data.get("files", {}).items():
                is_epphapp = file_data.get("type") == EPPHAPP_FILE_TYPE

                if include_epphapp != is_epphapp:
                    continue

                if not include_epphapp and file_data.get("access"):
                    raise UnexpectedValue(
                        message=(
                            "Public split contains restricted files after excluding "
                            f"EPPHAPP files: {[key]}"
                        ),
                        stage="load",
                        recid=split["record"]["recid"],
                        priority="critical",
                    )

                current_version_files[key] = deepcopy(file_data)

            if not current_version_files:
                continue

            versioned_files.update(current_version_files)

            signature = tuple(
                sorted(
                    (
                        key,
                        file_data.get("checksum"),
                        file_data.get("id_bibdoc"),
                        file_data.get("version"),
                        file_data.get("type"),
                        file_data.get("access"),
                    )
                    for key, file_data in versioned_files.items()
                )
            )
            # If the signature is the same, skip the version
            if signature == previous_signature:
                continue

            previous_signature = signature

            version_access = deepcopy(version_data.get("access", {}))
            access_obj = deepcopy(version_access.get("access_obj", {}))

            if include_epphapp:
                access_obj["record"] = "restricted"
                access_obj["files"] = "restricted"
            else:
                access_obj["record"] = "public"
                access_obj["files"] = "public"
                # Remove the meta field from the public version access
                version_access.pop("meta", None)

            version_access["access_obj"] = access_obj

            new_version_data = deepcopy(version_data)
            new_version_data["files"] = deepcopy(versioned_files)
            new_version_data["access"] = version_access

            new_versions[len(new_versions) + 1] = new_version_data

        if not new_versions:
            raise UnexpectedValue(
                message=(
                    "No EPPHAPP files found to load for EP approval restricted split"
                    if include_epphapp
                    else "No public files found to load for EP approval public split"
                ),
                stage="load",
                recid=split["record"]["recid"],
                priority="critical",
            )

        if not include_epphapp:
            split["record"]["access"] = "public"
            self._add_cern_scientific_community(split)
            # Add the approval report number to the public record metadata
            _, _, report_number = self._ep_approval_parsed
            split["record"]["json"]["metadata"]["identifiers"].append(
                {
                    "identifier": report_number,
                    "scheme": "apprn",
                }
            )

        split["versions"] = new_versions
        self._remove_ep_report_numbers_from_metadata(split, include_epphapp)
        self._remove_doi_pid_from_metadata(split, include_epphapp)

        return split
