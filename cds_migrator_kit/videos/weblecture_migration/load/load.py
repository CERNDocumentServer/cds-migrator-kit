# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos migration load module."""

import json
import logging
from pathlib import Path

from cds.modules.flows.deposit import index_deposit_project
from cds.modules.flows.files import init_object_version
from cds.modules.legacy.minters import legacy_recid_minter
from cds.modules.legacy.models import CDSMigrationLegacyRecord
from cds.modules.records.providers import CDSReportNumberProvider
from invenio_db import db
from invenio_pidstore.errors import PIDAlreadyExists
from invenio_pidstore.models import PersistentIdentifier
from invenio_rdm_migrator.load.base import Load
from invenio_records_files.api import Record
from sqlalchemy.exc import IntegrityError

from cds_migrator_kit.errors import (
    ManualImportRequired,
    MissingRequiredField,
    UnexpectedValue,
)
from cds_migrator_kit.reports.log import RDMJsonLogger

from .helpers import (
    copy_additional_files,
    copy_frames,
    create_flow,
    create_project,
    create_video,
    extract_metadata,
    publish_video_record,
    transcode_task,
)


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

    def _after_publish_update_created(self, record_uuid, entry):
        """Update created timestamp post publish."""
        # Fix the `created` timestamp forcing the one from the legacy system
        record = Record.get_record(record_uuid)
        created_date = entry["record"]["created"].datetime
        record.model.created = created_date

    def _after_publish_mint_legacy_recid(self, record_uuid, entry):
        """Mint legacy ids for redirections."""
        legacy_recid = entry["record"]["recid"]
        legacy_recid_minter(legacy_recid, record_uuid)

    def _after_publish(self, published_record, entry):
        """Run fixes after record publish."""
        # Get published record uuid
        recid_pid, _ = published_record.fetch_published()
        record_uuid = str(recid_pid.object_uuid)
        # Update creation date
        self._after_publish_update_created(record_uuid, entry)
        # Mint legacy recid
        self._after_publish_mint_legacy_recid(record_uuid, entry)
        # Save the original marcxml
        self._save_original_dumped_record(record_uuid, entry)

    def _get_submitter(self, entry):
        """Get the user id of the submitter."""
        submitter = entry.get("record", {}).get("owned_by", "")
        if not submitter:
            raise ManualImportRequired(f"No submitter found", stage="load")
        return submitter

    def _get_files(self, entry):
        """Get lecturemedia files."""
        if self.dry_run:
            # Use dummy files for loading; existence is already checked in the transform stage.
            return {
                "master_video": "tests/cds-videos/data/files/media_data/2025/1/presenter-720p.mp4",
                "frames": [
                    str(f)
                    for f in Path(
                        "tests/cds-videos/data/files/media_data/2025/1/frames"
                    ).iterdir()
                    if f.is_file() and not f.name.startswith(".")
                ],
                "subformats": [
                    {
                        "path": "tests/cds-videos/data/files/media_data/2025/1/presenter-360p.mp4",
                        "quality": "360p",
                    }
                ],
                "additional_files": [
                    "tests/cds-videos/data/files/media_data/2025/1/1_en.vtt"
                ],
            }
        return entry.get("record", {}).get("json", {}).get("media_files")

    def reserve_report_number(self, video_deposit, report_number):
        try:
            # Reserve report number
            CDSReportNumberProvider.create(
                object_type="rec",
                object_uuid=None,
                data=video_deposit,
                pid_value=report_number,
            )
        except IntegrityError as e:
            raise ManualImportRequired(f"Report number reserve failed! {e}")
        except PIDAlreadyExists:
            raise ManualImportRequired(
                f"Report number reserve failed! {report_number} already exists!"
            )

    def create_publish_single_video_record(self, entry):
        """Create and publish project and video for single video record."""
        # Get transformed metadata
        metadata = entry.get("record", {}).get("json", {}).get("metadata")
        # Get report_number
        report_number = metadata.get("report_number", None)
        # Get transformed media files
        media_files = self._get_files(entry)

        # Owner
        submitter = self._get_submitter(entry)

        # TODO `type` will be changed
        project_metadata = {"category": "CERN", "type": "VIDEO"}
        try:
            # Create project
            project_deposit = create_project(project_metadata, submitter)

            # Create video
            video_deposit, master_object = create_video(
                project_deposit, metadata, media_files["master_video"], submitter
            )

            # Get the deposit_id and bucket_id
            video_deposit_id = video_deposit["_deposit"]["id"]
            bucket_id = video_deposit["_buckets"]["deposit"]

            # Create flow and payload
            flow, payload = create_flow(
                master_object, video_deposit_id, user_id=submitter["id"]
            )

            # Create tags for the master video file
            init_object_version(flow.flow_metadata, has_remote_file_to_download=None)

            # Extract metadata
            extract_metadata(payload)

        except ManualImportRequired:
            db.session.rollback()
            # TODO if `copy_file_to_bucket` method failes it's deleting the file
            # but if anything else fails, should we try to delete all the files?
            raise

        # Just log if something goes wrong: Frames, Subformats, Additional Files
        frame_paths = media_files["frames"]
        copy_frames(payload=payload, frame_paths=frame_paths)

        subformat_paths = media_files["subformats"]
        transcode_task(payload=payload, subformats=subformat_paths)

        additional_files = media_files["additional_files"]
        copy_additional_files(str(bucket_id), additional_files)

        # Index deposit
        index_deposit_project(video_deposit_id)

        # Reserve the report number
        if report_number:
            self.reserve_report_number(video_deposit, report_number[0])

        # Publish video
        published_video = publish_video_record(deposit_id=video_deposit_id)

        # Run after publish fixes
        self._after_publish(published_video, entry)

    def _save_original_dumped_record(self, record_uuid, entry):
        """Save the original dumped record.

        This is the originally extracted record before any transformation.
        """
        _original_dump = entry["_original_dump"]

        _original_dump_model = CDSMigrationLegacyRecord(
            json=_original_dump,
            migrated_record_object_uuid=record_uuid,
            legacy_recid=entry["record"]["recid"],
        )
        db.session.add(_original_dump_model)
        db.session.commit()

    def _have_migrated_recid(self, recid):
        """Check if we have minted `lrecid` pid."""
        pid = PersistentIdentifier.query.filter_by(
            pid_type="lrecid",
            pid_value=recid,
        ).one_or_none()
        return pid is not None

    def _should_skip_recid(self, recid):
        """Check if recid should be skipped."""
        if self._have_migrated_recid(recid):
            return True
        return False

    def _load(self, entry):
        """Use the services to load the entries."""
        migration_logger = RDMJsonLogger(collection="weblectures")

        if entry:
            recid = entry.get("record", {}).get("recid", {})

            if self._should_skip_recid(recid):
                migration_logger.add_success_state(
                    recid, state={"message": "Record already migrated", "value": recid}
                )
                migration_logger.add_success(recid)
                return

            try:
                self.create_publish_single_video_record(entry)
                migration_logger.add_success(recid)
            except (
                UnexpectedValue,
                ManualImportRequired,
                MissingRequiredField,
            ) as e:
                migration_logger.add_log(e, record=entry)

    def _cleanup(self, *args, **kwargs):
        """Cleanup the entries."""
        pass
