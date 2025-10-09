# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos migration load module."""

from pathlib import Path

from cds.modules.flows.deposit import index_deposit_project
from cds.modules.flows.files import init_object_version
from cds.modules.flows.tasks import ExtractChapterFramesTask
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
from cds_migrator_kit.reports.log import (
    MigrationProgressLogger,
    RecordStateLogger,
)
from cds_migrator_kit.videos.weblecture_migration.logger import VideosJsonLogger
from cds_migrator_kit.videos.weblecture_migration.transform.xml_processing.quality.identifiers import (
    transform_legacy_urls,
)

from .helpers import (
    copy_additional_files,
    copy_frames,
    create_flow,
    create_project,
    create_video,
    extract_metadata,
    publish_project,
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
        collection=None,  # weblectures
        migration_logger=None,
        record_state_logger=None,
    ):
        """Constructor."""
        self.dry_run = dry_run
        self.migration_logger = migration_logger or MigrationProgressLogger(
            collection="weblectures"
        )
        self.record_state_logger = record_state_logger or RecordStateLogger(
            collection="weblectures"
        )

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

    def _after_publish(self, published_record, entry, legacy_handling=True):
        """Run fixes after record publish."""
        # Get published record uuid
        recid_pid, _ = published_record.fetch_published()
        record_uuid = str(recid_pid.object_uuid)
        # Update creation date
        self._after_publish_update_created(record_uuid, entry)
        if legacy_handling:
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

    def _get_files(self, media_files, afs_files=[]):
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
                "chapters": [],
                "master_path": "/media_data/2025/1",
            }
        media_files.setdefault("additional_files", []).extend(afs_files)
        return media_files

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

    def _create_video_and_flow(self, project_deposit, metadata, media_files, submitter):
        """
        Create a video deposit and initialize its flow/payload.
        Run the extract metadata task.

        Returns:
            tuple: (video_deposit, video_deposit_id, bucket_id, payload)
        """
        try:
            # Create video
            video_deposit, master_object = create_video(
                project_deposit, metadata, media_files, submitter
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

            return video_deposit, video_deposit_id, bucket_id, payload

        except ManualImportRequired:
            db.session.rollback()
            # TODO if `copy_file_to_bucket` method failes it's deleting the file
            # but if anything else fails, should we try to delete all the files?
            raise

    def _run_tasks_and_publish_video(
        self,
        video_deposit,
        video_deposit_id,
        bucket_id,
        payload,
        media_files,
        report_number,
    ):
        """
        Handle frames, subformats, additional files, indexing, report number,
        publishing, and chapters tasks for a video record.
        Returns:
            The published video record.
        """
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

        # Run ExtractChapterFramesTask
        ExtractChapterFramesTask().s(**payload).apply_async()

        return published_video

    def create_publish_single_video_record(self, entry):
        """Create and publish project and video for single video record."""
        # Get transformed metadata
        metadata = entry.get("record", {}).get("json", {}).get("metadata")
        # Get report_number
        report_number = metadata.get("report_number", None)
        # Get transformed media files
        ceph_files = entry.get("record", {}).get("json", {}).get("media_files", {})
        afs_files = entry.get("record", {}).get("json", {}).get("files", [])
        media_files = self._get_files(ceph_files, afs_files)

        # Owner
        submitter = self._get_submitter(entry)

        project_metadata = {
            "category": "LECTURES",
            "type": "VIDEO",
            "_access": metadata["_access"],
        }
        # Create project
        project_deposit = create_project(project_metadata, submitter)

        # Create video and flow
        video_deposit, video_deposit_id, bucket_id, payload = (
            self._create_video_and_flow(
                project_deposit, metadata, media_files, submitter
            )
        )

        # Run tasks and publish video
        published_video = self._run_tasks_and_publish_video(
            video_deposit,
            video_deposit_id,
            bucket_id,
            payload,
            media_files,
            report_number,
        )

        legacy_recid = entry["record"]["recid"]
        cds_videos_recid = str(published_video["recid"])
        VideosJsonLogger.log_record_redirection(
            legacy_id=legacy_recid,
            cds_videos_id=cds_videos_recid,
        )

        # Run after publish fixes
        self._after_publish(published_video, entry)

    def create_publish_multiple_video_record(self, entry):
        """Create and publish project and video records for multiple video record."""
        json_data = entry.get("record", {}).get("json", {})
        # Get transformed metadata
        common_metadata = json_data.get("metadata")
        # Dont mint report number for multiple video record
        report_number = None

        # Owner
        submitter = self._get_submitter(entry)

        project_metadata = {
            "category": "LECTURES",
            "type": "VIDEO",
            "_access": common_metadata["_access"],
            "title": common_metadata["title"],
            "description": common_metadata["description"],
            "contributors": common_metadata["contributors"],
        }
        # Create project
        project_deposit = create_project(project_metadata, submitter)
        project_deposit_id = project_deposit["_deposit"]["id"]

        multiple_video_record = json_data.get("multiple_video_record")

        for record in multiple_video_record:
            # Combine ceph and afs files
            media_files = self._get_files(record["files"], json_data.get("files", []))
            master_file_id = media_files["master_path"].split("/")[-1]

            # Use the correct metadata for each record
            event_id = record.get("event_id")
            url = record.get("url")
            date = record["date"]
            location = record.get("location")

            metadata = common_metadata.copy()
            related_identifiers = list(metadata.get("related_identifiers", []))
            if event_id:
                # Insert event_id at the beginning
                related_identifiers.insert(
                    0,
                    {
                        "scheme": "Indico",
                        "identifier": str(event_id),
                        "relation_type": "IsPartOf",
                    },
                )
            if url:
                url = transform_legacy_urls(url, type="indico")
                url_identifier = {
                    "scheme": "URL",
                    "identifier": url,
                    "relation_type": "IsPartOf",
                }
                if url_identifier not in related_identifiers:
                    related_identifiers.append(url_identifier)

            metadata["related_identifiers"] = related_identifiers
            metadata["date"] = date
            if location:
                metadata["location"] = location

            # Create video and flow
            video_deposit, video_deposit_id, bucket_id, payload = (
                self._create_video_and_flow(
                    project_deposit, metadata, media_files, submitter
                )
            )

            # Run tasks and publish video
            published_video = self._run_tasks_and_publish_video(
                video_deposit,
                video_deposit_id,
                bucket_id,
                payload,
                media_files,
                report_number,
            )

            # Run after publish fixes, only update _created field
            self._after_publish(published_video, entry, legacy_handling=False)

            # Migration state with legacy recid, master file id and new recid
            legacy_recid = entry["record"]["recid"]
            cds_videos_recid = str(published_video["recid"])
            VideosJsonLogger.log_record_redirection(
                legacy_id=legacy_recid,
                legacy_anchor_id=master_file_id,
                cds_videos_id=cds_videos_recid,
            )

        # Publish project
        published_project = publish_project(deposit_id=project_deposit_id)
        # Run after publish fixes
        self._after_publish(published_project, entry)

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
        if pid:
            # If it's migrated, log recids for redirection
            record_pid = PersistentIdentifier.query.filter_by(
                object_uuid=pid.object_uuid,
                pid_type="recid"
            ).one()
            cds_videos_recid = record_pid.pid_value
            VideosJsonLogger.log_record_redirection(
                legacy_id=recid,
                cds_videos_id=cds_videos_recid,
            )
        return pid is not None

    def _should_skip_recid(self, recid):
        """Check if recid should be skipped."""
        if self._have_migrated_recid(recid):
            return True
        return False

    def _load(self, entry):
        """Use the services to load the entries."""

        if entry:
            recid = entry.get("record", {}).get("recid", {})

            if self._should_skip_recid(recid):
                self.migration_logger.add_information(
                    recid, state={"message": "Record already migrated", "value": recid}
                )
                self.migration_logger.finalise_record(recid)
                return
            is_multiple_video_record = (
                entry.get("record", {}).get("json", {}).get("is_multiple_video_record")
            )
            try:
                if not is_multiple_video_record:
                    self.create_publish_single_video_record(entry)
                elif is_multiple_video_record:
                    self.create_publish_multiple_video_record(entry)

                self.migration_logger.finalise_record(recid)
            except (
                UnexpectedValue,
                ManualImportRequired,
                MissingRequiredField,
            ) as e:
                self.migration_logger.add_log(e, record=entry)

    def _cleanup(self, *args, **kwargs):
        """Cleanup the entries."""
        pass


class DummyLoad(Load):
    """Dummy load class."""

    def __init__(
        self,
    ):
        """Constructor."""

    def _load(self, entry):
        """Load."""
        pass

    def _cleanup(self):
        """Cleanup data after loading."""
        pass
