# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos migration load module helper."""


import logging
import os
import shutil
import tempfile
from pathlib import Path

from cds.modules.deposit.api import Project, Video, deposit_video_resolver
from cds.modules.deposit.ext import _create_tags
from cds.modules.flows.api import AVCFlowCeleryTasks, FlowService
from cds.modules.flows.models import FlowMetadata, FlowTaskStatus
from cds.modules.flows.tasks import (
    ExtractFramesTask,
    ExtractMetadataTask,
    TranscodeVideoTask,
)
from cds.modules.invenio_deposit.signals import post_action
from cds.modules.opencast.tasks import _get_opencast_subformat_info
from cds.modules.records.api import CDSVideosFilesIterator
from cds.modules.xrootd.utils import file_opener_xrootd
from celery import chain as celery_chain
from flask import current_app
from invenio_db import db
from invenio_files_rest.models import (
    Bucket,
    FileInstance,
    ObjectVersion,
    ObjectVersionTag,
    as_object_version,
)
from invenio_files_rest.storage import pyfs_storage_factory
from sqlalchemy.orm.attributes import flag_modified as db_flag_modified

from cds_migrator_kit.errors import ManualImportRequired


def copy_file_to_bucket(bucket_id, file_path, is_master=False):
    """Create a FileInstance, move the file to FileInstance storage, return the created object version."""
    logger_files = logging.getLogger("files")

    def _cleanup_on_failure(error_msg):
        """Attempt to delete the file instance and log errors if copy fails."""
        try:
            file.delete()
            file_storage.delete()
        except Exception as cleanup_error:
            logger_files.error(f"[ERROR] Cleanup failed after copy file fail: {cleanup_error}")
        if is_master: # Fail if file is master
            raise ManualImportRequired(error_msg, stage="load")

    try:
        video_bucket = Bucket.get(bucket_id)
        video_name = os.path.basename(file_path)
        file = FileInstance.create()

        # Get the location for the file instance
        default_location = video_bucket.location.uri

        # Get the file storage
        file_storage = file.storage(default_location=default_location)
        fs, path = file_storage._get_fs()
        fs.open(path, mode="wb")
        full_path = Path(file_storage.fileurl.replace("root://eosmedia.cern.ch/", ""))        
        
        # For local migration 
        if not current_app.config["MOUNTED_MEDIA_CEPH_PATH"].startswith("/eos"):
            storage = pyfs_storage_factory(
                fileinstance=file, default_location=default_location
            )
            fp = storage.open(mode="wb")
            full_path = Path(fp.name.decode()).resolve()

        # Check if the destination already exists
        if full_path.exists() and full_path.is_dir() and any(full_path.iterdir()):
            raise FileExistsError(f"{full_path} already exists.")

        # Copy file to storage.
        shutil.copy(file_path, full_path)

        # Control if the file copied succesfully
        if os.path.getsize(file_path) != os.path.getsize(full_path):
            error_message = (
                f"File copy failed: Checksum mismatch! "
                f"Source: {file_path}, Destination: {full_path}"
            )
            if is_master:
                # Fail if it's master
                _cleanup_on_failure(error_message)
            else:
                # Log if it's frames/subformats/additional
                logger_files.warning(f"[WARNING]" + error_message)

        # Update FileInstance
        file_checksum = file_storage.checksum(use_default_impl=True)
        file_size = os.path.getsize(full_path)
        file.set_uri(file_storage.fileurl, file_size, file_checksum)

        # Create object version
        object_version = ObjectVersion.create(
            video_bucket, video_name, _file_id=file.id
        )
        return object_version

    except FileNotFoundError:
        error_msg = f"File '{file_path}' not found."
        logger_files.warning(f"[WARNING]" + error_msg)
        _cleanup_on_failure(error_msg)
    except Exception as e:
        error_msg = f"Error uploading file '{file_path}' to bucket: {e}"
        logger_files.warning(f"[WARNING]" + error_msg)
        _cleanup_on_failure(error_msg)


def create_project(project_metadata, submitter):
    """Create a project with metadata."""
    try:
        # Add submitter
        project_metadata["_access"] = {"update": [submitter.get("email")]}
        project_deposit = Project.create(project_metadata)
        submitter_id = submitter.get("id")

        # Update deposit owners
        project_deposit["_deposit"]["owners"].append(submitter_id)
        project_deposit["_deposit"]["created_by"] = submitter_id
        project_deposit.commit()

        return project_deposit
    except Exception as e:
        raise ManualImportRequired(f"Project creation failed! {e}", stage="load")


def create_video(project_deposit, video_metadata, video_file_path, submitter):
    """Create a video in project with metadata and master video file.

    Returns video_deposit and master_object"""
    try:
        # Create video_deposit
        video_metadata["_project_id"] = project_deposit["_deposit"]["id"]
        video_deposit = Video.create(video_metadata)

        # Update deposit owners
        video_deposit["_deposit"]["owners"] = project_deposit["_deposit"]["owners"]
        video_deposit["_cds"]["modified_by"] = submitter.get("id")
        video_deposit.commit()

        # Copy the master video to bucket
        bucket_id = video_deposit["_buckets"]["deposit"]
    except Exception as e:
        raise ManualImportRequired(f"Video creation failed! {e}", stage="load")

    object_version = copy_file_to_bucket(bucket_id=bucket_id, file_path=video_file_path, is_master=True)

    return video_deposit, object_version


def create_flow(object_version, deposit_id, user_id):
    """Create a payload and flow."""
    try:
        deposit_id = str(deposit_id)
        payload = dict(
            version_id=str(object_version.version_id),
            key=object_version.key,
            bucket_id=str(object_version.bucket_id),
            deposit_id=deposit_id,
        )
        # Create FlowMetadata
        flow_metadata = FlowMetadata.create(
            deposit_id=deposit_id,
            user_id=user_id,
            payload=payload,
        )
        payload["flow_id"] = str(flow_metadata.id)

        flow_metadata.payload = payload
        flow = FlowService(flow_metadata)
        # Flag the change
        db_flag_modified(flow_metadata, "payload")
        return flow, payload
    except Exception as e:
        raise ManualImportRequired(f"Flow creation failed! {e}", stage="load")

def publish_video_record(deposit_id):
    """Publish video record."""
    try:
        # Fetch record
        video_deposit = deposit_video_resolver(str(deposit_id))
        # Publish record
        video_published = video_deposit.publish()
        video_published.commit()

        # Send signal to trigger after_publish actions
        post_action.send(
            current_app._get_current_object(),
            action="publish",
            deposit=video_published,
        )

        db.session.commit()
        return video_published
    except Exception as e:
        raise ManualImportRequired(f"Deposit:{deposit_id} Publish failed: {e}", stage="load")


def extract_metadata(payload):
    """Extract the metadata of the master video file."""
    try:
        celery_task, kwargs = AVCFlowCeleryTasks.create_task(
            ExtractMetadataTask, payload, delete_copied=False
        )
        task_signature = AVCFlowCeleryTasks.create_task_signature(celery_task, **kwargs)
        celery_chain(task_signature).apply()
    except Exception as e:
        raise ManualImportRequired("Metadata extraction failed!", stage="load") from e


def run_frames_task(payload):
    """Create the frames of the master video file."""
    logger_flows = logging.getLogger("flows")
    try:
        celery_task, kwargs = AVCFlowCeleryTasks.create_task(
            ExtractFramesTask, payload, delete_copied=False
        )
        task_signature = AVCFlowCeleryTasks.create_task_signature(celery_task, **kwargs)
        celery_chain(task_signature).apply()
    except Exception as e:
        if celery_task:
            flow_task_metadata = celery_task.get_or_create_flow_task()
            flow_task_metadata.status = FlowTaskStatus.FAILURE
            logger_flows.error(f"[ERROR] ExtractFramesTask failed! Deposit id: {payload['deposit_id']}, flow id: {payload['flow_id']}")


def copy_frames(payload, frame_paths):
    """Load frames for the master video file without running the celery task.

    payload (dict):
        - version_id (str): The version ID of the main video file.
        - key (str): The file name of the main video file.
        - bucket_id (str): The bucket id of the record.
        - deposit_id (str): The deposit id of the record.
        - flow_id (str): The flow id.
    frame_paths (list): A list of file paths of the frames.
    """
    logger_flows = logging.getLogger("flows")
    logger_files = logging.getLogger("files")

    frames_payload = payload.copy()
    if not len(frame_paths) == 10:
        logger_flows.warning(f"[WARNING] Deposit: {frames_payload['deposit_id']} frames are creating with celery task.")
        # Missing/extra frames, don't use them, create with celery task
        run_frames_task(frames_payload)
        return

    version_id = frames_payload["version_id"]
    object_version = as_object_version(version_id)

    frames_task, kwargs = AVCFlowCeleryTasks.create_task(
        ExtractFramesTask, frames_payload)
    frames_task.flow_id = frames_payload.get("flow_id")
    # FramesTask Metadata
    flow_task_metadata = frames_task.get_or_create_flow_task()
    frames_payload["task_id"] = str(flow_task_metadata.id)
    # Update the payload
    flow_task_metadata.payload = frames_task.get_full_payload(**frames_payload)
    flow_task_metadata.status = FlowTaskStatus.STARTED

    # Calculate time positions
    options = frames_task._time_position(
        duration=object_version.get_tags()["duration"],
        frames_start=0,
        frames_end=90,
        frames_gap=10,
    )
    # Needed for creating gif file
    output_folder = None
    try:
        # Create ObjectVersions like ExtractFramesTask._create_frames
        # Since frame files already sorted during transform we can rename:frame-1.jpg
        # Renaming is needed because cds-videos getting the first frame file by it's name(frame-1.jpg) for the poster
        start_time = options.get("start_time")
        time_step = options.get("time_step")
        [frames_task._create_object(
                bucket=object_version.bucket,
                key=f"frame-{i+1}.jpg",
                stream=file_opener_xrootd(filename, "rb"),
                size=os.path.getsize(filename),
                media_type="image",
                context_type="frame",
                master_id=object_version.version_id,
                timestamp=start_time + (i + 1) * time_step,
            )
            for i, filename in enumerate(frame_paths)]

        # Temp folder to create gif file
        output_folder = tempfile.mkdtemp()
        frames_task._create_gif(
            bucket=str(object_version.bucket.id),
            frames=frame_paths,
            output_dir=output_folder,
            master_id=version_id,
        )
        flow_task_metadata.status = FlowTaskStatus.SUCCESS

    except FileNotFoundError as e:
        logger_files.error(
            f"[ERROR] Frame file not found: {e} | Deposit ID: {payload['deposit_id']}, Flow ID: {payload['flow_id']}"
        )
        # Delete frame ObjectVersions
        frames_task.clean(version_id=version_id)
        flow_task_metadata.status = FlowTaskStatus.FAILURE
    except Exception as e:
        logger_flows.error(f"[ERROR] Frame uploading failed: {e}")
        flow_task_metadata.status = FlowTaskStatus.FAILURE
        # Delete frame ObjectVersions
        frames_task.clean(version_id=version_id)
    finally:
        # Cleanup the temp folder if it was created
        if output_folder:
            shutil.rmtree(output_folder, ignore_errors=True)


def _copy_subformat(payload, preset_quality, path):
    """Copy the subformat file to bucket and add subformat tags"""
    obj = copy_file_to_bucket(payload["bucket_id"], path)
    if not obj:
        return
    # Add tags to the subformat
    ObjectVersionTag.create(obj, "master", payload["version_id"])
    ObjectVersionTag.create(obj, "media_type", "video")
    ObjectVersionTag.create(obj, "context_type", "subformat")
    ObjectVersionTag.create(obj, "smil", "true")
    ObjectVersionTag.create(obj, "preset_quality", preset_quality)
    quality_config = current_app.config["CDS_OPENCAST_QUALITIES"][preset_quality]
    if "tags" in quality_config:
        for key, value in quality_config["tags"].items():
            ObjectVersionTag.create(obj, key, value)
    # Get subformat info from the config file
    info = _get_opencast_subformat_info({}, preset_quality)
    for key, value in info.items():
        ObjectVersionTag.create(obj, key, str(value))
    return obj


def transcode_task(payload, subformats):
    """Load subformats for the master video file without running celery task.

    payload (dict):
        - version_id (str): The version ID of the main video file.
        - key (str): The file name of the main video file.
        - bucket_id (str): The bucket id of the record.
        - deposit_id (str): The deposit id of the record.
        - flow_id (str): The flow id.
    subformat_paths (list): A list of file paths of the subformats.
    """
    logger_flows = logging.getLogger("flows")

    for item in subformats:
        path = item["path"]
        preset_quality = item["quality"]
        quality_config = current_app.config["CDS_OPENCAST_QUALITIES"].get(preset_quality)

        # If it's different subformat quality than (360,480,720...) don't upload
        if not quality_config:
            logger_flows.warning(f"[WARNING] Deposit: {payload['deposit_id']} Subformat quality:{preset_quality} not found in config, skipping {path}")
            continue
        # It'll log if fails
        _copy_subformat(payload, preset_quality, path)
        db.session.commit()

    # Create TranscodeVideoTask
    subformat_payload = payload.copy()
    transcode_task, kwargs = AVCFlowCeleryTasks.create_task(
        TranscodeVideoTask, payload)
    object_version = as_object_version(subformat_payload["version_id"])
    transcode_task.object_version = object_version
    transcode_task.flow_id = subformat_payload["flow_id"]
    transcode_task._base_payload = subformat_payload
    flow_tasks = transcode_task._start_transcodable_flow_tasks_or_cancel()

    # Check the master video file has subformats
    video_deposit = deposit_video_resolver(payload["deposit_id"])
    original_file = CDSVideosFilesIterator.get_master_video_file(video_deposit)
    video_subformats = CDSVideosFilesIterator.get_video_subformats(original_file)
    # Update the tasks
    added_qualities = [item["tags"]["preset_quality"] for item in video_subformats]
    for task in flow_tasks:
        preset_quality = task.payload["preset_quality"]
        if preset_quality in added_qualities:
            task.status = FlowTaskStatus.SUCCESS
        else:
            # Update the status as FAILURE if missing
            logger_flows.warning(f"[WARNING] Deposit: {payload['deposit_id']} missing subformat: {preset_quality}!")
            task.status = FlowTaskStatus.FAILURE

    db.session.commit()


def copy_additional_files(bucket_id, additional_files):
    """Load additional files for the master video file."""
    for path in additional_files:
        # Copy the file to FileInstance and create ObjectVersion
        obj = copy_file_to_bucket(bucket_id, path)

        # It'll log if copying fail
        if not obj:
            continue

        # Add tags to the additional file
        ObjectVersionTag.create_or_update(obj, "context_type", "additional_file")
        _create_tags(obj)


