# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos migration load module helper."""


import os
import re
import shutil
import tempfile
from pathlib import Path

from cds.modules.deposit.api import deposit_video_resolver
from cds.modules.flows.api import AVCFlowCeleryTasks
from cds.modules.flows.models import FlowTaskMetadata, FlowTaskStatus
from cds.modules.flows.tasks import (
    ExtractFramesTask,
    TranscodeVideoTask,
)
from cds.modules.opencast.tasks import _get_opencast_subformat_info
from cds.modules.records.api import CDSVideosFilesIterator
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

from cds_migrator_kit.errors import ManualImportRequired


def load_file_to_bucket(bucket_id, file_path):
    """Create a FileInstance, move the video file to FileInstance storage, return the created object version."""
    try:
        video_bucket = Bucket.get(bucket_id)
        video_name = os.path.basename(file_path)
        file = FileInstance.create()

        # Get the location for the file instance
        default_location = video_bucket.location.uri
        file_storage = pyfs_storage_factory(
            fileinstance=file, default_location=default_location
        )
        fp = file_storage.open(mode="wb")
        full_path = Path(fp.name.decode()).resolve()

        # Copy file to storage.
        shutil.copy2(file_path, full_path)

        # Update FileInstance
        file_size = os.path.getsize(file_storage.fileurl)
        file_checksum = file_storage.checksum()
        file.set_uri(file_storage.fileurl, file_size, file_checksum)

        # Create object version
        object_version = ObjectVersion.create(
            video_bucket, video_name, _file_id=file.id
        )
        return object_version

    except FileNotFoundError:
        raise ManualImportRequired(f"File '{video_name}' not found.", stage="load")
    except Exception as e:
        raise ManualImportRequired(
            f"Error uploading file to bucket'{video_name}': {e}", stage="load"
        )


def load_frames(payload, frame_paths):
    """Load frames for the master video file without running the celery task.

    payload (dict):
        - version_id (str): The version ID of the main video file.
        - key (str): The file name of the main video file.
        - bucket_id (str): The bucket id of the record.
        - deposit_id (str): The deposit id of the record.
        - flow_id (str): The flow id.
    frame_paths (list): A list of file paths of the frames.
    """
    # TODO sort frames_list?
    try:
        version_id = payload["version_id"]
        object_version = as_object_version(version_id)

        frames_task, kwargs = AVCFlowCeleryTasks.create_task(
            ExtractFramesTask, payload, delete_copied=False
        )
        frames_task.flow_id = payload.get("flow_id")
        # FramesTask Metadata
        flow_task_metadata = frames_task.get_or_create_flow_task()
        flow_task_metadata.status = FlowTaskStatus.STARTED

        # Calculate time positions
        options = frames_task._time_position(
            duration=object_version.get_tags()["duration"],
            frames_start=0,  # TODO they should be changed with using the frame file name?
            frames_end=90,
            frames_gap=10,
        )
        frames = frames_task._create_frames(
            frame_paths,
            object_version,
            options.get("start_time"),
            options.get("time_step"),
        )
        output_folder = tempfile.mkdtemp()
        frames_task._create_gif(
            bucket=str(object_version.bucket.id),
            frames=frames,
            output_dir=output_folder,
            master_id=version_id,
        )
        flow_task_metadata.status = FlowTaskStatus.SUCCESS

        # Cleanup
        shutil.rmtree(output_folder)
    except Exception:
        shutil.rmtree(output_folder, ignore_errors=True)
        frames_task.clean(version_id=version_id)
        raise ManualImportRequired("Frame uploading failed!", stage="load")


def load_subformats(payload, subformat_paths):
    """Load subformats for the master video file without celery task.

    payload (dict):
        - version_id (str): The version ID of the main video file.
        - key (str): The file name of the main video file.
        - bucket_id (str): The bucket id of the record.
        - deposit_id (str): The deposit id of the record.
        - flow_id (str): The flow id.
    subformat_paths (list): A list of file paths of the subformats.
    """
    # TODO should we need to check all the subformats exists? If missing should we generate?
    # TODO maybe logging is enough?

    # Create FlowTaskMetadata
    subformat_payload = payload.copy()
    transcode_task = FlowTaskMetadata.create(
        flow_id=payload["flow_id"],
        name=TranscodeVideoTask.name,
        payload=subformat_payload,
    )

    transcode_task.status = FlowTaskStatus.STARTED

    for path in subformat_paths:
        video_name = os.path.basename(path)
        preset_quality = None

        # TODO this regex might be changed for the composite subformats
        # Get quality from file name
        match = re.search(r"-(\d{3,4}p)-quality\.[a-zA-Z0-9]+$", video_name)
        if match:
            preset_quality = match.group(1)  # Extracts the quality part (e.g. "480p")
        if not preset_quality:
            raise ManualImportRequired(f"Subformat quality not found.", stage="load")

        # Copy the file to FileInstance and create ObjectVersion
        obj = load_file_to_bucket(payload["bucket_id"], path)

        # Add tags to the subformat
        ObjectVersionTag.create(obj, "master", payload["version_id"])
        ObjectVersionTag.create(obj, "media_type", "video")
        ObjectVersionTag.create(obj, "context_type", "subformat")
        ObjectVersionTag.create(obj, "smil", "true")
        ObjectVersionTag.create(obj, "_opencast_event_id", "")  # TODO is it needed?
        ObjectVersionTag.create(
            obj, "_opencast_file_download_time_in_seconds", ""
        )  # TODO is it needed?
        ObjectVersionTag.create(obj, "preset_quality", preset_quality)
        qualitiy_config = current_app.config["CDS_OPENCAST_QUALITIES"][preset_quality]
        if "tags" in qualitiy_config:
            for key, value in qualitiy_config["tags"].items():
                ObjectVersionTag.create(obj, key, value)
        # Get subformat info from the config file
        info = _get_opencast_subformat_info({}, preset_quality)
        for key, value in info.items():
            ObjectVersionTag.create(obj, key, str(value))

    # Check the master video file has subformats
    # TODO it can be improved to check if missing any possible subformat
    video_deposit = deposit_video_resolver(payload["deposit_id"])
    original_file = CDSVideosFilesIterator.get_master_video_file(video_deposit)
    subformats = CDSVideosFilesIterator.get_video_subformats(original_file)
    if subformats:
        transcode_task.status = FlowTaskStatus.SUCCESS
        db.session.add(transcode_task)
        db.session.commit()
    else:
        transcode_task.status = FlowTaskStatus.FAILURE
        raise ManualImportRequired(f"Subformat load failed.", stage="load")
