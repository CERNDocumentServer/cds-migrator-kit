# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos file copy script."""
import argparse
import datetime
import io
import json
import logging
import os
import re
import subprocess
import sys
from collections import defaultdict

from pathlib2 import Path


class CDSMigrationException(Exception):
    """CDSDoJSONException class."""

    description = None

    def __init__(
        self,
        message=None,
        field=None,
        subfield=None,
        value=None,
        stage=None,
        recid=None,
        exc=None,
        priority=None,
        *args,
        **kwargs
    ):
        """Constructor."""
        self.subfield = subfield
        self.field = field
        self.value = value
        self.stage = stage
        self.recid = recid
        self.type = str(self.__class__.__name__)
        self.exc = exc
        self.message = message
        self.priority = priority
        super(CDSMigrationException, self).__init__(*args)


class TransformFiles:
    """
    Transform lecturemedia links to eos file paths for video record.

    Initialize the class and call the `transform` method.
    """

    def __init__(
        self,
        recid,
        entry_files,
        logger_files,
        collection="weblectures",
        media_folder="",
    ):
        """Constructor."""
        self.recid = recid
        self.entry_files = entry_files
        self.logger_files = logger_files
        self.collection = collection
        self.media_folder = media_folder
        self.transformed_files_json = {}
        # composite str
        self.composite_str = "composite"
        # use_composite is true, if master folder has presenter and presentation
        self.use_composite = False
        self.record_media_data_folder = None

    def _get_master_path(self):
        master_paths = [
            item["master_path"] for item in self.entry_files if "master_path" in item
        ]
        # It should have one master_path
        if len(master_paths) != 1:
            raise CDSMigrationException(
                message="Multiple/missing master files!",
                stage="transform",
                recid=self.recid,
                value="master_path",
                priority="critical",
            )
        return master_paths[0]

    def _get_all_files_in_folder(self, directory):
        """Get all files in a folder."""
        # Get all the files, ignore hidden files
        try:
            return [
                f.name
                for f in Path(directory).iterdir()
                if f.is_file() and not f.name.startswith(".")
            ]
        except (IOError, OSError):
            # No files are found, raise an error
            raise CDSMigrationException(
                message="Folder couldn't be found!",
                stage="transform",
                recid=self.recid,
                value=directory,
                priority="critical",
            )

    def _add_files_to_file_json(self, json_key, folder, files_list):
        """Append the files to the transformed file_info_json."""
        for file in files_list:
            # Subformats
            if isinstance(file, dict):
                # Handle dict format (path + quality)
                self.transformed_files_json[json_key].append(
                    {"path": str(folder / file["path"]), "quality": file["quality"]}
                )
            # Frames, additional files
            else:
                # Handle str format (just the file path)
                self.transformed_files_json[json_key].append(str(folder / file))

    def _check_composite_exists(self, file_paths, streams_length):
        """Check if the folder contains `composite` files."""
        for file_path in file_paths:
            file_name = Path(file_path).stem.lower()
            if self.composite_str in file_name and streams_length == 2:
                return True
        return False

    def _frames_exists(self, frames_list):
        """Check if there's exactly 10 frames."""
        if len(frames_list) == 10:
            return True
        # Frames will be generated
        return False

    def _get_highest_and_other_composites(self, file_list):
        """
        Find and return the highest quality composite video (1080p) and subformats (720p, 480p, 360p).

        Composite videos will always be inside the media_data folder.
        """

        required_resolutions = {1080, 720, 480, 360}
        composite_videos = {}

        # Extract composite videos and their resolutions
        for file in file_list:
            match = re.search(r"composite-(\d+)p-quality", file)
            if match:
                resolution = int(match.group(1))
                if resolution in required_resolutions:
                    composite_videos[resolution] = file

        if not composite_videos:
            raise CDSMigrationException(
                message="Composite videos are missing!!",
                stage="transform",
                recid=self.recid,
                value=self.record_media_data_folder,
                priority="critical",
            )

        # Log missing subformats
        missing_resolutions = required_resolutions - set(composite_videos.keys())
        if missing_resolutions:
            self.logger_files.warning(
                "Folder:%s missing composite subformats: %s",
                self.record_media_data_folder,
                sorted(missing_resolutions),
            )

        # Sort composite videos by resolution (descending order)
        sorted_composites = sorted(
            composite_videos.items(), key=lambda x: x[0], reverse=True
        )

        # Get the highest quality
        highest_quality_composite = sorted_composites[0][1]
        self.transformed_files_json["master_quality"] = sorted_composites[0][0]

        # The rest are the other composites sorted by resolution
        other_composites = [
            {"path": composite[1], "quality": "{0}p".format(composite[0])}
            for composite in sorted_composites[1:]
        ]

        return highest_quality_composite, other_composites

    def _set_composite_files(self, all_files):
        """Find all the composite files and add them to transformed file_info_json."""
        # Find the master and subformat composites
        master_composite, subformats = self._get_highest_and_other_composites(
            file_list=all_files
        )

        # Add the master composite to file_info_json
        self.transformed_files_json["master_video"] = str(
            self.record_media_data_folder / master_composite
        )

        # Add subformats to file_info_json
        self._add_files_to_file_json(
            json_key="subformats",
            folder=self.record_media_data_folder,
            files_list=subformats,
        )

        # Frames (generated using composite)
        frame_folder = self.record_media_data_folder / "frames"

        # Frame folder is missing! Log it
        if not frame_folder.is_dir():
            self.logger_files.warning(
                "Record:%s frames folder for composite is missing:%s",
                self.recid,
                frame_folder,
            )
            return

        frames_list = self._get_all_files_in_folder(frame_folder)
        # Sort frames eg:"frame-1.jpg" to "frame-10.jpg"
        sorted_frames = sorted(
            frames_list, key=lambda f: int(re.search(r"frame-(\d+)", f).group(1))
        )
        # Check if missing/extra frames
        if self._frames_exists(sorted_frames):
            # Add frames to file_info_json
            self._add_files_to_file_json(
                json_key="frames", folder=frame_folder, files_list=sorted_frames
            )

    def _check_files_in_media_data_folder(self, files_paths):
        """
        Check and return all the files in media_data_folder.

        files_paths: Founded paths from the record marcxml.
        """
        # No path found in the record
        if not files_paths:
            raise CDSMigrationException(
                message="No media file found in the record!",
                stage="transform",
                value=self.record_media_data_folder,
                recid=self.recid,
                priority="critical",
            )

        # Get all files (even the subfolder files) in media_data folder
        all_files = {
            file.name
            for file in Path(self.record_media_data_folder).rglob("*")
            if file.is_file()
        }
        # No file found in media_data folder
        if not all_files:
            raise CDSMigrationException(
                message="No file found in the media_data folder!",
                value=self.record_media_data_folder,
                stage="transform",
                recid=self.recid,
                priority="critical",
            )

        # Check all files_paths exists in media_data folder
        for path in files_paths:
            file_name = Path(path).name
            if file_name not in all_files:
                self.logger_files.error(
                    "File not found in the media_data folder: {0}!".format(file_name)
                )
        return all_files

    def _get_highest_and_subformats_from_datajson(self, datajson):
        """Get the highest quality presenter/presentation file, and subformats."""
        highest_quality_videos = []
        all_subformats = []
        streams = datajson.get("streams", [])
        # One video record should have 1 stream
        if not self.use_composite and len(streams) != 1:
            raise CDSMigrationException(
                "Missing presenter/presentation files for composite record!"
            )
        try:
            for stream in streams:
                subformats = stream.get("sources", {}).get("mp4", [])
                if not subformats:
                    raise CDSMigrationException(
                        "Missing MP4 formats in one of the streams"
                    )

                if len(subformats) == 1:
                    # TODO is there any better solution to migrate these records?
                    # Only one video — no need to sort
                    highest_quality_videos.append(subformats[0]["src"].strip("/"))
                    if not self.use_composite:
                        # If not composite, set the master quality from the only subformat
                        self.transformed_files_json["master_quality"] = (
                            subformats[0].get("res", {}).get("h")
                        )
                    continue

                sorted_subformats = sorted(
                    subformats,
                    key=lambda x: int(x.get("res", {}).get("h", 0)),
                    reverse=True,
                )
                # Add highest quality video path
                highest_quality_videos.append(sorted_subformats[0]["src"].strip("/"))
                # Add the subformats
                for file in sorted_subformats[1:]:
                    res = file.get("res", {})
                    all_subformats.append(
                        {
                            "path": file["src"].strip("/"),
                            "quality": "{0}p".format(res["h"]),
                        }
                    )
                if not self.use_composite:
                    # If not composite, set the master quality from the highest subformat
                    self.transformed_files_json["master_quality"] = (
                        sorted_subformats[0].get("res", {}).get("h")
                    )
        except Exception as e:
            raise CDSMigrationException(
                message=(
                    "Subformat transform failed! Check data.v2.json: {0}. Error: {1}".format(
                        self.record_media_data_folder, e
                    )
                ),
                stage="transform",
                priority="critical",
            )

        return highest_quality_videos, all_subformats

    def _set_media_files(self, files_paths):
        """
        Check the media_data folder with the file paths found in recod marcxml and set the paths to file_info_json.

        files_paths: file paths comes from the record.
        media_data_folder includes composite file, vtt files, subformats, frames.
        """
        # Check the folder and all files exists in the folder
        all_files = self._check_files_in_media_data_folder(files_paths)

        try:
            # Read "data.v2.json"
            data_v2_json = self.record_media_data_folder / "data.v2.json"
            with io.open(str(data_v2_json), "r", encoding="utf-8") as file:
                data = json.load(file)
        except (IOError, OSError):
            raise CDSMigrationException(
                message="data_v2_json file not found!",
                stage="transform",
                recid=self.recid,
                value=data_v2_json,
                priority="critical",
            )
        self.data_json = str(data_v2_json)

        # Check if the record_media_data folder has the composite video
        self.use_composite = self._check_composite_exists(
            all_files, len(data.get("streams", []))
        )

        # Get master and subformats in data.v2.json
        highest_presenter_presentation, subformats = (
            self._get_highest_and_subformats_from_datajson(data)
        )

        # Extract frame list in data.v2.json and sort by time
        frames_list = sorted(
            data.get("frameList", []), key=lambda frame: frame.get("time", 0)
        )

        # Use the composite, composite exists
        if self.use_composite:
            # Find and set the all composite files in the folder: (Master, subformats, frames)
            self._set_composite_files(all_files=all_files)

            # Add presenter and presentation as additinal video
            self._add_files_to_file_json(
                json_key="additional_files",
                folder=self.media_folder,
                files_list=highest_presenter_presentation,
            )

        # We dont have the composite we'll get the main files from the folder
        else:
            # ~~~~MASTER & SUBFORMATS
            if len(highest_presenter_presentation) != 1:
                raise CDSMigrationException("Master video file is missing!")
            self.transformed_files_json["master_video"] = str(
                self.media_folder / highest_presenter_presentation[0]
            )

            # Add them as subformats
            self._add_files_to_file_json(
                json_key="subformats", folder=self.media_folder, files_list=subformats
            )

            # ~~~~FRAMES~~~~
            # Check missing or extra frames
            if self._frames_exists(frames_list):
                self._add_files_to_file_json(
                    json_key="frames",
                    folder=self.media_folder,
                    files_list=[
                        frame["url"].strip("/") for frame in frames_list
                    ],  # Get the paths
                )

        # Duration
        self.transformed_files_json["duration"] = data.get("metadata", {}).get(
            "duration", 0
        )

        # ~~~~Chapters~~~~
        chapters = [frame["time"] for frame in frames_list if "time" in frame]
        self.transformed_files_json["chapters"] = chapters

        # ~~~~SUBTITLES~~~~
        # Get subtitles from data.v2.json
        subtitles = [item["url"].strip("/") for item in data.get("captions", [])]
        self._add_files_to_file_json(
            json_key="additional_files", folder=self.media_folder, files_list=subtitles
        )

        # Exclude already added files from additional files
        subformat_paths = [item["path"] for item in subformats]
        already_added_files = set(
            subformat_paths + highest_presenter_presentation + subtitles
        )
        additional_files = [
            file
            for file in files_paths
            if file not in already_added_files and Path(file).name in all_files
        ]

        # Add additional files
        self._add_files_to_file_json(
            json_key="additional_files",
            folder=self.media_folder,
            files_list=additional_files,
        )

    def _set_poster_image(self):
        """Set the poster image if exists."""
        # Find the poster
        poster_path = None
        poster_entry = None
        for entry in self.entry_files:
            file_type = entry.get("type", "").lower()
            if file_type and file_type == "pngthumbnail":
                poster_path = entry.get("path", "").strip("/")
                poster_entry = entry  # Store the entry to remove later
                break
        if poster_path:
            poster_image = self.media_folder / poster_path
            if poster_image.is_file():
                self.transformed_files_json["poster"] = str(poster_image)
                if poster_entry:
                    # Remove the poster from entry_files to avoid duplication in additional_files
                    self.entry_files.remove(poster_entry)

    def transform(self):
        """Transform the files for the record."""
        # Initialize the output json
        self.transformed_files_json = {
            "master_video": "",  # Full path of the master video file
            "additional_files": [],
            "frames": [],
            "subformats": [],
            "chapters": [],
            "master_path": "",  # Master folder needed for multi video records
            "poster": "",  # Poster image path if exists
            "duration": 0,  # Duration in seconds (from data.v2.json)
            "master_quality": "",
            "year": None,
        }
        try:
            # Get master path from the record
            master_path = self._get_master_path()
            self.transformed_files_json["master_path"] = master_path

            # year
            after_master_data = master_path.split("master_data/")[1]
            year_str = after_master_data.split("/")[0]
            self.transformed_files_json["year"] = int(year_str)

            # Get the year and id eg: master_data/year/event_id
            path = master_path.split("master_data/", 1)[-1]

            self.record_media_data_folder = self.media_folder / path

            self._set_poster_image()

            # Get the paths of the files (comes from the record marcxml)
            path_files = [
                item["path"].strip("/") for item in self.entry_files if "path" in item
            ]
            # Check and set the media_files checking the
            self._set_media_files(path_files)

            master_quality = self.transformed_files_json["master_quality"]
            if not master_quality or master_quality == "None":
                self.logger_files.error(
                    "Recid:{0} master_quality is missing!".format(self.recid)
                )

            if not self.transformed_files_json["duration"]:
                self.transformed_files_json["duration"] = 0

            return self.transformed_files_json

        except Exception as e:
            # Try to extract message and value from custom exceptions
            message = getattr(e, "message", str(e))
            value = getattr(e, "value", None)

            log_msg = "Transform failed for record {0}: {1}".format(self.recid, message)
            if value:
                log_msg += " | value: {0}".format(value)

            self.logger_files.error(log_msg, exc_info=True)

    def copy_needed_files(self, new_base, is_master=False):
        """
        Copy all needed files to new_base (e.g. /eos/media/cds-videos/dev/stage/media_data)
        and return a transformed_files_json with updated destination paths.
        """
        transformed = self.transformed_files_json.copy()

        def copy_and_update(src_path_str):
            """Copy file if needed and return new dest path as string."""
            if not src_path_str:
                return ""

            src_path = Path(src_path_str)
            if not src_path.is_file():
                if is_master:
                    self.logger_files.error("Master video missing: %s", src_path)
                else:
                    self.logger_files.error("Missing source: %s", src_path)
                return None

            try:
                rel_path = src_path.relative_to(self.media_folder)
            except ValueError:
                self.logger_files.error("[SKIP] Not under media_folder: %s", src_path)
                return None

            dest_path = new_base / rel_path
            dest_path.parent.mkdir(parents=True, exist_ok=True)

            # Check if file already copied and complete
            if dest_path.exists():
                try:
                    src_stat = src_path.stat()
                    dst_stat = dest_path.stat()
                    if (
                        src_stat.st_size == dst_stat.st_size
                        and dst_stat.st_mtime >= src_stat.st_mtime
                    ):
                        self.logger_files.info(
                            "[SKIP] Already identical: %s", dest_path
                        )
                        return str(dest_path)
                except Exception as e:
                    self.logger_files.warning(
                        "[CHECK] Size/mtime failed for %s: %s", dest_path, e
                    )

            try:
                rsync_copy(src_path_str, str(dest_path), self.logger_files)
            except subprocess.CalledProcessError as e:
                self.logger_files.error("rsync failed: %s", e)
            except Exception as e:
                self.logger_files.error(
                    "Copy failed: %s -> %s (%s)", src_path, dest_path, e
                )
            return str(dest_path)

        # --- Update each field ---
        transformed["master_video"] = copy_and_update(
            self.transformed_files_json["master_video"]
        )
        transformed["poster"] = copy_and_update(
            self.transformed_files_json.get("poster")
        )

        # For list fields
        def update_list_field(field):
            updated = []
            for item in self.transformed_files_json.get(field, []):
                if isinstance(item, dict):
                    new_path = copy_and_update(item.get("path", ""))
                    if new_path:
                        updated.append(
                            {"path": new_path, "quality": item.get("quality")}
                        )
                else:
                    new_path = copy_and_update(item)
                    if new_path:
                        updated.append(new_path)
            transformed[field] = updated

        for key in ["frames", "subformats", "additional_files"]:
            update_list_field(key)

        # Copy the data.v2.json in case if needed during migration
        copy_and_update(self.data_json)

        self.logger_files.info("Copy process finished for record %s", self.recid)
        return transformed


def load_json(json_file_path):
    path = Path(json_file_path)
    with io.open(str(path), "r", encoding="utf-8") as fp:
        return json.load(fp)


def rsync_copy(src_path, dst_path, logger):
    """
    Copy using rsync from CephFS to EOS.
    """
    cmd = ["rsync", "-ah", "--progress", src_path, dst_path]
    try:
        subprocess.check_call(cmd)
        logger.info("[OK] Copied %s", src_path)
        return True
    except subprocess.CalledProcessError as e:
        logger.error("[FAIL] rsync failed: %s (%s)", src_path, e)
        return False


def process_json_file(
    json_path, ceph_media_folder, eos_media_folder, output_folder, before_year
):
    """Process a single JSON file and produce corresponding output + log file."""
    base_name = os.path.basename(json_path)
    prefix = os.path.splitext(base_name)[0].replace("marc_", "")

    log_file = os.path.join(output_folder, "copy_log_{0}.txt".format(prefix))
    transformed_files_path = os.path.join(output_folder, "eos_{0}.json".format(prefix))

    # === Setup logger ===
    logger_files = logging.getLogger(prefix)
    logger_files.setLevel(logging.INFO)
    handler = logging.FileHandler(log_file, mode="a")
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    logger_files.addHandler(handler)

    logger_files.info(
        "=== Starting Transform + Copy Process for {0} ===".format(base_name)
    )

    # === Folder Access Check ===
    for folder in [ceph_media_folder, eos_media_folder]:
        folder_str = str(folder)
        if os.path.exists(folder_str) and os.access(folder_str, os.R_OK):
            logger_files.info("Access OK: {0}".format(folder_str))
        else:
            logger_files.error("Cannot access folder: {0}".format(folder_str))
            logger_files.error(
                "Stopping execution — required folder missing or not readable."
            )
            sys.exit(1)

    # === Load Records ===
    records = load_json(json_path)
    total = len(records)
    all_records_data = defaultdict(list)

    for idx, record in enumerate(records, 1):
        recid = record["recid"]
        logger_files.info(
            "Processing record {0}/{1} (recid={2})".format(idx, total, recid)
        )
        entry_files = record["files"]
        transform_files = TransformFiles(
            recid=recid,
            entry_files=entry_files,
            logger_files=logger_files,
            media_folder=ceph_media_folder,
        )
        file_info_json = transform_files.transform()
        if not file_info_json:
            continue
        if before_year and file_info_json["year"] >= before_year:
            logger_files.info(
                "Skipping record! Year: {0} is greater or equal to {1}".format(
                    file_info_json.get("year"), before_year
                )
            )
            continue
        # Destination
        copied_file_info_json = transform_files.copy_needed_files(eos_media_folder)
        all_records_data[recid].append(copied_file_info_json)

    # Save to a JSON file
    with io.open(transformed_files_path, "w", encoding="utf-8") as f:
        text = json.dumps(all_records_data, indent=4, ensure_ascii=False)
        if not isinstance(text, unicode):
            text = text.decode("utf-8")
        f.write(text)

    logger_files.info("=== Finished processing {0} ===".format(base_name))
    logger_files.info("Output written to {0}".format(transformed_files_path))
    logger_files.info("Log written to {0}".format(log_file))
    handler.close()
    logger_files.removeHandler(handler)


def main():
    parser = argparse.ArgumentParser(
        description="Transform and copy JSON records from a dumps folder."
    )
    parser.add_argument(
        "folder", help="Path to the folder containing marc_files_*.json dumps"
    )
    parser.add_argument(
        "--ceph-media-folder",
        default="/mnt/cephfs/media_data/",
        help="Path to CEPh media folder",
    )
    parser.add_argument(
        "--eos-media-folder",
        default="/eos/media/cds-videos/dev/stage/media_data",
        help="Path to EOS media folder",
    )
    parser.add_argument(
        "--output-folder",
        default="/tmp/copied_files/",
        help="Path to write logs and output JSONs",
    )
    parser.add_argument(
        "--before-year",
        default=None,
        type=int,
        help="Copy the files before a certain year (integer)",
    )

    args = parser.parse_args()
    folder = args.folder

    if not os.path.isdir(folder):
        print("Error: Folder does not exist: {0}".format(folder))
        sys.exit(1)

    # Iterate over JSON files
    for fname in os.listdir(folder):
        if fname.startswith("marc_") and fname.endswith(".json"):
            json_path = os.path.join(folder, fname)
            process_json_file(
                json_path,
                Path(args.ceph_media_folder),
                Path(args.eos_media_folder),
                args.output_folder,
                args.before_year,
            )


if __name__ == "__main__":
    main()
