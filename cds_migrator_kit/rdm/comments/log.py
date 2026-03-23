# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-Migrator-Kit is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Migrator-Kit comments logger module."""

import csv
import logging
import os

from flask import current_app


class CommentsLogger:
    """Migrator comments logger."""

    REPORT_COLUMNS = [
        "recid",
        "legacy_comment_url",
        "new_comment_deeplink",
        "status",
        "error_message",
    ]

    REPORT_FILENAME = "comments_migration.csv"

    def __init__(self, log_dir, collection=None):
        """Constructor."""
        self.log_dir = log_dir
        os.makedirs(self.log_dir, exist_ok=True)

        # Initializes logging format and file handlers for logging module (not CSV report).
        formatter = logging.Formatter(
            fmt="%(asctime)s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        logger = logging.getLogger("comments-migrator")
        logger.setLevel(logging.DEBUG)
        # Info to file
        fh_info = logging.FileHandler(self.log_dir / "info.log")
        fh_info.setFormatter(formatter)
        fh_info.setLevel(logging.INFO)
        logger.addHandler(fh_info)
        # Errors to file
        fh = logging.FileHandler(self.log_dir / "error.log")
        fh.setLevel(logging.ERROR)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        # Info to stream/stdout
        sh = logging.StreamHandler()
        sh.setFormatter(formatter)
        sh.setLevel(logging.INFO)
        logger.addHandler(sh)

        if collection:
            self.report_dir = os.path.join(self.log_dir, collection)
            os.makedirs(self.report_dir, exist_ok=True)
            self.report_path = os.path.join(self.report_dir, self.REPORT_FILENAME)
            # CSV report to file
            self._csv_file = open(self.report_path, "a", newline="", encoding="utf-8")
            self._csv_writer = csv.DictWriter(
                self._csv_file, fieldnames=self.REPORT_COLUMNS
            )
            # Write header only if file is empty
            self._csv_file.seek(0, os.SEEK_END)
            if self._csv_file.tell() == 0:
                self._csv_writer.writeheader()
            self._csv_file.flush()
        else:
            self.report_path = None
            self._csv_file = None

    @classmethod
    def get_logger(cls):
        """Get migration logger."""
        return logging.getLogger("comments-migrator")

    @classmethod
    def get_comments_report(cls, collection):
        """Get the comments report for the collection."""
        report_dir = os.path.join(
            current_app.config["CDS_MIGRATOR_KIT_LOGS_PATH"], "comments", collection
        )
        report_path = os.path.join(report_dir, cls.REPORT_FILENAME)
        comments = []
        if os.path.exists(report_path):
            with open(report_path, "r", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    comments.append(row)
        return comments

    def add_comment_log(
        self,
        legacy_recid,
        legacy_comment_id,
        status,
        new_comment_id=None,
        parent_comment_id=None,
        community_slug=None,
        request_id=None,
        error_message=None,
    ):
        """Add a comment record to the migration CSV report.

        :param legacy_recid: Legacy record ID.
        :param legacy_comment_id: Legacy comment ID.
        :param status: Migration status.
        :param new_comment_id: The new system's comment ID.
        :param parent_comment_id: The ID of the parent comment.
        :param community_slug: The slug of the community.
        :param request_id: The ID of the associated request in the new system.
        :param error_message: Error message.
        """
        # Compose links for legacy and new comments
        legacy_comment_url = (
            f"https://cds.cern.ch/record/{legacy_recid}/comments#C{legacy_comment_id}"
        )
        if new_comment_id:
            comment_id = (
                f"{parent_comment_id}_{new_comment_id}"
                if parent_comment_id
                else new_comment_id
            )
            new_comment_deeplink = f"{current_app.config['CDS_MIGRATOR_KIT_SITE_UI_URL']}/communities/{community_slug}/requests/{request_id}#commentevent-{comment_id}"
        else:
            new_comment_deeplink = None

        data = {
            "recid": legacy_recid,
            "legacy_comment_url": legacy_comment_url,
            "new_comment_deeplink": new_comment_deeplink,
            "status": status,
            "error_message": error_message,
        }
        self._csv_writer.writerow(data)

    def finalize(self):
        """Close file handlers."""
        if self._csv_file:
            self._csv_file.close()
