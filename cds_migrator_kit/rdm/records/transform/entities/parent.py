# -*- coding: utf-8 -*-
#
# Copyright (C) 2022-2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""The RDM parent record for one migrated CDS record."""
import re

from flask import current_app
from invenio_accounts.models import User
from sqlalchemy.exc import NoResultFound

from cds_migrator_kit.errors import ManualImportRequired, UnexpectedValue
from cds_migrator_kit.rdm.records.transform.mappers.base import RecordTransformContext
from cds_migrator_kit.rdm.records.transform.mappers.record import AccessGrantsMapper

EMAIL_PATTERN = re.compile(r"[^@]+@[^@]+\.[^@]+")


class RecordParent:
    """The parent record for one migrated CDS record.

    Owns everything needed to build the RDM parent's access, community
    membership, and access grants - built by ``build()``, called from
    ``CDSToRDMRecordTransform._transform()`` (the one place that has both
    the already-built record content and the DOJSON-processed
    ``dojson_entry`` that access-grant resolution needs).
    """

    def __init__(
        self, record, raw_dump_entry, dojson_entry, communities_ids, access_grants_view
    ):
        """Constructor.

        :param record: the already-built ``RecordEntry`` (needs ``recid``).
        :param raw_dump_entry: the original harvested legacy entry (needs
            ``recid``, for error reporting).
        :param dojson_entry: the DOJSON-processed record data - owner/
            communities/access grants are popped straight off it
            (``submitter``/``communities``/``access_grants``) - this is the
            last entity to touch it, so it's also where those keys stop
            existing for the forgotten-keys check in
            ``CDSToRDMRecordTransform._check_forgotten_keys()``.
        :param communities_ids: configured target community ids for this
            migration run (``CDSToRDMRecordTransform.communities_ids``).
        :param access_grants_view: configured collection-wide view grants
            (``CDSToRDMRecordTransform.access_grants_view``).
        """
        self.record = record
        self.raw_dump_entry = raw_dump_entry
        self.dojson_entry = dojson_entry
        self.communities_ids = communities_ids
        self.access_grants_view = access_grants_view
        self.body = None
        self.communities = None
        self.access_grants = None

    def build(self):
        """Populate ``body``/``communities``/``access_grants``; returns self."""
        access = self._build_access()
        self.communities = self._build_communities()
        self.access_grants = self._build_access_grants_from_record_marc()
        self.body = {
            # loader is responsible for creating/updating if the PID exists,
            # this part will be simply omitted.
            "id": f"{self.record.recid}-parent",
            "access": access,
            "communities": self.communities,
        }
        return self

    def _build_access(self):
        """Resolve the owner and return the parent's access dict."""
        email = self.dojson_entry.pop("submitter", None)
        if not email:
            owner = "system"
        else:
            try:
                user = User.query.filter_by(email=email).one()
                owner = user.id
            except NoResultFound:
                raise UnexpectedValue(
                    message=f"{email} not found - did you run user migration?",
                    stage="transform",
                    recid=self.raw_dump_entry["recid"],
                    value=email,
                    priority="critical",
                )
        return {"owned_by": {"user": owner}}

    def _build_communities(self):
        """Combine the configured target communities with the record's own."""
        communities = self.dojson_entry.pop("communities", [])
        communities = self.communities_ids + [slug for slug in communities]
        if communities:
            return {"ids": communities, "default": self.communities_ids[0]}
        return {}

    def _build_access_grants_from_record_marc(self):
        """Compute the access grants to create on this parent after publish."""
        ctx = RecordTransformContext(
            dojson_entry=self.dojson_entry,
            raw_dump_entry=self.raw_dump_entry,
            access_grants_view=self.access_grants_view,
        )
        return AccessGrantsMapper().map_value(ctx)

    def resolve_grants(self, specific_file_restrictions=""):
        """Resolve which groups/emails/permissions get access grants for a version.

        Combines this parent's own ``access_grants`` (record-level, from
        legacy access-grant metadata) with ``specific_file_restrictions`` (a
        version-specific file-restriction status string, e.g. "firerole:
        allow group ..."). Pure computation - the caller (``load.py``) is
        responsible for actually creating the grants against the RDM parent.

        :param specific_file_restrictions: the ``meta`` value from a
            version's access dict (``VersionEntry["access"]["meta"]``), or
            "" if that version has no individual file restriction.
        :return: ``(groups, emails, grants_with_perms)`` - see
            ``load.py::_after_publish_load_parent_access_grants()`` for how
            these are consumed to create the actual grants.
        """
        default_permission = "view"
        groups = set()
        emails = set()
        grants_with_perms = {}

        # ----Parse file status metadata----#
        if specific_file_restrictions:
            group_mappings = current_app.config.get("CDS_ACCESS_GROUP_MAPPINGS", {})

            if specific_file_restrictions in group_mappings:
                try:
                    groups.update(group_mappings[specific_file_restrictions])
                except KeyError:
                    raise ManualImportRequired(
                        message="Missing permission mapping",
                        field="access",
                        subfield="subject.id",
                        stage="load",
                        recid=self.record.recid,
                        priority="critical",
                        value=specific_file_restrictions,
                    )
            elif specific_file_restrictions == "restricted":
                # https://cds.cern.ch/admin/webaccess/webaccessadmin.py/showroledetails?id_role=69
                groups.add("cern-personnel")
            elif specific_file_restrictions.strip().endswith("[CERN]") and not any(
                kw in specific_file_restrictions for kw in ("firerole:", "allow ")
            ):
                # bare CERN e-group name, e.g.
                # "cds-ph-ep-publications-referee-non-lhc [CERN]"
                groups.add(self._normalize_group_name(specific_file_restrictions))
            else:
                if not any(
                    kw in specific_file_restrictions
                    for kw in ("firerole: allow group", "allow email")
                ):
                    raise ManualImportRequired(
                        message="Unexpected permission format.",
                        field="access",
                        subfield="subject.id",
                        stage="load",
                        recid=self.record.recid,
                        priority="critical",
                        value=specific_file_restrictions,
                    )

                meta_str = specific_file_restrictions.replace("\r\n", "\n")

                # Parse groups
                group_matches = re.search(
                    r'allow group\s+((?:"[^"]+",?\s*)+)', meta_str
                )
                if group_matches:
                    group_values = re.findall(r'"([^"]+)"', group_matches.group(1))
                    for g in group_values:
                        groups.add(self._normalize_group_name(g))

                # Parse emails
                email_matches = re.search(
                    r'allow email\s+((?:"[^"]+",?\s*)+)', meta_str
                )
                if email_matches:
                    email_values = re.findall(r'"([^"]+)"', email_matches.group(1))
                    emails.update(email_values)

        # ----Parse record access grants----#
        for grant_info in self.access_grants:
            if not isinstance(grant_info, dict) or not grant_info:
                continue

            subject, permission = next(iter(grant_info.items()))
            permission = permission or default_permission
            grants_with_perms[subject] = permission

            # attention!
            # this is important - if there was no specific restrictions on the file,
            # then the record grands takes over - but if file had specific status,
            # then we take the least possible access
            if not specific_file_restrictions:
                if EMAIL_PATTERN.match(subject):
                    emails.add(subject)
                else:
                    groups.add(self._normalize_group_name(subject))

        return groups, emails, grants_with_perms

    @staticmethod
    def _normalize_group_name(subject):
        if subject.endswith(" [CERN]"):
            subject = subject.replace(" [CERN]", "")
        return subject.strip()
