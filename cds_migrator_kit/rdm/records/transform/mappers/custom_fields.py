# -*- coding: utf-8 -*-
#
# Copyright (C) 2022-2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""``custom_fields`` mappers for CDS to RDM record transformation."""

from cds_migrator_kit.errors import (
    MissingRequiredField,
    RecordFlaggedCuration,
    UnexpectedValue,
)
from cds_migrator_kit.rdm.records.transform.config import EXPERIMENT_ALIASES
from cds_migrator_kit.rdm.records.transform.mappers.base import CustomFieldMapper
from cds_migrator_kit.rdm.records.transform.mappers.vocabulary import search_vocabulary


class ExperimentsMapper(CustomFieldMapper):
    """Sets cern:experiments.

    Raises ``UnexpectedValue`` (a hard, whole-record failure) for the first
    unmatched experiment name, having first added it as a subject fallback
    so it's visible if the raw dump is inspected.
    """

    def apply(self, ctx):
        """Set ctx.custom_fields["cern:experiments"]."""
        experiments_out = ctx.custom_fields["cern:experiments"] = []
        experiments = ctx.dojson_entry.get("custom_fields", {}).get(
            "cern:experiments", []
        )
        for experiment in experiments:
            if experiment.lower().strip() in ["not applicable", "xx"]:
                continue
            experiment = EXPERIMENT_ALIASES.get(experiment.lower().strip(), experiment)
            result = search_vocabulary(experiment, "experiments")
            if result and result not in experiments_out:
                experiments_out.append(result)
            elif not result:
                raise UnexpectedValue(
                    subfield="e",
                    value=experiment,
                    field="693",
                    message=f"Experiment {experiment} not found",
                    stage="vocabulary match",
                )


class DepartmentsMapper(CustomFieldMapper):
    """Sets cern:departments.

    For the first unmatched department, adds it as the administrative unit
    and as a subject fallback, and flags the record for curation (soft
    fail - handled locally, doesn't abort the record).
    """

    def apply(self, ctx):
        """Set ctx.custom_fields["cern:departments"]."""
        departments_out = ctx.custom_fields["cern:departments"] = []
        departments = ctx.dojson_entry.get("custom_fields", {}).get(
            "cern:departments", []
        )
        for department in departments:
            if "-" in department:
                dep = department.split("-")[0]
            else:
                dep = department
            result = search_vocabulary(dep, "departments")
            if result and result not in departments_out:
                departments_out.append(result)
            elif not result:
                if department.lower() == "cern?":
                    continue
                # Written into the shared source entry (not the already-built
                # metadata output) so metadata's own SubjectsMapper picks it
                # up naturally - see RecordEntry.transform(), which
                # runs custom_fields mappers before metadata mappers for
                # exactly this reason.
                ctx.dojson_entry.setdefault("subjects", []).append(
                    {"subject": department}
                )

                if ctx.custom_fields.get("cern:administrative_unit"):
                    raise UnexpectedValue(
                        subfield="5",
                        value=department,
                        field="710",
                        message=f"conflict on administrative unit "
                        f"{ctx.custom_fields['cern:administrative_unit']} VS {department}",
                        stage="vocabulary match",
                    )
                ctx.custom_fields["cern:administrative_unit"] = department
                ctx.flag_curation(
                    RecordFlaggedCuration(
                        subfield="a",
                        value=department,
                        field="department",
                        message=f"Department {department} not found. "
                        f"Added as unit and subject",
                        stage="vocabulary match",
                    )
                )
                # first unmatched department halts department processing for
                # this record (matching the original single-raise behavior);
                # other custom_fields mappers still run.
                return


class AcceleratorsMapper(CustomFieldMapper):
    """Sets cern:accelerators.

    Raises ``UnexpectedValue`` (hard failure) for an unmatched accelerator.
    """

    def apply(self, ctx):
        """Set ctx.custom_fields["cern:accelerators"]."""
        accelerators_out = ctx.custom_fields["cern:accelerators"] = []
        accelerators = ctx.dojson_entry.get("custom_fields", {}).get(
            "cern:accelerators", []
        )
        for accelerator in accelerators:
            if accelerator.lower().strip() in ["not applicable", "xx", "fermi"]:
                continue
            result = search_vocabulary(accelerator, "accelerators")
            if result and result not in accelerators_out:
                accelerators_out.append(result)
            elif not result:
                raise UnexpectedValue(
                    subfield="a",
                    value=accelerator,
                    field="accelerators",
                    message=f"Accelerator {accelerator} not found.",
                    stage="vocabulary match",
                )


class BeamsMapper(CustomFieldMapper):
    """Sets cern:beams.

    Raises ``UnexpectedValue`` (hard failure) for an unmatched beam.
    """

    def apply(self, ctx):
        """Set ctx.custom_fields["cern:beams"]."""
        beams_out = ctx.custom_fields["cern:beams"] = []
        beams = ctx.dojson_entry.get("custom_fields", {}).get("cern:beams", [])
        for beam in beams:
            if beam.lower().strip() == "not applicable":
                continue
            result = search_vocabulary(beam, "beams")
            if result and result not in beams_out:
                beams_out.append(result)
            elif not result:
                raise UnexpectedValue(
                    subfield="a",
                    value=beam,
                    field="beams",
                    message=f"Beam {beam} not found.",
                    stage="vocabulary match",
                )


class ProgrammesMapper(CustomFieldMapper):
    """Sets cern:programmes, defaulting theses without one to {"id": "None"}.

    Left unset (rather than set to None) when not applicable, so it's
    dropped from the final record the same way an absent key would be.
    """

    def apply(self, ctx):
        """Set ctx.custom_fields["cern:programmes"], or leave it unset."""
        record_json = ctx.dojson_entry
        programme = record_json.get("custom_fields", {}).get("cern:programmes")
        resource_type = record_json.get("resource_type")
        if resource_type is None:
            raise MissingRequiredField(message="resource_type", field="980")

        if programme:
            result = search_vocabulary(programme, "programmes")
            if not result:
                raise UnexpectedValue(
                    value=programme,
                    field="programme",
                    message=f"programme {programme} not found",
                    stage="vocabulary match",
                )
            ctx.custom_fields["cern:programmes"] = result
        elif resource_type == "publication-thesis":
            ctx.custom_fields["cern:programmes"] = {"id": "None"}


class JournalMapper(CustomFieldMapper):
    """Sets journal:journal.

    Flags a partial (titleless) journal field for curation (soft fail) and
    drops it, rather than aborting the record.
    """

    def apply(self, ctx):
        """Set ctx.custom_fields["journal:journal"]."""
        journal = ctx.dojson_entry.get("custom_fields", {}).get("journal:journal", {})
        if journal and not journal.get("title"):
            ctx.flag_curation(
                RecordFlaggedCuration(
                    message="found partial journal field, to be checked",
                    stage="transform",
                    field="773",
                )
            )
            journal = {}
        ctx.custom_fields["journal:journal"] = journal
