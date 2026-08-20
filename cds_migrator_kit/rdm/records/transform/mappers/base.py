# -*- coding: utf-8 -*-
#
# Copyright (C) 2022-2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Base classes shared by all CDS-RDM record field mappers."""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class RecordTransformContext:
    """Shared, mutable context passed to field mappers building one record.

    ``metadata`` and ``custom_fields`` are the in-progress output dicts:
    mappers write their own value into them (metadata mappers return a
    value that the caller assigns; custom_fields list-accumulator mappers
    mutate ``custom_fields`` directly) and later mappers in the same phase
    may read earlier ones (e.g. the title mapper reads the already-resolved
    ``metadata["resource_type"]``).
    """

    json_entry: dict
    entry: dict
    migration_logger: object = None
    affiliations_mapping: object = None
    access_grants_view: object = None
    json_output: dict = None
    metadata: dict = field(default_factory=dict)
    custom_fields: dict = field(default_factory=dict)

    def flag_curation(self, exc):
        """Log a caught ``RecordFlaggedCuration`` for curation follow-up."""
        self.migration_logger.add_information(
            self.json_entry["recid"],
            {"message": exc.message, "value": exc.value},
        )


class FieldMapper(ABC):
    """Derives a single output value, identified by ``id``, from the entry.

    Used both for ``metadata.<id>`` fields and for other top-level
    ``record_json_output`` fields (e.g. ``access_grants``) - the caller
    decides where the returned value is assigned.
    """

    id: str

    @abstractmethod
    def map_value(self, ctx: RecordTransformContext):
        """Return the value for this field, or a falsy value to omit it."""
        raise NotImplementedError


class PassthroughMapper(FieldMapper):
    """Copies a key from the source entry through unchanged."""

    def __init__(self, id):
        """Constructor."""
        self.id = id

    def map_value(self, ctx):
        """Return the raw value of ``self.id`` from the source entry."""
        return ctx.json_entry.get(self.id)


class CustomFieldMapper(ABC):
    """Writes one or more ``custom_fields`` keys into ``ctx.custom_fields``.

    Every mapper owns its own soft-fail (curation) handling internally, via
    ``ctx.flag_curation()`` - mirroring how affiliation matching handles its
    own curation flags in ``mappers/contributors.py``. Only a genuinely hard
    failure (e.g. ``UnexpectedValue``) is left to propagate and abort the
    whole record. This lets the caller run every mapper the same way, in a
    single uninterrupted loop, with no branching per mapper.
    """

    @abstractmethod
    def apply(self, ctx: RecordTransformContext):
        """Apply the mapper, writing into ``ctx.custom_fields``."""
        raise NotImplementedError


class PassthroughCustomFieldMapper(CustomFieldMapper):
    """Copies a custom_fields key from the source entry through unchanged."""

    def __init__(self, id, default=None):
        """Constructor."""
        self.id = id
        self.default = default

    def apply(self, ctx):
        """Copy ``self.id`` from the source custom_fields, or use the default."""
        source = ctx.json_entry.get("custom_fields", {})
        ctx.custom_fields[self.id] = source.get(self.id, self.default)
