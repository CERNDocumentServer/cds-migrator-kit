# -*- coding: utf-8 -*-
#
# Copyright (C) 2022-2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""YAML-backed vocabulary lookup used by custom_fields mappers."""
from pathlib import Path

import yaml
from flask import current_app

_VOCAB_FILENAMES = {
    "experiments": "experiments.yaml",
    "departments": "departments.yaml",
    "programmes": "programmes.yaml",
    "accelerators": "accelerators.yaml",
    "beams": "beams.yaml",
}


class VocabularyCache:
    """Vocabulary lookup cache loaded once from YAML files at startup."""

    def __init__(self, default_dir, override_dir=None):
        """Load all vocabularies into memory.

        For each vocabulary file, ``override_dir`` (e.g. a test-local
        directory overriding just a subset of files) is preferred when it
        contains that file, falling back to ``default_dir`` otherwise.
        """
        self._cache = {}
        default_dir = Path(default_dir)
        override_dir = Path(override_dir) if override_dir else None
        for vocab_type, filename in _VOCAB_FILENAMES.items():
            filepath = default_dir / filename
            if override_dir and (override_dir / filename).exists():
                filepath = override_dir / filename
            self._cache[vocab_type] = self._load(filepath)

    @staticmethod
    def _load(filepath):
        """Build a case-insensitive term→id lookup from a vocabulary YAML."""
        with open(filepath) as f:
            entries = yaml.safe_load(f)
        lookup = {}
        for entry in entries:
            entry_id = entry["id"]
            lookup[entry_id.lower()] = entry_id
            title = entry.get("title", {}).get("en", "")
            if title and title.lower() != entry_id.lower():
                lookup[title.lower()] = entry_id
        return lookup

    def get(self, term, vocab_type):
        """Return {"id": vocab_id} if term matches, else None."""
        entry_id = self._cache[vocab_type].get(term.strip().lower())
        return {"id": entry_id} if entry_id else None


_vocabulary_cache = None


def _get_vocabulary_cache():
    global _vocabulary_cache
    if _vocabulary_cache is None:
        import cds_rdm

        default_dir = Path(cds_rdm.__file__).parent / "app_data" / "vocabularies"
        override_dir = current_app.config.get("CDS_MIGRATOR_KIT_VOCABULARIES_DIR")
        _vocabulary_cache = VocabularyCache(default_dir, override_dir)
    return _vocabulary_cache


def search_vocabulary(term, vocab_type):
    """Look up a vocabulary term using the pre-loaded YAML cache.

    Returns {"id": vocab_id} if found, else None.
    """
    return _get_vocabulary_cache().get(term, vocab_type)
