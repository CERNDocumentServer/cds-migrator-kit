# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-Migrator-Kit is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""Per-collection migration configuration.

The configuration of a collection is a ``<collection>.yaml`` file kept in git,
in ``migration_cli/collections/``. It does not depend on the environment, so
the same file is used on every environment (and by the legacy script on
``cds-migration-01``). It replaces the per-collection entries of
``streams.yaml``.

The data of a collection lives apart, under the root of the migration data of
the environment:

    <root>/                             /eos/media/cds/cds-rdm/<env>/migration
        users/                          people.csv, shared by all collections
        <collection>/                   the working directory
            dump/  files/  duplicated_pids.json
            comments/                   comments_metadata.json, users/, <recid>/

The root is, in this order: given explicitly (``--root`` or the
``CDS_MIGRATOR_KIT_MIGRATION_ROOT`` config), else the EOS path of the
environment. ``local`` has no default: the CLI gives it
``CDS_MIGRATOR_KIT_LOCAL_MIGRATION_ROOT`` (``migration_cli/rdm-data``, relative
to the root of the repository).

``migration_cli/collection_migration.py`` (legacy host) writes the data and
``migration_cli/migrating-a-collection.md`` describes the whole flow.
"""

import os
import re
import warnings

import yaml

EOS_ROOT = "/eos/media/cds/cds-rdm/{env}/migration"
DUPLICATES_FILENAME = "duplicated_pids.json"

# Excluded from every model built with `conf_query`: the records the dump never
# has (the other exclusions of the dump vary by model, they are given to it)
MODEL_EXCLUSIONS = "-980:DELETED -980:HIDDEN -980__a:DUMMY"

# names an environment may be deployed under -> EOS folder
ENV_FOLDERS = {"production": "prod"}

# flags that map 1:1 to a key of the collection entry
_RECORD_FLAGS = (
    "restricted",
    "create_inclusion_request",
    "update_new_version_publication_date",
    "access_grants_view",
    "preferred_model",
)


def env_folder(env):
    """EOS folder name of an environment."""
    return ENV_FOLDERS.get(env, env)


def default_confs_dir():
    """The ``migration_cli/collections`` folder of the checkout."""
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.normpath(
        os.path.join(here, "..", "..", "migration_cli", "collections")
    )


def repo_root():
    """Root of the repository (checkout) the package runs from."""
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.normpath(os.path.join(here, "..", ".."))


def migration_root(env, root=None):
    """Root folder of the migration data for the environment.

    A relative ``root`` is relative to the root of the repository, so it does
    not depend on the directory the command is run from.
    """
    if root:
        return os.path.normpath(os.path.join(repo_root(), root))
    if env == "local":
        raise ValueError(
            "`local` has no default root of the migration data: set "
            "CDS_MIGRATOR_KIT_MIGRATION_ROOT or CDS_MIGRATOR_KIT_LOCAL_MIGRATION_ROOT"
        )
    return EOS_ROOT.format(env=env_folder(env))


def users_dir(env, root=None):
    """Folder with the people.csv shared by all collections."""
    return os.path.join(migration_root(env, root), "users")


def conf_path(collection, confs_dir=None):
    """Path of the configuration file of the collection."""
    return os.path.join(confs_dir or default_confs_dir(), collection + ".yaml")


def read_conf(collection, confs_dir=None):
    """Read and validate the configuration of the collection."""
    path = conf_path(collection, confs_dir)
    if not os.path.isfile(path):
        raise FileNotFoundError(
            "No configuration for `{}`: {}".format(collection, path)
        )
    with open(path) as fp:
        conf = yaml.safe_load(fp) or {}
    if conf.get("name") != collection:
        raise ValueError(
            "{} is the configuration of `{}`, not of `{}`".format(
                path, conf.get("name"), collection
            )
        )
    conf.setdefault("flags", {})
    return conf


def working_dir(conf, env, root=None):
    """Working directory of the collection (``folder`` groups collections)."""
    return os.path.join(migration_root(env, root), conf.get("folder", conf["name"]))


def build_stream_config(
    collection, env, db_uri, resolve_community, root=None, confs_dir=None
):
    """Build the equivalent of a streams.yaml for one collection.

    ``resolve_community`` turns the slug of a community into its id in the
    current instance. The result has the layout the runners expect (``records``
    and ``comments`` sections).
    """
    conf = read_conf(collection, confs_dir)
    base = working_dir(conf, env, root)
    flags = conf["flags"]
    people_dir = users_dir(env, root)

    slugs = conf.get("communities")
    if not slugs:
        raise ValueError(
            "`communities` (slugs) is not defined for `{}`".format(collection)
        )

    record = {"data_dir": base}
    for flag in _RECORD_FLAGS:
        if flag in flags:
            record[flag] = flags[flag]
    # `include_plots` is the key of the collection configuration, the runner
    # (and streams.yaml) call it `plots`
    if "include_plots" in flags:
        record["plots"] = flags["include_plots"]
    record["extract"] = {"dirpath": os.path.join(base, "dump")}
    record["transform"] = {
        "files_dump_dir": os.path.join(base, "files") + "/",
        "missing_users": people_dir,
        "communities_ids": [resolve_community(slug) for slug in slugs],
    }
    if flags.get("workers"):
        record["transform"]["workers"] = flags["workers"]

    duplicates = os.path.join(base, DUPLICATES_FILENAME)
    if os.path.isfile(duplicates):
        record["load"] = {"legacy_pids_to_redirect": duplicates}

    comments = {
        "dir_path": os.path.join(base, "comments"),
        "users_dir": people_dir,
        "community_reviewers": conf.get("community_reviewers", []),
    }
    return {
        "db_uri": db_uri,
        "records": {collection: record},
        "comments": {collection: comments},
    }


# a field of a legacy search query: tag, then up to two indicators and a subfield
_QUOTED = re.compile(r'("(?:[^"\\]|\\.)*")')
_FIELD = re.compile(r"(^|[\s(+-])(\d{3}[A-Za-z0-9_]{0,3})(?=:)")
# a search keyword (`collection:`, `title:`) instead of a MARC field
_KEYWORD = re.compile(r"(?:^|[\s(+-])([A-Za-z][A-Za-z_]*):")


# the value of a field with a wildcard: `"CLIC-Note*"` or `CLIC-Note*`
_WILDCARD = re.compile(r':("[^"]*\*[^"]*"|[^\s()"/]*\*[^\s()"/]*)')


def _wildcard_to_regex(match):
    value = match.group(1).strip('"')
    pattern = ".*".join(re.escape(piece) for piece in value.split("*"))
    # the legacy search matches the whole value, `*` stands for anything
    if not value.startswith("*"):
        pattern = "^" + pattern
    if not value.endswith("*"):
        pattern += "$"
    # (?i): the legacy search ignores the case (CLICDP-CONF-2013-006 is found by
    # "CLICdp-Conf-*"), the matcher does not
    return ":/(?i)" + pattern + "/"


def _model_field(match):
    prefix, field = match.group(1), match.group(2)
    if len(field) == 3:  # `088`: any indicators in the search, blank in a model
        field += "__"
    elif len(field) == 4:  # `690C`
        field += "_"
    elif len(field) == 6:  # `980__a`: a model separates the subfield with a dot
        field = field[:5] + "." + field[5]
    return prefix + field


def legacy_to_model_query(query):
    """Translate a legacy search query to the notation of a model ``__query__``.

    The legacy search reads ``980__a:X`` and ``088:X``, the matcher of the models
    only understands ``980__.a:X`` and ``088__:X`` (and silently matches nothing
    otherwise):

    * a field of 6 characters (tag, indicators, subfield) gets a dot before the
      subfield: ``980__a`` -> ``980__.a``
    * a field without (all) the indicators is padded with blank ones:
      ``088`` -> ``088__``, ``690C`` -> ``690C_``. The search matches any
      indicators there, the model only the blank ones.

    * a value with a wildcard (``"CLIC-Note*"``, ``*ARCHIVE*``) becomes a regular
      expression (``/(?i)^CLIC.Note/``, ``/(?i).*ARCHIVE.*/``, ignoring the case like the
      search), as the matcher does not know the wildcard

    The other values, quoted strings, operators and parentheses are left as they are, and
    a query already in the notation of the models is not changed. The matcher is
    case sensitive, the legacy search is not, and it does not know the search
    keywords (``collection:"X"``): a warning is given for them, as they never
    match.
    """
    # the matcher reads a value literally (`"X*"` matches only `X*`, `*X` is an
    # invalid pattern), so a wildcard is turned into a regular expression
    query = _WILDCARD.sub(_wildcard_to_regex, query)
    parts = _QUOTED.split(query)
    for part in parts[::2]:
        for keyword in _KEYWORD.findall(part):
            warnings.warn(
                "`{}:` in `{}` is a search keyword, not a MARC field: a model "
                "never matches it".format(keyword, query)
            )
    # the odd parts are the quoted strings
    return "".join(
        part if index % 2 else _FIELD.sub(_model_field, part)
        for index, part in enumerate(parts)
    )


def conf_query(collection, exclusions="", confs_dir=None):
    """The ``__query__`` of a model, built from the queries of the collection conf.

    The queries of the conf (``queries``, each with its ``q``) are translated
    with ``legacy_to_model_query`` and joined with OR. The deleted, hidden and
    dummy records (``MODEL_EXCLUSIONS``) are always excluded. The other exclusions
    vary by model: they are given in ``exclusions``, in either notation (e.g.
    ``"-980__c:MIGRATED -690C_:SCICOM"``, translated the same way).

    A model is imported when the application starts, so the folder of the confs
    is not read from the application config: it is the environment variable
    ``INVENIO_CDS_MIGRATOR_KIT_COLLECTIONS_DIR`` (set in the Dockerfile), else
    the ``migration_cli/collections`` folder of the checkout.
    """
    confs_dir = (
        confs_dir
        or os.environ.get("INVENIO_CDS_MIGRATOR_KIT_COLLECTIONS_DIR")
        or default_confs_dir()
    )
    queries = []
    for item in read_conf(collection, confs_dir).get("queries") or []:
        text = item.get("q") if isinstance(item, dict) else item
        if not text:
            raise ValueError(
                "A query of `{}` has no text (`q`): {!r}".format(collection, item)
            )
        queries.append("({})".format(legacy_to_model_query(text.strip())))
    if not queries:
        raise ValueError(
            "`queries` is not set for `{}` in {}".format(collection, confs_dir)
        )
    exclusions = " ".join((MODEL_EXCLUSIONS, exclusions.strip())).strip()
    return "({}) {}".format(" OR ".join(queries), legacy_to_model_query(exclusions))
