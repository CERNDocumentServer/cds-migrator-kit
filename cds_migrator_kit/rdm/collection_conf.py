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

import yaml

EOS_ROOT = "/eos/media/cds/cds-rdm/{env}/migration"
DUPLICATES_FILENAME = "duplicated_pids.json"

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
    if "access_collections" in flags:
        record["transform"]["access_collections"] = flags["access_collections"]

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
