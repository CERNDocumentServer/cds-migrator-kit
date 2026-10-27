# Migrating a collection locally (example: `antares`)

How to run a collection migration on your own machine, isolated from EOS and OpenShift: the migration data lives in a local folder, and the legacy system is only used to produce the dumps.

Other documents:

- migration on the test environments: `migration_cli/migrating-a-collection.md`
- each stage in detail: `migration_cli/step_by_step_documentation.md`
- writing the transformation models: `cds_migrator_kit/rdm/readme_models.md`

## What is different locally

With `CDS_MIGRATOR_KIT_ENV` set to `local` the loader does not read the legacy files: **every file is replaced by a dummy PDF**, and the checksum and access-grant verifications are skipped. So you only need the records dump (and the comments metadata); there is no need to copy the legacy files.

## 0. Prerequisites

- the services running (PostgreSQL, OpenSearch, RabbitMQ, Redis) and a Celery worker
- the package installed: `pip install -e ".[rdm]"`
- `people.csv` of the environment, see step 2

### Initialise the instance (first time only)

Run in the cds-rdm environment, on empty database and indices (the commands are those of `reinit-instance.sh`):

```shell
invenio db init
invenio index init
invenio rdm-records custom-fields init
invenio communities custom-fields init
invenio rdm-records fixtures      # vocabularies, the CERN Research community (the common community id used in the confs), ...
invenio rdm fixtures              # static pages
invenio rdm rebuild-all-indices
```

`invenio rdm-records fixtures` loads everything declared in cds-rdm's `app_data/` (`vocabularies.yaml`: subjects, affiliations, funders, awards, experiments, departments, committees, accelerators, beams, programmes, resource types, community types, ...; and `communities.yaml`). When the files in `app_data/` change later, update the existing entries with `invenio rdm-records add-to-fixture <name>` (`programmes`, `subjects`, `accelerators`, `awards`, `experiments`, `resourcetypes`, `communitytypes`).

Then populate the vocabularies that are not shipped as fixtures: the ROR affiliations and the ORCID names. They are the Celery task `process_datastream` of invenio-vocabularies, which you can call from `invenio shell`, using the configuration of the corresponding jobs. The Celery worker must be running: the tasks only read the source and hand the entries over to the worker.

```python
# invenio shell
from invenio_vocabularies.jobs import ImportORCIDJob, ProcessRORAffiliationsJob
from invenio_vocabularies.services.tasks import process_datastream

# ROR affiliations
process_datastream.delay(**ProcessRORAffiliationsJob.build_task_arguments(None))

# ORCID names
process_datastream.delay(**ImportORCIDJob._build_task_arguments(None))
```

- `ProcessRORAffiliationsJob` downloads the ROR dump into the affiliations vocabulary (the fixtures only load the subset in `app_data/vocabularies/affiliations_ror.yaml`).
- `ImportORCIDJob` reads the ORCID summaries from the ORCID S3 bucket, and needs `VOCABULARIES_ORCID_ACCESS_KEY` and `VOCABULARIES_ORCID_SECRET_KEY` in your configuration. `VOCABULARIES_ORCID_SYNC_SINCE` limits how far back it goes.
- `.delay(...)` queues the task and returns at once, follow it in the Celery worker log. To run the reading in the shell itself, call `process_datastream(**...)` without `.delay`.

### Save a "clean slate" snapshot

The instance takes long to initialise (vocabularies, ROR, ORCID), and every migration run leaves records behind. Right after the initialisation, and before any migration, dump the clean state with `scripts/snapshot.sh` (PostgreSQL and OpenSearch together, in `snapshots/<name>/`):

```shell
scripts/snapshot.sh dump clean-slate
```

and bring the instance back to it in a few minutes whenever you want to rerun a migration from scratch:

```shell
scripts/snapshot.sh restore clean-slate        # asks before destroying the current data, FORCE=1 skips it
```

The script expects a running PostgreSQL container and the cds-rdm virtualenv in `../cds-rdm/.venv` (`CDS_RDM_VENV_ACTIVATE` to change it), see its header. `scripts/postgres-init.sh` and `scripts/opensearch-init.sh` do each half on its own.

**Keep the snapshot up to date.** It stores the data model as it was when it was taken, so refresh it after upgrading cds-rdm or invenio:

1. restore the snapshot, then upgrade the database schema: `invenio alembic upgrade`
2. update the OpenSearch indices to the new mappings: the restore recreates the indices from the code with `invenio index init`, but an index that already exists is changed with `invenio index update <index>`; then reindex with `invenio rdm rebuild-all-indices`
3. run `invenio rdm-records custom-fields init` and `invenio communities custom-fields init` if custom fields were added, and `invenio rdm-records add-to-fixture ...` for changed fixtures
4. dump it again: `scripts/snapshot.sh dump clean-slate` (same name overwrites it, or use a dated name)

## 1. Local data folder

The local "root" of the migration data is the folder `migration_cli/rdm-data/` of this repository (the default of the `local` environment; relative paths are relative to the repository root, whatever the directory you run `invenio` from). The commands below are run from the repository root. The working directory of a collection is `<root>/<collection>/`:

```
<repo>/migration_cli/rdm-data/        <- the root (default of the local environment)
    users/people.csv                   shared by all collections
    antares/                           <- the working directory of the collection
        dump/                          records dump
        comments/                      optional, comments of the collection
```

```shell
mkdir -p migration_cli/rdm-data/users migration_cli/rdm-data/antares
```

`people.csv` is the people authority export of the environment. Copy it to `migration_cli/rdm-data/users/people.csv` from the EOS `users` folder, or recreate it as described in `migration_cli/migrating-a-collection.md`.

## 2. Configuration

The configuration of a collection is the file `migration_cli/collections/antares.yaml` of the repository, the same for every environment (all the options in `migration_cli/collections/_example.yaml`):

```yaml
name: antares
queries:
  - q: '<TODO legacy query of the collection>'
    dump_file_prefix: antares
flags:
  include_plots: true
communities:
  - cern-research
  - antares
```

The communities are given by slug and are resolved to ids by the migration, so they must exist in your local instance. `cern-research` is created by the fixtures (step 0). Create the collection community, in `invenio shell`:

```python
from invenio_access.permissions import system_identity
from invenio_communities.proxies import current_communities

service = current_communities.service
try:
    service.read(system_identity, "antares")
except Exception:  # not created yet
    service.create(
        system_identity,
        {"slug": "antares", "metadata": {"title": "Antares"}, "access": {"visibility": "public"}},
    )
```

## 3. Get the dumps from the legacy system

The legacy tools exist only on `cds-migration-01`. Run the script there with a scratch folder as root (not on EOS), then download the result:

```shell
# on cds-migration-01
S=/opt/cds-migrator-kit/migration_cli      # a checkout that has the configuration of the collection
python $S/collection_migration.py dump records --collection antares --env local --root /tmp/antares-root
ipython $S/collection_migration.py -- dump comments --collection antares --env local --root /tmp/antares-root   # optional
python $S/collection_migration.py prepare --collection antares --env local --root /tmp/antares-root             # optional

# on your machine
rsync -av cds-migration-01:/tmp/antares-root/antares/dump/ migration_cli/rdm-data/antares/dump/
rsync -av cds-migration-01:/tmp/antares-root/antares/comments/ migration_cli/rdm-data/antares/comments/   # optional
rsync -av cds-migration-01:/tmp/antares-root/antares/{duplicated_pids,ep_report_number_dups}.json migration_cli/rdm-data/antares/   # optional, after prepare
```

`prepare` is optional: it searches the collection for duplicated records (same title, description and file checksums, and `981__a` merges) and writes the legacy recids to redirect in `duplicated_pids.json` (and the EP report numbers shared with other records in `ep_report_number_dups.json`) in the working directory of the collection. Run it if you want to test the redirections: when `duplicated_pids.json` is there, the migration reads it, otherwise nothing is redirected. It needs `analyze_collection_migration.py` and the legacy Invenio, like the dumps. It searches the records of all the `queries` of the collection configuration together.

The dumps change only when the legacy records change (f.e. when curators intervene) , so they do not need to be refreshed on every run. `dump files` is not needed locally.

## 4. Environment

Nothing to set: `CDS_MIGRATOR_KIT_ENV` is `local` and the data root is `CDS_MIGRATOR_KIT_LOCAL_MIGRATION_ROOT` (`migration_cli/rdm-data` of the repository) by default in `migration_config.py`. To use another folder, export `INVENIO_CDS_MIGRATOR_KIT_MIGRATION_ROOT` with an absolute path, or a path relative to the repository root.

The root holds `users/` and the collection folders. The configuration is read from `migration_cli/collections/` of the checkout (`INVENIO_CDS_MIGRATOR_KIT_COLLECTIONS_DIR` to change it). Logs and the records state file go to `INVENIO_CDS_MIGRATOR_KIT_LOGS_PATH` (default `cds_migrator_kit/tmp/logs/`).

## 5. Migrate

Try each command with `--dry-run` first.

```shell
invenio migration users submitters run --collection antares
invenio migration run --collection antares

# only if you dumped the comments
invenio migration users commenters run --collection antares
invenio migration comments run --collection antares
```

Then reindex, to see the missing user accounts in the UI:

```shell
invenio rdm rebuild-all-indices -o users
```

The migration report can be browsed with:

```shell
gunicorn -b :8080 --timeout 120 --graceful-timeout 60 cds_migrator_kit.app:app --reload --log-level debug --capture-output --access-logfile '-' --error-logfile '-'
```

## 6. Statistics (optional in local)

The legacy events are read from the legacy OpenSearch, so this needs access to it: export `CDS_MIGRATOR_KIT_SRC_SEARCH_HOSTS` as described in `migration_cli/step_by_step_documentation.md`, then:

```shell
invenio migration stats events run --collection antares
invenio migration stats aggregate
```

## 7. Rerun from a clean state

Restore the clean-slate snapshot taken in step 0, then run step 5 again:

```shell
scripts/snapshot.sh restore clean-slate
```

It restores PostgreSQL and OpenSearch to the state before the migration, and resets the file location of the instance. This replaces the manual cleanup described in `migration_cli/step_by_step_documentation.md` ("Local rerun migration from clean state"), which is only needed when there is no snapshot. Only do it on a local instance. If the snapshot is old, update it first as described in step 0.
