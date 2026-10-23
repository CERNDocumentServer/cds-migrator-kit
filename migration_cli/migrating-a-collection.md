# Migrating a collection

Replace every `<TODO collection name>` with your collection name, and every `<TODO env>` with your environment (`dev`, `sandbox`, `prod`).

Everything is driven by one file per collection, `migration_cli/collections/<TODO collection name>.yaml`, kept in git. It is the same on every environment: both the legacy script (`cds-migration-01`) and the migrator pod read it, so there is **no `streams.yaml` entry and no config map change** for a collection. A change of the file is a normal merge request and a deployment of the image.

The data of the collection is kept apart, under the root of the migration data of the environment:

```
/eos/media/cds/cds-rdm/<env>/migration/<collection>/
    dump/                      records dump                      (dump records)
    comments/                  comments, commenters, attachments (dump comments / files)
    files/                     record files                      (dump files)
    duplicated_pids.json       duplicates to redirect            (prepare)
    ep_report_number_dups.json EP report numbers to review       (prepare)
/eos/media/cds/cds-rdm/<env>/migration/users/    people.csv, shared by all collections
```

## 1. Data analysis (before moving to environments)

Prepare the data locally first, see https://digital-repositories.web.cern.ch/cds/cds.cern.ch/migration/ and note the results in the issue:

- [ ] check the permissions references and submissions (Submissions tab of the reference spreadsheet)
- [ ] check whether the e-group with the SBI right is still used; if yes, contact them
- [ ] check the fields of the submission form for unknown fields
- [ ] check the submission elements for hidden restrictions (specific emails and such)
- [ ] check the already submitted records and which collections they belong to, and which migration rules are missing

Community details (fill in with the community):

| Role    | e-group/person |
|---------|----------------|
| Reader  | TODO           |
| Curator | TODO           |
| Manager | TODO           |
| Owner   | TODO           |

## 2. Write the collection configuration

Copy `migration_cli/collections/_example.yaml` to `migration_cli/collections/<TODO collection name>.yaml` and fill it in:

```yaml
name: <TODO collection name>
filename_prefix: <TODO collection name>
queries:
  - '980__a:PRIVANTARES'
flags:
  restricted: true
  plots: true
communities:
  - cern-research
  - <TODO slug of the collection community>
```

- Write the queries **without** the standard exclusions: the script appends `-690C_:SCICOM -595:*ARCHIVE* -980:ANNOUNCEMENT -980:DELETED -980:HIDDEN -980__a:DUMMY -690C_:movingimages -970__b:MERGE-* -980:GREYBOOK`. `-980__c:MIGRATED` is not excluded on purpose, some records belong to several communities.
- With several queries, also set `analyze_query`: one query, without `-980:DELETED -980:HIDDEN -980__c:MIGRATED -980__a:DUMMY` (duplicates are searched among them to be redirected to).
- `communities` are slugs, the same in every environment. The community must already exist in the environment where you migrate: the pod resolves the slugs to ids and stops if one does not exist.
- Optional: `folder` (working directory under the data root, default the name), `comments_queries` (default `queries`) and `reviewers` for the comments, and the `flags` (`update_new_version_publication_date: true` only for administrative documents, e.g. HR; `skip_plot_files`; `workers`; ...).

Merge it and update the checkout on `cds-migration-01` (`/opt/cds-migrator-kit`) and the image of the migration pod before going further.

## 3. Block the submissions

- [ ] merge and deploy the PR that blocks the submissions of the collection

## 4. Export from the legacy system (on `cds-migration-01`)

The scripts are read from the cds-migrator-kit checkout on the machine, `/opt/cds-migrator-kit/migration_cli` (make sure it is up to date). Use an identity that can write to the EOS migration folder, and run from the migration folder of the environment (the environment is read from the current directory), or pass `--env <TODO env>`.

```shell
kinit cdsrdmeos<TODO env>
cd /eos/media/cds/cds-rdm/<TODO env>/migration
S=/opt/cds-migrator-kit/migration_cli

python $S/collection_migration.py dump records  --collection <TODO collection name>
python $S/collection_migration.py dump comments --collection <TODO collection name>
python $S/collection_migration.py dump files    --collection <TODO collection name>
python $S/collection_migration.py prepare       --collection <TODO collection name>
```

| Command | Result |
|---|---|
| `dump records` | `inveniomigrator dump records` for each query, the dump files in `dump/` |
| `dump comments` | `comments/comments_metadata.json` and `comments/users/{active,missing}_commentors*.json`. Needs the legacy Invenio: `ipython $S/collection_migration.py -- dump comments --collection <TODO collection name>` |
| `dump files` | copies the files of the dumped records to `files/` (checksums verified, `files.log` written) and the files attached to comments to `comments/<recid>/` |
| `prepare` | runs `analyze_collection_migration.py` from `subscripts/`. Duplicates (same title, description and file checksums, `981__a` merges, ignoring `CER` values) in `duplicated_pids.json`; EP report numbers shared with other records in `ep_report_number_dups.json` (review them) |


If `people.csv` needs to be recreated (once per environment, no `--collection` needed):

```shell
cd /eos/media/cds/cds-rdm/<TODO env>/migration
python /opt/cds-migrator-kit/migration_cli/collection_migration.py dump users      # dumps the authority records to users/
# on the migration pod
invenio migration users people-run --filepath /eos/media/cds/cds-rdm/<TODO env>/migration/users/people.csv \
    --dirpath /eos/media/cds/cds-rdm/<TODO env>/migration/users
```

All the commands are safe to re-run.

## 5. Before the migration

- [ ] make sure the pod runs the latest cds-migrator-kit (the collection configuration is part of the image)
- [ ] **TAKE THE DB BACKUP**
- [ ] (optional) add the community to `INVENIO_CDS_COMMITTEE_APPROVAL_COMMUNITIES` (invenio config map) if needed
- [ ] the pod needs `INVENIO_CDS_MIGRATOR_KIT_ENV` set to the environment (`dev`, `sandbox`, `prod`; `production` is accepted for `prod`)

## 6. Migrate (on the migration pod)

```shell
oc project cds-rdm-sandbox
oc rsh <migration-pod>

# accounts of the record submitters, then of the commenters
invenio migration users submitters run --collection <TODO collection name>
invenio migration users commenters run --collection <TODO collection name>   # make sure they are indexed, not only in the DB

# records, then comments
invenio migration run --collection <TODO collection name>
invenio migration comments run --collection <TODO collection name>
```

Every command accepts `--dry-run` (try it first). The comments runner can be re-run after errors: it skips the comments already created.

## 7. Statistics

Raw events of the migrated collection (reads the records state file written by `migration run`, no path to give; `--filepath` overrides it):

```shell
invenio migration stats events run --collection <TODO collection name>
```

When the events of **all** the collections are ingested, aggregate them once:

```shell
invenio migration stats aggregate
```

It resets the `stats-bookmarks` index (with the index prefix of the environment), aggregates `record-view-agg` and `file-download-agg` up to today (`--end-date` to change) and reindexes `stats-record-view` and `stats-file-download`.

## 8. After the migration

- [ ] reindex all users `invenio rdm rebuild-all-indices -o users` to add missing migrated accounts
- [ ] set the community slug and deploy the redirections: [TODO PR with redirection]
- [ ] merge the Puppet `qa` branch to `master` to activate the submissions/collections/search redirections
- [ ] run the MIGRATED tagging on old CDS: https://gitlab.cern.ch/cds-team/production_scripts/-/blob/master/cds/set_records_migrated_to_rdm.py

## Notes

- **Data root**: the data goes to `/eos/media/cds/cds-rdm/<env>/migration` for `dev`, `sandbox` and `prod` (no setting needed). Locally (`INVENIO_CDS_MIGRATOR_KIT_ENV=local`, the default) the pod side uses `migration_cli/rdm-data` of the repository, or `INVENIO_CDS_MIGRATOR_KIT_MIGRATION_ROOT` (a relative path is relative to the repository root); the legacy script takes `--root` (or `CDS_MIGRATION_ROOT`) together with `--env local`. `dump files` takes `--legacy-files-root` for the source of the legacy files. The dump steps still need the legacy tools (`inveniomigrator`, legacy Invenio). See `cds_migrator_kit/rdm/README.md` for the local flow.
- **Configuration folder**: `migration_cli/collections` of the checkout. On the pod it is set by `INVENIO_CDS_MIGRATOR_KIT_COLLECTIONS_DIR` (done in the `Dockerfile`), on the legacy script by `--confs-dir`.
- Collections that still have an entry in `cds_migrator_kit/rdm/streams.yaml` and no configuration file in `migration_cli/collections/` keep working as before: the CLI falls back to `streams.yaml` if there is no `migration_cli/collections/<collection>.yaml`.
- `people.csv` is read from `/eos/media/cds/cds-rdm/<env>/migration/users/`; it is not copied into the collection.
