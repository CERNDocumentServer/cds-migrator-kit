# Migration manual

## Dump a subset of records on legacy

```bash

inveniomigrator dump records -q '980:INTNOTECMSPUBL 980:NOTE -980:DELETED' --file-prefix cms-notes --latest-only --chunk-size=1000

```

## Define transforming models and rules resolution

You can adapt XML processing to different subsets of records by implementing different data models for each subset (f.e. collection).
Let's take CMS notes as an example:

```python

class CMSNote(CdsOverdo):
    """Translation Index for CDS Books."""

    __query__ = (
        '980__:INTNOTECMSPUBL 980__:NOTE'
    )

    __model_ignore_keys__ = {}

    _default_fields = {}



model = CMSNote(bases=(),
                entry_point_group="cds_rdm.migrator.rules"
                )

```

**query** - defines the MARC fields to which specific record should match. Attention: It does not recognise regexes that are used to specify the collection query in the admin interface of legacy CDS.

\***\*model_ignore_keys\*\*** - set of keys to be ignored for this data model - fields will not be migrated

**bases** - by defining bases of models you can specify a parent model which fits all the subsets of records (f.e. 245 - title field MARC to JSON translation could be the same for all the models)

**entry_point_group** - reference to where the model should lookup for the set of the MARC translation rules, see the entrypoints below.

After defining your model and set of rules,you have to register them in the entrypoints of your application, in setup.cfg:

```editorconfig

[options.entry_points]
cds_migrator_kit.migrator.models =
    cms_note = cds_rdm.migration.transform.models.note:model
cds_migrator_kit.migrator.rules =
    base = cds_rdm.migration.transform.xml_processing.rules

```

## Run migration

All the commands should be run from cds-rdm project root, inside cds-rdm virtualenv.

Initialise an empty DB:

```
invenio-cli services setup --force --no-demo-data
```

Wait until all the fixtures are propagated and indexed.
Dump communities ids by running this script in `invenio shell`:

```python
import yaml
from pathlib import Path
from invenio_communities.communities.records.models import CommunityMetadata

community_map = {comm.slug: str(comm.id) for comm in CommunityMetadata.query.all()}
streams_path = str(Path('site/cds_rdm/migration/streams.yaml').absolute())
streams = {}

with open(streams_path, 'r') as fp:
    streams = yaml.safe_load(fp)

streams["records"]["load"]["cache"]["communities"] = community_map

with open(streams_path, 'w') as fp:
    yaml.safe_dump(streams, fp, default_flow_style=False)


```

Load the previously dumped legacy records. The configuration is already defined in streams.yaml - check the documentation of invenio-rdm-migrator for more details

```
invenio migration run
```

Once it has finished, run the re-indexing:

```
invenio rdm-records rebuild-index
```

Alternatively, you can index each resource separately:

Note that this step will strain your CPU rendering your laptop almost useless. In a `invenio-cli pyshell` run:

```python
# You might want to first run users, then rebuild index.
# Then run records and rebuild its index.
from invenio_access.permissions import system_identity
from invenio_rdm_records.proxies import current_rdm_records_service
from invenio_users_resources.proxies import current_users_service
current_users_service.rebuild_index(identity=system_identity)
current_rdm_records_service.rebuild_index(identity=system_identity)
```

- When the workers have no longer any tasks to run, in the _pyshell_ run:

```python
current_users_service.indexer.refresh()
current_rdm_records_service.indexer.refresh()
```

or if memory is an issue then you can generate the index batches with the code below

```
from invenio_rdm_records.proxies import current_rdm_records_service
from invenio_db import db

model_cls = current_rdm_records_service.record_cls.model_cls
records = db.session.query(model_cls.id).filter(
    model_cls.is_deleted == False,
).yield_per(1000)

current_rdm_records_service.indexer.bulk_index((rec.id for rec in records))
```

### To visualise the errors (locally):

```shell
gunicorn -b :8080 --timeout 120 --graceful-timeout 60 cds_migrator_kit.app:app --reload --log-level debug --capture-output --access-logfile '-' --error-logfile '-'
```

### Full migration workflow of one collection

#### Legacy

This is the recipe on how to dump the metadata and files. For local tests it is not necessary to do it often, especially for files which are static.
It makes sense to dump metadata if any changes are applied on legacy

```shell
ssh cds-wn-31 # inveniomigrator tool installed here
kinit cdsrdmeosdev
cd /eos/media/cds/cds-rdm/dev/migration/summer-student-notes/dump
inveniomigrator dump records -q '980__:NOTE 037__:CERN-STUDENTS-Note-* -980:DELETED' --file-prefix summer-studends-notes --latest-only --chunk-size=1000
python copy_collection_files.py --dump-folder /eos/media/cds/cds-rdm/dev/migration/summer-student-notes/dump --files-destination /eos/media/cds/cds-rdm/dev/migration/summer-student-notes/files
```

## How to mount eos locally on MAC (to copy over the dumps and files)

1. Go to Finder icon on your dock
2. right click to get the contextual menu
3. choose connect to server
4. type `https://cernbox.cern.ch/cernbox/webdav/eos/media/cds/cds-rdm/dev/migration`
5. click connect
6. use eos account dev credentials

#### Openshift migration pod

```shell
invenio migration run
```

Once it has finished, run the re-indexing:

```
invenio rdm rebuild-all-indices
```

### Migrate the statistics for the successfully migrated records

When the `invenio migration run` command ends it will produce a `rdm_records_state.json` file which has linked information about the migrated records and the old system. The format will be similar to below:

```json
{
  "legacy_recid": "2884810",
  "parent_recid": "zts3q-6ef46",
  "latest_version": "1mae4-skq89",
  "versions": [
    {
      "new_recid": "1mae4-skq89",
      "version": 2,
      "files": [
        {
          "legacy_file_id": 1568736,
          "bucket_id": "155be22f-3038-49e0-9f17-9518eaac783a",
          "file_key": "Summer student program report.pdf",
          "file_id": "06cdb9d2-635f-4dbe-89fe-4b27afddeaa2",
          "size": "1690854"
        }
      ]
    }
  ]
}
```

- Open the `cds_migrator_kit/rdm/migration/stats/config.py` and

  - export the below 2 environmental variables
    - `CDS_MIGRATOR_KIT_SRC_SEARCH_HOSTS`: e.g `export CDS_MIGRATOR_KIT_SRC_SEARCH_HOSTS='[{"host": "os-cds-legacy.cern.ch", "url_prefix": "/os", "timeout": 30, "port": 443, "use_ssl": true, "verify_certs": false, "http_auth": ["LEGACY_PRODUCTION_OPENSEARCH_USERNAME", "<LEGACY_PRODUCTION_OPENSEARCH_PASSWORD>"]}]'`
      - you find the credentials for `LEGACY_PRODUCTION_OPENSEARCH_PASSWORD` by `tbag show LEGACY_PRODUCTION_OPENSEARCH_PASSWORD --hg cds`
      - you find the credentials for `LEGACY_PRODUCTION_OPENSEARCH_USERNAME` by `tbag show LEGACY_PRODUCTION_OPENSEARCH_USERNAME --hg cds`

- Open a shell and run the following commands

```bash
$ invenio migration stats run --filepath "path/to/file/of/rdm_records_state.json"
```

This will migrate only the raw statistic events. When all events are ingested to the new cluster then we will need to aggregate them.

To do so, you need to run after you have set the correct bookmark for each event:

on opensearch

```shell
DELETE /cds-rdm-stats-bookmarks

POST /cds-rdm-stats-bookmarks/_doc
{
  "date": "2000-06-26T15:56:05.755394",
  "aggregation_type": "file-download-agg"
}
POST /cds-rdm-stats-bookmarks/_doc
{
  "date": "2000-06-26T15:56:05.755394",
  "aggregation_type": "record-view-agg"
}
POST /cds-rdm-stats-bookmarks/_doc
{
  "date": "2000-06-26T15:56:05.755394",
  "aggregation_type": "stats_reindex"
}
```

```
from invenio_stats.tasks import aggregate_events

start_date = '2000-01-01'
end_date = '2024-12-01'

aggregations = ["record-view-agg", "file-download-agg"]
aggregate_events(aggregations)

from invenio_rdm_records.services.tasks import reindex_stats
stats_indices = [ "stats-record-view", "stats-file-download",]

reindex_stats(stats_indices)
```

visit https://migration-cds-rdm-dev.app.cern.ch for report
