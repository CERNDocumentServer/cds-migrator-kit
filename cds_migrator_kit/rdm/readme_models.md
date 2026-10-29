# Migration model manual

## Dump users

! Attention If you need to dump the users from legacy DB or you need to process the people collection

https://gitlab.cern.ch/cds-team/production_scripts/-/blob/master/cds-rdm/migration/dump_users.py?ref_type=heads


## Dump a subset of records on legacy

on webnode: `cds-migration-01`

```bash
inveniomigrator dump records -q '980:INTNOTECMSPUBL 980:NOTE -980:DELETED' --file-prefix cms-notes --chunk-size=1000

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

The query of a model can be built from the `queries` of the collection configuration (`migration_cli/collections/<collection>.yaml`), so the records that are dumped and the records the model matches come from the same definition:

```python
from cds_migrator_kit.rdm.collection_conf import conf_query

class CLICModel(CdsOverdo):
    __query__ = conf_query(
        "clic",
        "-690C_:SCICOM -595:*ARCHIVE* -980:ANNOUNCEMENT",  # exclusions that vary by model
    )
```

- The queries of the configuration are written for the legacy search (`980__a:X`, `088:X`) and `conf_query` translates them to the notation the models need (`980__.a:X`, `088__:X`). The second argument, the exclusions of the model, can be written in either notation.
- The queries of the configuration are joined with `OR`. Deleted, hidden and dummy records (`-980__:DELETED -980__:HIDDEN -980__.a:DUMMY`) are always excluded.
- The matcher of the models is **case sensitive** and knows only MARC fields. A search keyword such as `collection:"X"` never matches (`conf_query` gives a warning for it), and a field written without indicators (`088:X`) is read as blank indicators.
- The model is loaded when the application starts, so the configuration of the collection must be there: `migration_cli/collections` of the checkout, or the folder in `INVENIO_CDS_MIGRATOR_KIT_COLLECTIONS_DIR` (set in the Dockerfile).

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
