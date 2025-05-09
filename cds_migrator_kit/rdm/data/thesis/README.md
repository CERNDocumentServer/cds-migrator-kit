## Dump users

! Attention If you need to dump the users from legacy DB or you need to process the people collection

https://gitlab.cern.ch/cds-team/production_scripts/-/blob/master/cds-rdm/migration/dump_users.py?ref_type=heads

(creates active_users.json and missing_users.json)

## Dump latest people collection
```
inveniomigrator dump records -q '980__:"AUTHORITY" 980__:"PEOPLE"' --file-prefix peoples --chunk-size=1000
```

extract info from people collection (creates people.csv)
```
invenio migration users people-run --filepath cds_migrator_kit/rdm/data/users/people.csv --dirpath cds_migrator_kit/rdm/data/users/dump
```

add missing accounts (uses missing_users.json and people.csv)
```
invenio migration users submitters-run --dirpath /Users/kprzerwa/INVENIO/cds-migrator-kit/cds_migrator_kit/rdm/data/thesis/dump
```

first creates latest dump

```bash
inveniomigrator dump records -q '980:THESIS -980:DELETED -980:HIDDEN -980__c:MIGRATED -980__a:DUMMY' --file-prefix thesis --chunk-size=1000

```





invenio rdm-records add-to-fixture programmes
invenio rdm-records add-to-fixture awards
invenio rdm-records custom-fields init



0-1. Adapt ILS to consume thesis
0. Push all thesis to ILS
1. Run affiliations
2. Run users
3. Run duplicates (and 981__b) mergers
   3.1. add duplicated_pids.json file
4. Identify UDC records
5. Identify records with relations (2)
6. irecords with comments, migrate comments


next deployment

change branch installed in migrator-kit from feature to master

1. on worker pod
2.
invenio rdm-records add-to-fixture programmes
invenio rdm-records add-to-fixture awards

2. both on migration and worker pod
invenio rdm-records custom-fields init
invenio communities custom-fields init
