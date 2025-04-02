```bash
inveniomigrator dump records -q '980__:THESIS -980__c:DELETED -980__c:HIDDEN -980__c:MIGRATED' --file-prefix thesis --chunk-size=1000

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
