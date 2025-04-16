# Weblectures Migration

To run the migration of weblectures locally, follow these steps:

## Dump a Subset of Records on Legacy

Run the following command to dump a subset of records:

.. code-block:: bash

    inveniomigrator dump records -q '8567_x:"Absolute master path" 8567_d:/mnt/master_share* -980__C:MIGRATED -980__c:DELETED -5831_a:digitized' --file-prefix lectures --chunk-size=1000

Place your dumps into the `cds_migrator_kit/videos/weblecture_migration/data/weblectures/dump/` folder, or update the `records/weblectures/extract/dirpath:` in `cds_migrator_kit/videos/weblecture_migration/streams.yaml`.

For the files, modify the `migration_config.py` file located at `cds_migrator_kit/videos/migration_config.py`, specifically updating the following variables:

- `MOUNTED_MEDIA_CEPH_PATH`

## Media Path

- The media path should contain subformats, frames, subtitles, additional files, and composite videos (if available).
- Composite videos will always be named as `<id-composite-...p-quality.mp4>`, and frames of the composite will be stored in `MOUNTED_MEDIA_CEPH_PATH/frames`.
- If no composite exists (i.e., the master contains only one video), subformats and frames will be obtained using `data.v2.json`.

## Missing Users Migration

If you need to migrate missing users, you need `missing_users.json` and `people.csv` files, you can find the files in CERNBOX:` \CDS Videos\Projects\Weblectures migration\user-files`. Place the files in `cds_migrator_kit/videos/weblecture_migration/data/users/` or update the `submitters/data_dir` in `cds_migrator_kit/videos/weblecture_migration/streams.yaml`. User migration is also using the same record dumps with record migration, so make sure you have your dumps in `cds_migrator_kit/videos/weblecture_migration/data/weblectures/dump/` folder.

To migrate the missing users, run:

.. code-block:: bash

    invenio migration videos submitters run

### How to Generate Required Files

#### `people.csv` file

- This file contains all transformed people records from the CDS *People Collection*.
- It can be co-used with rdm migration, no need to generate it specifically for videos migration.

To generate:

1. **Dump the People Collection**:

   .. code-block:: bash

       inveniomigrator dump records -q '980__:"AUTHORITY" 980__:"PEOPLE"' --file-prefix peoples --chunk-size=1000

2. **Set up a separate environment with RDM**:

   .. code-block:: bash

       pip install ".[rdm]"

3. **Run the people migration** to create the `people.csv` file:

   .. code-block:: bash

       invenio migration users people-run --filepath <folder_path_to_people_dump>

4. **Place the generated file in** `cds_migrator_kit/videos/weblecture_migration/data/users/`


#### `missing_users.json` file

- This file contains all users in CDS Legacy `user` table.
- Like `people.csv`, it can also be co-used with rdm migration. 

To generate:
1. Go to `production_scripts` repository in GitLab
2. Run the `dump_users.py` script in `cds-rdm/migration/` folder.
3. Place the created file in `cds_migrator_kit/videos/weblecture_migration/data/users/`


#### Make the files specific for videos

If you don't want to use all users for the videos migration, you can create a smaller subset using only the possible submitters from the records to be migrated.

- For the weblecture migration:
    - There are 634 unique submitters
    - 123 of them are missing in videos production.

- You can generate a subset using your list of submitter emails:
    - All weblecture submitter emails in CERNBOX: ` \CDS Videos\Projects\Weblectures migration\user-files\different_submitters.txt`

- You can use already generated subset files in CERNBOX folder: `CDS Videos\Projects\Weblectures migration\user-files\`


## Creating System User

If you need to use a system user for the migration, you can configure the username by updating the `WEBLECTURES_MIGRATION_SYSTEM_USER` variable in the `migration_config.py` file.

To create the system user (if it doesn't already exist), run:

.. code-block:: bash

    invenio migration videos submitters create-system-user

## Running the Migration

Once you have the dump and files, you can proceed with the migration.

To create records with actual files, run:

.. code-block:: bash

    invenio migration videos weblectures run

To create records with dummy files (validating that actual files exist), run:

.. code-block:: bash

    invenio migration videos weblectures run --dry-run

## Visualizing Errors Locally

To visualize errors locally, run:

.. code-block:: shell

    gunicorn -b :8080 --timeout 120 --graceful-timeout 60 cds_migrator_kit.app:app --reload --log-level debug --capture-output --access-logfile '-' --error-logfile '-'
