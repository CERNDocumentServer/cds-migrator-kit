# Weblectures Migration

To run the migration of weblectures locally, follow these steps:

## Dump a Subset of Records on Legacy

Run the following command on webnode: `cds-migration-01` to dump a subset of records:

.. code-block:: bash

    inveniomigrator dump records -q "8567_x:'Absolute master path' 8567_d:/mnt/master_share* -980__C:MIGRATED -980__c:DELETED -5831_a:digitized" --file-prefix lectures --chunk-size=1000


> **Note:**  
> For the query, be sure to use single quotes (`'`) instead of double quotes (`"`),  

Place your dumps into the `cds_migrator_kit/videos/weblecture_migration/data/weblectures/dump/` folder, or update the `records/weblectures/extract/dirpath:` in `cds_migrator_kit/videos/weblecture_migration/streams.yaml`.

For the files, modify the `migration_config.py` file located at `cds_migrator_kit/videos/migration_config.py`, specifically updating the following variables:

- `MOUNTED_MEDIA_CEPH_PATH`

## Media Path

- The media path should contain subformats, frames, subtitles, additional files, and composite videos (if available).
- Composite videos will always be named as `<id-composite-...p-quality.mp4>`, and frames of the composite will be stored in `MOUNTED_MEDIA_CEPH_PATH/frames`.
- If no composite exists (i.e., the master contains only one video), subformats and frames will be obtained using `data.v2.json`.

For copying files, see [How to copy CEPH media files](#how-to-copy-ceph-media-files).

## Record Files

Some records contain additional AFS files besides the lecturemedia files. These AFS files should be copied to EOS before migration. Make sure to update the `records/weblectures/transform/files_dump_dir` in `cds_migrator_kit/videos/weblecture_migration/streams.yaml` with EOS path where your AFS files are stored.

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

### Configure how to retrieve media files

There are two ways to obtain the correct files during migration:

#### 1. Use pre-generated JSON files with record IDs
To generate these files, see [**Copy only needed files and generate their EOS paths to use in migration**](#option-1-copy-only-needed-files-and-generate-their-eos-paths-to-use-in-migration).  

Set the configuration variable:  
`USE_GENERATED_FILE_PATHS = True` (default)

#### 2. Transform file paths during the migration process
Ensure you have copied all the required files and the `data.v2.json` for each folder.  

Set the configuration variable:  
`USE_GENERATED_FILE_PATHS = False`

File paths will then be transformed during the migration.

### Run the migration

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


## Clean-up dev env

### 1. Drop all public tables

```bash
psql -U cds -h dbod-cdsvideos-dev.cern.ch -p 6623 -d cds
# password from paas secret
```

```sql
DO $$
DECLARE
    stmt text;
BEGIN
    SELECT string_agg(
        'DROP TABLE IF EXISTS "' || tablename || '" CASCADE;', 
        ' '
    )
    INTO stmt
    FROM pg_tables
    WHERE schemaname = 'public';

    -- EXECUTE dynamic SQL string
    EXECUTE stmt;
END $$;
```
### 2. Reinitialize database and index

```bash
cds db init create
cds index destroy --force --yes-i-know
cds index init --force
cds index queue init purge
```

### 3. Declare stat queues

```bash
cds queues declare
```
### 4. Create QA role and permissions

```bash
cds roles create cds-operators-qa
cds access allow deposit-admin-access role cds-operators-qa
cds access allow superuser-access role cds-operators-qa
cds roles create cern-user
cds access allow videos-upload-access role cern-user

cds roles add <user> cds-operators-qa
```

### 5. Create a default files location

```bash
cds files location --default videos root://eosmedia.cern.ch//eos/media/cds-videos/dev/files/
```

### 6. Load demo data

```bash
cds fixtures sequence-generator
cds fixtures categories
cds fixtures pages
cds fixtures keywords
cds fixtures licenses
```

### 7. Reindex OpenDefinition licenses

```bash
cds index reindex -t od_lic --yes-i-know
cds index run
```

## How to copy CEPH media files

### Step 1: Generate media files

Change `folders/extract/dirpath` in your steams.yaml file. It should be the folder of your dumps.

You can generate the needed media files list with this command:

```bash
invenio migration videos weblectures extract-files-paths
```

This will create a json file with all the files in the marc record.

Alternatively, you can use the pre-generated json file here: 
[marc files](https://cernbox.cern.ch/files/spaces/eos/project/d/digital-repositories/Services/CDS/CDS%20Videos/Projects/Weblectures%20migration/weblecture_migration_marc_files.json)

**If you want to copy the full media_data folder for the records:**
You can use the pre-generated txt file here: 
[all_media_files.txt](https://cernbox.cern.ch/files/spaces/eos/project/d/digital-repositories/Services/CDS/CDS%20Videos/Projects/Weblectures%20migration/master_folders.txt)

### Step 2: Copy the file to the target machine

Copy the created file to `cds-test-wn-21/tmp`

### Step 3: Connect and prepare the environment

To be more safe, connect your VM and in your VM:

1. Connect to machine: `cds-test-wn-21`
2. Mount CEPH, check [here](https://gitlab.cern.ch/cds-team/cds-videos-openshift/-/issues/13)
3. Mount EOS if needed:
   ```bash
   kinit videoseostest
   ```

### Step 4: Run the copy script

#### Option 1: Copy only needed files and generate their EOS paths to use in migration

1. Open an **IPython** shell.  
2. Run the [`copy_files.py`](scripts/copy_files.py) script.  

This script will:
- Copy only the files needed for migration.  
- Generate a **JSON file** containing all the corresponding EOS paths for the records.  

After the script finishes, update your configuration:
1. Open the streams.yaml file: `cds_migrator_kit/videos/weblecture_migration/streams.yaml`
2. Update the following field with the EOS directory where your generated JSON files are stored: `records/weblectures/transform/eos_file_paths_dir: <path_to_your_generated_json_files>`

#### Option 2: Copy directly the folder

Create a shell script file and paste the following content:

```bash
#!/bin/bash

SOURCE_ROOT="/mnt/cephfs"
DEST_ROOT="/eos/media/cds-videos/dev/stage"
TXT_FILE="/tmp/master_folders.txt"

# Read each line from the txt file
while IFS= read -r line; do
    # Expected format: "<recid>--<path>"
    record_id="${line%%--*}"
    relative_path="${line#*--}"

    # Validation
    if [[ "$line" != *"--"* ]] || [ -z "$record_id" ] || [ -z "$relative_path" ]; then
        echo "Warning: Malformed line: $line"
        continue
    fi

    # Full source and destination paths
    full_source_path="$SOURCE_ROOT$relative_path"
    full_dest_path="$DEST_ROOT$relative_path"

    # Check if the source folder exists
    if [ -d "$full_source_path" ]; then
        # Skip if destination already exists
        if [ -d "$full_dest_path" ]; then
            echo "Skipped (already exists): $full_dest_path"
            continue
        fi

        # Create the destination directory
        mkdir -p "$full_dest_path"

        # Copy the contents
        cp -a "$full_source_path/." "$full_dest_path/"
        echo "Copied: $full_source_path -> $full_dest_path"
    else
        echo "Warning: Source folder does not exist: $full_source_path"
    fi
done < "$TXT_FILE"

echo "Done."
```

Make your shell file executable and run it:

```bash
chmod +x your_script.sh
./your_script.sh
```

### Alternative: Copy files within EOS

If you want to copy files from EOS to EOS within `cds-test-wn-21`, you can run:

```bash
rsync -av --ignore-existing /eos/media/cds-videos/dev/acad/media_data/ /eos/media/cds-videos/dev/stage/media_data/
```