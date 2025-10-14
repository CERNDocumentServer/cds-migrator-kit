import datetime
import json

from cds.modules.deposit.api import (
    deposit_video_resolver,
    get_master_object,
    record_video_resolver,
)
from cds.modules.flows.models import FlowMetadata, FlowTaskMetadata, FlowTaskStatus
from cds.modules.flows.tasks import (
    ExtractChapterFramesTask,
    ExtractFramesTask,
    ExtractMetadataTask,
    TranscodeVideoTask,
)
from cds.modules.records.utils import parse_video_chapters
from invenio_db import db
from invenio_files_rest.models import ObjectVersion, ObjectVersionTag

SUCCESS_LOG_PATH = None
ERROR_LOG_PATH = None


def log_success(message):
    """Write a success message to the success log file."""
    if not SUCCESS_LOG_PATH:
        raise RuntimeError("SUCCESS_LOG_PATH is not initialized.")
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(SUCCESS_LOG_PATH, "a") as log_file:
        log_file.write(f"[{timestamp}] {message}\n")
    print(message)


def log_error(message):
    """Write an error message to the error log file."""
    if not ERROR_LOG_PATH:
        raise RuntimeError("ERROR_LOG_PATH is not initialized.")
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(ERROR_LOG_PATH, "a") as log_file:
        log_file.write(f"[{timestamp}] {message}\n")
    print(message)


def copy_master_tags_between_buckets(src_bucket, dst_bucket):
    """Copy tags of the master ObjectVersion from src_bucket to dst_bucket."""
    # Find master in deposit
    src_master = get_master_object(src_bucket)

    # Find master in record
    dst_master = ObjectVersion.get(dst_bucket, src_master.key)

    # Update tags because it'll not update during publish
    for tag in src_master.tags:
        ObjectVersionTag.create_or_update(dst_master, tag.key, tag.value)

    db.session.commit()


def _find_celery_task_by_name(name):
    for celery_task in [
        ExtractMetadataTask,
        ExtractFramesTask,
        ExtractChapterFramesTask,
        TranscodeVideoTask,
    ]:
        if celery_task.name == name:
            return celery_task


def find_failed_tasks(deposit_id):
    db.session.expire_all()
    flow = FlowMetadata.get_by_deposit(deposit_id)
    failed_tasks = []
    for task in flow.tasks:
        task = db.session.query(FlowTaskMetadata).get(task.id)
        if task.status == FlowTaskStatus.FAILURE:
            failed_tasks.append((task.name, task.id))

    return flow, failed_tasks


def find_succeded_tasks(deposit_id):
    db.session.expire_all()
    flow = FlowMetadata.get_by_deposit(deposit_id)
    succeded_tasks = []
    for task in flow.tasks:
        task = db.session.query(FlowTaskMetadata).get(task.id)
        if task.status == FlowTaskStatus.SUCCESS:
            succeded_tasks.append((task.name, task.id))

    return flow, succeded_tasks


def run_metatadata_task(failed_tasks, flow, deposit_id, record_id):
    task_names = [task[0] for task in failed_tasks]
    deposit = deposit_video_resolver(deposit_id)
    
    if ExtractMetadataTask.name not in task_names:
        if deposit["_deposit"]["status"] == "draft":
            log_success(f"ExtractMetadataTask not failed and deposit already in draft for record {record_id}.")
            return True
        return False

    if deposit["_deposit"]["status"] == "published":
        deposit.edit().commit()
        db.session.commit()
        log_success(f"Deposit {deposit_id} set to draft for record {record_id}.")

    payload = flow.payload.copy()
    failed_task = next(t for t in flow.tasks if t.name == ExtractMetadataTask.name)
    task_id = failed_task.id
    task = db.session.query(FlowTaskMetadata).get(task_id)
    task.status = FlowTaskStatus.PENDING
    db.session.commit()

    log_success(f"Re-running ExtractMetadataTask for record {record_id}")
    payload["task_id"] = str(task.id)

    celery_task = ExtractMetadataTask()
    celery_task.clean(deposit_id=deposit_id, version_id=payload["version_id"])
    celery_task.s(**payload).apply_async()
    db.session.commit()
    return True


def run_failed_tasks(failed_tasks, flow, deposit_id, record_id):
    failed_tasks = failed_tasks.copy()
    payload = flow.payload.copy()
    task_names = [task[0] for task in failed_tasks]
    republish_needed = False
    if ExtractFramesTask.name in task_names:
        deposit = deposit_video_resolver(deposit_id)
        if deposit["_deposit"]["status"] == "published":
            deposit.edit().commit()
            db.session.commit()
            log_success(f"Deposit {deposit_id} set to draft for record {record_id}.")
            republish_needed = True

    # --- Handle if other task failed ---
    for task_name, task_id in failed_tasks:
        log_success(f"Re-running failed task: {task_name} for record {record_id}")

        task_cls = _find_celery_task_by_name(task_name)
        if not task_cls:
            log_success(f"No Celery task class found for {task_name}. Skipping.")
            continue

        task = db.session.query(FlowTaskMetadata).get(task_id)
        task.status = FlowTaskStatus.PENDING
        db.session.commit()

        log_success(f"Re-running {task_name} for record {record_id}")
        payload["task_id"] = str(task.id)

        celery_task = task_cls()
        celery_task.clean(deposit_id=deposit_id, version_id=payload["version_id"])
        celery_task.s(**payload).apply_async()
        db.session.commit()
    return republish_needed


def run_chapters_task(deposit_id, record_id, flow_id):
    """Run the chapters task for a given deposit and flow."""
    deposit = deposit_video_resolver(deposit_id)
    if deposit["_deposit"]["status"] != "published":
        log_error(
            f"ERROR: Deposit not published for record {record_id}. Chapter task will not run."
        )
        return

    # Always work on a clean session to avoid cached data
    db.session.expire_all()

    # Get the record to see if it has chapters
    record = record_video_resolver(record_id)
    description = record.get("description", "")
    chapters = parse_video_chapters(description)
    if not chapters:
        log_success(f"No chapters found for record {record_id}. Skipping chapters.")
        return

    flow = FlowMetadata.get_by_deposit(deposit_id)
    flow_id = flow.id
    payload = flow.payload.copy()

    # Determine if ExtractChapterFramesTask needs to run
    run_chapters_task = True
    for task in flow.tasks:
        if (
            task.name == ExtractChapterFramesTask.name
            and task.status == FlowTaskStatus.SUCCESS
        ):
            run_chapters_task = False
        if (
            task.name == ExtractMetadataTask.name or task.name == ExtractFramesTask.name
        ) and task.status == FlowTaskStatus.FAILURE:
            run_chapters_task = False
            log_error(
                f"ERROR: ExtractMetadataTask/ExtractFramesTask failed for record {record_id}. Chapter task will not run."
            )

    if run_chapters_task:
        log_success("Running ExtractChapterFramesTask...")

        # Create a FlowTaskMetadata
        new_task = FlowTaskMetadata(
            flow_id=flow_id,
            name=ExtractChapterFramesTask.name,
            status=FlowTaskStatus.PENDING,
        )
        db.session.add(new_task)
        db.session.commit()

        payload["task_id"] = str(new_task.id)
        ExtractChapterFramesTask().s(**payload).apply_async()

        log_success("ExtractChapterFramesTask started async.")
    else:
        log_success("Skipped ExtractChapterFramesTask.")


def rerun_chapters_task(deposit_id, record_id, flow_id):
    """Rerun the chapters task for a given deposit and flow."""
    deposit = deposit_video_resolver(deposit_id)
    if deposit["_deposit"]["status"] != "published":
        log_error(
            f"ERROR: Deposit not published for record {record_id}. Chapter task will not run."
        )
        return

    # Always work on a clean session to avoid cached data
    db.session.expire_all()

    # Get the record to see if it has chapters
    record = record_video_resolver(record_id)
    description = record.get("description", "")
    chapters = parse_video_chapters(description)
    if not chapters:
        log_success(f"No chapters found for record {record_id}. Skipping chapters.")
        return

    flow = FlowMetadata.get_by_deposit(deposit_id)
    flow_id = flow.id
    payload = flow.payload.copy()
    
    task = next(t for t in flow.tasks if t.name == ExtractChapterFramesTask.name)

    # Determine if ExtractChapterFramesTask needs to run
    run_chapters_task = True
    for task in flow.tasks:
        if (
            task.name == ExtractChapterFramesTask.name
            and task.status == FlowTaskStatus.SUCCESS
        ):
            run_chapters_task = False
        if (
            task.name == ExtractMetadataTask.name or task.name == ExtractFramesTask.name
        ) and task.status == FlowTaskStatus.FAILURE:
            run_chapters_task = False
            log_error(
                f"ERROR: ExtractMetadataTask/ExtractFramesTask failed for record {record_id}. Chapter task will not run."
            )

    if run_chapters_task:
        log_success("Running ExtractChapterFramesTask...")

        if not task:
            # Create a FlowTaskMetadata
            new_task = FlowTaskMetadata(
                flow_id=flow_id,
                name=ExtractChapterFramesTask.name,
                status=FlowTaskStatus.PENDING,
            )
            db.session.add(new_task)
            db.session.commit()
        else:
            new_task = task

        payload["task_id"] = str(new_task.id)
        ExtractChapterFramesTask().s(**payload).apply_async()

        log_success("ExtractChapterFramesTask started async.")
    else:
        log_success("Skipped ExtractChapterFramesTask.")


def load_record_ids(record_states_file_path):
    with open(record_states_file_path, "r") as f:
        data = json.load(f)

    # Extract all cds_videos_id values
    record_ids = [item["cds_videos_recid"] for item in data]
    return record_ids


# ---------------------------
#        MAIN METHODS
# ---------------------------

# WEBLECTURES AYNC TASKS RUNNER
def weblectures_tasks_runner():
    global SUCCESS_LOG_PATH, ERROR_LOG_PATH
    # Create success and error log files
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    SUCCESS_LOG_PATH = f"/tmp/task_recovery_log_success_{timestamp}.txt"
    ERROR_LOG_PATH = f"/tmp/task_recovery_log_error_{timestamp}.txt"

    with open(SUCCESS_LOG_PATH, "w") as log_file:
        log_file.write(
            f"Success Log - Started at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        )
        log_file.write("=" * 80 + "\n\n")

    with open(ERROR_LOG_PATH, "w") as log_file:
        log_file.write(
            f"Error Log - Started at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        )
        log_file.write("=" * 80 + "\n\n")

    # Records states file created during migration
    record_states_file_path = "rdm_records_state.json"
    
    all_record_ids = load_record_ids(record_states_file_path)
    
    # Run in batches
    record_ids = all_record_ids[:1000] # any subset
    total = len(record_ids)
    deposits_to_republish = []

    # First run metadata tasks async
    for i, record_id in enumerate(record_ids, start=1):
        log_success(f"Processing {i}/{total} record: {record_id}")
        record = record_video_resolver(record_id)
        deposit_id = record["_deposit"]["id"]

        flow, failed_tasks = find_failed_tasks(deposit_id)
        if not failed_tasks:
            log_success(f"No failed tasks found for record {record_id}.")
        else:
            task_names = [task[0] for task in failed_tasks]
            republish_needed = run_metatadata_task(failed_tasks, flow, deposit_id, record_id)
            if republish_needed:
                deposits_to_republish.append((deposit_id, record_id))

    # !! Make sure metadata tasks finished!! Run remaining tasks (frames/transcoding) async 
    for i, record_id in enumerate(record_ids, start=1):
        log_success(f"Processing {i}/{total} record: {record_id}")
        record = record_video_resolver(record_id)
        deposit_id = record["_deposit"]["id"]

        flow, failed_tasks = find_failed_tasks(deposit_id)
        task_names = [task[0] for task in failed_tasks]
        if ExtractMetadataTask.name in task_names:
            log_error(f"ERROR: Record: {record_id} still has failed ExtractMetadataTask. Skipping further processing.")
            continue
        if not failed_tasks:
            log_success(f"No failed tasks found for record {record_id}.")
        else:
            log_success(f"Re-running failed tasks: {task_names}")
            republish_needed = run_failed_tasks(failed_tasks, flow, deposit_id, record_id)
            if republish_needed and (deposit_id, record_id) not in deposits_to_republish:
                print(f"Adding deposit {deposit_id} for record {record_id} to republish list.")
                deposits_to_republish.append((deposit_id, record_id))

    # Re-publish records
    total_publish = len(deposits_to_republish)
    for i, (deposit_id, record_id) in enumerate(deposits_to_republish, start=1):
        log_success(f"Processing publish {i}/{total_publish} record: {record_id}")
        deposit = deposit_video_resolver(deposit_id)
        if deposit["_deposit"]["status"] == "published":
            log_success(f"Deposit already published for record {record_id}. Skipping publish.")
            # continue
        flow, succeded_tasks = find_succeded_tasks(deposit_id)
        task_names = [task[0] for task in succeded_tasks]
        metadata_task_succeded = ExtractMetadataTask.name in task_names
        frames_task_succeded = ExtractFramesTask.name in task_names

        if metadata_task_succeded:
            record = record_video_resolver(record_id)
            deposit = deposit_video_resolver(deposit_id)
            copy_master_tags_between_buckets(
                src_bucket=deposit.bucket,
                dst_bucket=record["_buckets"]["record"],
            )
    
        if metadata_task_succeded and frames_task_succeded:
            deposit.publish(extract_chapters=False).commit()
            log_success(f"Deposit {deposit_id} published for record {record_id}.")
        else:
            log_error(f"ERROR: Record: {record_id} has failed tasks: ExtractMetadataTask: {metadata_task_succeded} ExtractFramesTask: {frames_task_succeded}")
            deposit.publish(extract_chapters=False).commit()
        db.session.commit()


# WEBLECTURES CHAPTERS ASYNC RUNNER
def weblectures_chapters_async_runner():
    global SUCCESS_LOG_PATH, ERROR_LOG_PATH
    # Create success and error log files
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    SUCCESS_LOG_PATH = f"/tmp/task_chapters_errored_log_success_{timestamp}.txt"
    ERROR_LOG_PATH = f"/tmp/task_chapters_errored_log_error_{timestamp}.txt"

    with open(SUCCESS_LOG_PATH, "w") as log_file:
        log_file.write(
            f"Success Log - Started at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        )
        log_file.write("=" * 80 + "\n\n")

    with open(ERROR_LOG_PATH, "w") as log_file:
        log_file.write(
            f"Error Log - Started at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        )
        log_file.write("=" * 80 + "\n\n")

    # Records states file created during migration
    record_states_file_path = "rdm_records_state.json"
    
    all_record_ids = load_record_ids(record_states_file_path)

    # Run in batches
    record_ids = all_record_ids[:2000] # any subset     
    total = len(record_ids)

    # Run chapters tasks async
    for i, record_id in enumerate(record_ids, start=1):
        log_success(f"Processing {i}/{total} record: {record_id}")
        record = record_video_resolver(record_id)
        deposit_id = record["_deposit"]["id"]
        flow, failed_tasks = find_failed_tasks(deposit_id)
        flow_id = flow.id
        run_chapters_task(deposit_id, record_id, flow_id)


# WEBLECTURES GENERAL FAILED TASK CHECKER
def weblectures_check_task_status():
    FAILED_TASKS_LOG = f"/tmp/failed_task_records.txt"
    DRAFT_DEPOSITS_LOG = f"/tmp/draft_deposits.txt"

    with open(FAILED_TASKS_LOG, "w") as log_file:
        pass

    with open(DRAFT_DEPOSITS_LOG, "w") as log_file:
        log_file.write(
            f"Error Log - Started at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        )
        log_file.write("=" * 80 + "\n\n")

    # Records states file created during migration
    record_states_file_paths = [
        "/tmp/weblectures_1_rdm_records_state.json",
        "/tmp/weblectures_2_rdm_records_state.json",
        "/tmp/weblectures_3_rdm_records_state.json",
        "/tmp/weblectures_4_rdm_records_state.json",
        "/tmp/weblectures_5_rdm_records_state.json",
        "/tmp/weblectures_6_rdm_records_state.json",
        "/tmp/weblectures_7_rdm_records_state.json",
        "/tmp/last_lectures_rdm_records_state.json",
        ]

    all_record_ids = []
    for record_states_file_path in record_states_file_paths:
        record_ids = load_record_ids(record_states_file_path)
        all_record_ids.extend(record_ids)

    record_ids = all_record_ids
    total = len(record_ids)

    failed_deposits = []     # stores (record_id, [task_names])
    failed_chapters_records = [] # stores record_ids with failed chapters task
    draft_deposits = []      # stores (record_id, deposit_id)

    for i, record_id in enumerate(record_ids, start=1):
        if i % 100 == 0:
            print(f"Processing {i}/{total} record: {record_id}")
        record = record_video_resolver(record_id)
        deposit_id = record["_deposit"]["id"]
        deposit = deposit_video_resolver(deposit_id)

        flow, failed_tasks = find_failed_tasks(deposit_id)
        if failed_tasks:
            task_names = [task[0] for task in failed_tasks]
            failed_deposits.append((record_id, task_names))
            if ExtractChapterFramesTask.name in task_names:
                failed_chapters_records.append(record_id)
        if deposit["_deposit"]["status"] == "draft":
            draft_deposits.append((record_id, deposit_id))

    # -----------------------------
    # Write failed tasks to log
    # -----------------------------
    with open(DRAFT_DEPOSITS_LOG, "a") as log_file:
        log_file.write("Records state file: " + record_states_file_path + "\n")
        log_file.write("-" * 80 + "\n")

        for record_id, deposit_id in draft_deposits:
            log_file.write(f"Record ID: {record_id}, Deposit ID: {deposit_id}\n")

        log_file.write("\n\n")

    with open(FAILED_TASKS_LOG, "a") as log_file:
        log_file.write("Records state file: " + record_states_file_path + "\n")
        log_file.write("-" * 80 + "\n")

        for record_id, task_names in failed_deposits:
            log_file.write(f"Record ID: {record_id}\n")
            log_file.write(f"Failed Tasks: {', '.join(task_names)}\n")
            log_file.write("\n")

    print("Logs written:")
    print(f"- {DRAFT_DEPOSITS_LOG}")
    print(f"- {FAILED_TASKS_LOG}")

    # publish drafts
    for i, (record_id, deposit_id) in enumerate(draft_deposits, start=1):
        log_success(f"Publish {i}/{len(draft_deposits)} record: {record_id}")
        db.session.expire_all()
        deposit = deposit_video_resolver(deposit_id)
        deposit.publish().commit()
        log_success(f"Republished record {record_id}.")
        db.session.commit()

    # run chapters again 
    total = len(failed_chapters_records)
    for i, record_id in enumerate(failed_chapters_records, start=1):
        log_success(f"Processing {i}/{total} record: {record_id}")
        record = record_video_resolver(record_id)
        deposit_id = record["_deposit"]["id"]
        flow, failed_tasks = find_failed_tasks(deposit_id)
        flow_id = flow.id
        rerun_chapters_task(deposit_id, record_id, flow_id)
