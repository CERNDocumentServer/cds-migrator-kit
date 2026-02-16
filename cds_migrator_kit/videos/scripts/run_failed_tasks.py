import datetime
import json
import time

from cds.modules.deposit.api import (
    deposit_video_resolver,
    get_master_object,
    record_video_resolver,
)
from cds.modules.flows.deposit import index_deposit_project
from cds.modules.flows.models import FlowMetadata, FlowTaskMetadata, FlowTaskStatus
from cds.modules.flows.tasks import (
    ExtractChapterFramesTask,
    ExtractFramesTask,
    ExtractMetadataTask,
    TranscodeVideoTask,
    sync_records_with_deposit_files,
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
    flow = FlowMetadata.get_by_deposit(deposit_id)
    failed_tasks = []

    for task in flow.tasks:
        task = db.session.query(FlowTaskMetadata).get(task.id)
        if task.status == FlowTaskStatus.FAILURE:
            failed_tasks.append((task.name, task.id))

    return flow, failed_tasks


def run_failed_tasks(failed_tasks, flow, deposit_id, record_id):
    failed_tasks = failed_tasks.copy()
    payload = flow.payload.copy()
    task_names = [task[0] for task in failed_tasks]
    flow_id = flow.id

    # --- Handle ExtractMetadataTask separately ---
    if ExtractMetadataTask.name in task_names:
        failed_task = next(t for t in failed_tasks if t[0] == ExtractMetadataTask.name)
        task_id = failed_task[1]
        task = db.session.query(FlowTaskMetadata).get(task_id)
        task.status = FlowTaskStatus.PENDING
        db.session.commit()

        log_success(f"Re-running ExtractMetadataTask for record {record_id}")
        payload["task_id"] = str(task.id)

        celery_task = ExtractMetadataTask()
        celery_task.clean(deposit_id=deposit_id, version_id=payload["version_id"])
        celery_task.s(**payload).apply_async()
        db.session.commit()
        fetch_tasks_status(flow_id, ExtractMetadataTask.name, timeout_seconds=300)

        # Remove from failed list so we don't run it twice
        failed_tasks = [t for t in failed_tasks if t[0] != ExtractMetadataTask.name]

        db.session.expire_all()
        try:
            # Make sure it finished
            fetch_tasks_status(flow_id, ExtractMetadataTask.name, timeout_seconds=100)
            flow = db.session.query(FlowMetadata).get(flow_id)
            deposit = deposit_video_resolver(deposit_id)
            extracted_metadata = deposit["_cds"]["extracted_metadata"]
            record = record_video_resolver(record_id)
            record["_cds"]["extracted_metadata"] = extracted_metadata
            record.commit()
            db.session.commit()
            copy_master_tags_between_buckets(
                src_bucket=deposit.bucket,
                dst_bucket=record["_buckets"]["record"],
            )
        except Exception as e:
            log_error(f"ERROR: ExtractMetadataTask: {e}")

    # --- Handle ExtractFramesTask separately ---
    if ExtractFramesTask.name in task_names:
        failed_task = next(t for t in failed_tasks if t[0] == ExtractFramesTask.name)
        task_id = failed_task[1]
        task = db.session.query(FlowTaskMetadata).get(task_id)
        task.status = FlowTaskStatus.PENDING
        db.session.commit()

        log_success(f"Re-running ExtractFramesTask for record {record_id}")
        payload["task_id"] = str(task.id)

        celery_task = ExtractFramesTask()
        celery_task.clean(deposit_id=deposit_id, version_id=payload["version_id"])
        celery_task.s(**payload).apply_async()
        db.session.commit()
        fetch_tasks_status(flow_id, ExtractFramesTask.name, timeout_seconds=120)
        # Sync files between deposit and record
        sync_records_with_deposit_files(deposit_id)

        # Remove from failed list so we don't run it twice
        failed_tasks = [t for t in failed_tasks if t[0] != ExtractFramesTask.name]

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

        payload["task_id"] = str(task.id)

        celery_task = task_cls()
        celery_task.clean(deposit_id=deposit_id, version_id=payload["version_id"])
        celery_task.s(**payload).apply_async()
        db.session.commit()


def fetch_tasks_status(flow_id, task_name, timeout_seconds=60):
    """Wait for a specific task in a flow to finish (SUCCESS or FAILURE)."""
    start_time = time.time()

    while True:
        elapsed_time = time.time() - start_time
        if elapsed_time >= timeout_seconds:
            log_error(f"Timeout reached after {timeout_seconds} seconds. Exiting.")
            break

        # Force SQLAlchemy to fetch fresh data from the DB
        db.session.expire_all()

        flow = db.session.query(FlowMetadata).get(flow_id)

        # Find the task with the given name
        task = next((t for t in flow.tasks if t.name == task_name), None)

        # Refresh the specific task from DB
        task = db.session.query(FlowTaskMetadata).get(task.id)

        if task.status == FlowTaskStatus.PENDING:
            print(f"Task '{task.name}' is still pending. Waiting...")
        elif task.status == FlowTaskStatus.STARTED:
            print(f"Task '{task.name}' is started. Waiting...")
        else:
            # Explicit success/failure logs
            if task.status == FlowTaskStatus.SUCCESS:
                log_success(f"SUCCESS Task '{task.name}' completed successfully.")
            elif task.status == FlowTaskStatus.FAILURE:
                log_error(f"ERROR: Task '{task.name}' failed.")
            else:
                log_success(
                    f"Task '{task.name}' finished with status: {task.status.name}."
                )
            break

        time.sleep(5)  # Poll every 5 seconds


def run_chapters_task(deposit_id, record_id, flow_id):
    """Run the chapters task for a given deposit and flow."""
    # Make sure all ExtractMetadataTask and ExtractFramesTask finished
    fetch_tasks_status(flow_id, ExtractMetadataTask.name, timeout_seconds=60)
    fetch_tasks_status(flow_id, ExtractFramesTask.name, timeout_seconds=60)

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
                f"ERROR: ExtractMetadataTask/ExtractFramesTask failed for deposit {deposit_id}. Chapter task will not run."
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


def load_record_ids(record_states_file_path):
    with open(record_states_file_path, "r") as f:
        data = json.load(f)

    # Extract all cds_videos_id values
    record_ids = [item["cds_videos_recid"] for item in data]
    return record_ids


def main():
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
    record_states_file_path = "/migration/logs/collection_name/rdm_records_state.json"
    all_record_ids = load_record_ids(record_states_file_path)
    record_ids = all_record_ids[:100]  # any subset
    for record_id in record_ids:
        log_success(f"Processing record {record_id}")
        record = record_video_resolver(record_id)
        deposit_id = record["_deposit"]["id"]

        flow, failed_tasks = find_failed_tasks(deposit_id)
        flow_id = flow.id
        if not failed_tasks:
            log_success(f"No failed tasks found for record {record_id}.")
        else:
            task_names = [task[0] for task in failed_tasks]
            log_success(f"Failed tasks: {task_names}")
            run_failed_tasks(failed_tasks, flow, deposit_id, record_id)

    # After all records are processed for metadata and frames we can run the chapters task
    for record_id in record_ids:
        log_success(f"Processing record {record_id}")
        record = record_video_resolver(record_id)
        deposit_id = record["_deposit"]["id"]

        flow, failed_tasks = find_failed_tasks(deposit_id)
        flow_id = flow.id
        run_chapters_task(deposit_id, record_id, flow_id)
