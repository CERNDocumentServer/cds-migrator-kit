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
from invenio_db import db
from invenio_files_rest.models import ObjectVersion, ObjectVersionTag


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

        print(f"Re-running ExtractMetadataTask for record {record_id}")
        payload["task_id"] = str(task.id)

        celery_task = ExtractMetadataTask()
        celery_task.clean(deposit_id=deposit_id, version_id=payload["version_id"])
        celery_task.s(**payload).apply_async()
        db.session.commit()
        fetch_tasks_status(flow_id, ExtractMetadataTask.name, timeout_seconds=120)

        # Remove from failed list so we don't run it twice
        failed_tasks = [t for t in failed_tasks if t[0] != ExtractMetadataTask.name]

        db.session.expire_all()
        try:
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
            print(f"An error occurred while handling ExtractMetadataTask: {e}")

    # --- Handle ExtractFramesTask separately ---
    if ExtractFramesTask.name in task_names:
        failed_task = next(t for t in failed_tasks if t[0] == ExtractFramesTask.name)
        task_id = failed_task[1]
        task = db.session.query(FlowTaskMetadata).get(task_id)
        task.status = FlowTaskStatus.PENDING
        db.session.commit()

        print(f"Re-running ExtractFramesTask for record {record_id}")
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
        print(f"Re-running failed task: {task_name} for record {record_id}")

        task_cls = _find_celery_task_by_name(task_name)
        if not task_cls:
            print(f"No Celery task class found for {task_name}. Skipping.")
            continue

        task = db.session.query(FlowTaskMetadata).get(task_id)
        task.status = FlowTaskStatus.PENDING
        db.session.commit()

        payload["task_id"] = str(task.id)

        celery_task = task_cls()
        celery_task.clean(deposit_id=deposit_id, version_id=payload["version_id"])
        celery_task.s(**payload).apply_async()
        db.session.commit()

    fetch_tasks_status(flow_id, task.name, timeout_seconds=120)


def fetch_tasks_status(flow_id, task_name, timeout_seconds=60):
    """Wait for a specific task in a flow to finish (SUCCESS or FAILURE)."""
    start_time = time.time()

    while True:
        elapsed_time = time.time() - start_time
        if elapsed_time >= timeout_seconds:
            print(f"Timeout reached after {timeout_seconds} seconds. Exiting.")
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
                print(f"✅ Task '{task.name}' completed successfully.")
            elif task.status == FlowTaskStatus.FAILURE:
                print(f"❌ Task '{task.name}' failed.")
            else:
                print(f"ℹTask '{task.name}' finished with status: {task.status.name}.")
            break

        time.sleep(5)  # Poll every 5 seconds


def finalize_tasks(deposit_id):
    # Always work on a clean session to avoid cached data
    db.session.expire_all()

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

    if run_chapters_task:
        #  Wait for ExtractFramesTask to finish to run ExtractChapterFramesTask
        print("Waiting for ExtractFramesTask to complete...")
        fetch_tasks_status(flow_id, ExtractFramesTask.name, timeout_seconds=240)
        print("Running ExtractChapterFramesTask...")

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

        # Poll for task completion
        fetch_tasks_status(flow_id, ExtractChapterFramesTask.name, timeout_seconds=240)


def fetch_flow_and_log(record_id, deposit_id, flow_id, failed_tasks, log_file_path):
    """Fetch the latest flow and write detailed info to the log file."""
    # Ensure we read the latest DB state
    db.session.expire_all()
    flow = db.session.query(FlowMetadata).get(flow_id)

    with open(log_file_path, "a") as log_file:
        log_file.write("\n" + "=" * 80 + "\n")
        log_file.write(f"Record ID: {record_id}\n")
        log_file.write(f"Deposit ID: {deposit_id}\n")
        log_file.write(f"Flow ID: {flow_id}\n")
        log_file.write("-" * 80 + "\n")

        # Log previously failed tasks
        if failed_tasks:
            log_file.write("Previously failed tasks:\n")
            for task_name, task_id in failed_tasks:
                log_file.write(f"  - {task_name} (ID: {task_id})\n")
        else:
            log_file.write("No previously failed tasks.\n")

        log_file.write("-" * 80 + "\n")
        log_file.write("Latest task statuses:\n")

        # Iterate all tasks in the flow and log their current statuses
        for task in flow.tasks:
            task_obj = db.session.query(FlowTaskMetadata).get(task.id)
            log_file.write(f"  • {task_obj.name:<30} | Status: {task_obj.status}\n")

        log_file.write("=" * 80 + "\n\n")


def load_record_ids(redirections_file_path):
    with open(redirections_file_path, "r") as f:
        data = json.load(f)

    # Extract all cds_videos_id values
    record_ids = [item["cds_videos_id"] for item in data]
    return record_ids


def main():
    # Create a log file
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file_path = f"/tmp/task_recovery_log_{timestamp}.txt"
    with open(log_file_path, "w") as log_file:
        pass

    redirections_file_path = "/tmp/record_redirections.json"
    all_record_ids = load_record_ids(redirections_file_path)
    record_ids = all_record_ids[:100] # any subset
    for record_id in record_ids:
        record = record_video_resolver(record_id)
        deposit_id = record["_deposit"]["id"]

        flow, failed_tasks = find_failed_tasks(deposit_id)
        flow_id = flow.id
        if not failed_tasks:
            print(f"No failed tasks found for record {record_id}.")
        else:
            run_failed_tasks(failed_tasks, flow, deposit_id, record_id)

        finalize_tasks(deposit_id)

        fetch_flow_and_log(record_id, deposit_id, flow_id, failed_tasks, log_file_path)
