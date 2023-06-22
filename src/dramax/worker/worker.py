import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Callable

import dramatiq
import minio
from dramatiq import MessageProxy
from dramatiq.brokers.rabbitmq import RabbitmqBroker
from dramatiq.middleware import CurrentMessage, Retries
from minio import Minio
from structlog import get_logger

from dramax.executor.docker import run_container
from dramax.manager import TaskManager, WorkflowManager
from dramax.models.task import Result, Status
from dramax.models.workflow import WorkflowStatus
from dramax.settings import settings

log = get_logger("dramax.worker")

log.info("Setting up RabbitMQ broker", url=settings.rabbit_dns)

broker = RabbitmqBroker(url=settings.rabbit_dns)
broker.add_middleware(CurrentMessage())

retries = Retries(
    max_retries=0
)  # TODO: Retries assigns a new message ID to the task, which breaks the workflow. Fix this.
broker.add_middleware(retries)

dramatiq.set_broker(broker)

log.info("Connected to queue", queue_name=settings.default_actor_opts.queue_name)

log.info("Worker is ready")


@dramatiq.actor(**settings.default_actor_opts.dict())
def worker(task: dict, workflow_id: str):
    """
    Executes an arbitrary function defined by a task and updates its state.

    :param task: Task execution request object.
    :param workflow_id: Workflow ID.
    """
    message = CurrentMessage.get_current_message()

    log = get_logger()
    log = log.bind(task_id=message.message_id, workflow_id=workflow_id)

    log.info("Running task")

    # Task options.
    task_id = message.message_id
    task_name = task["name"]
    task_author = task["metadata"]["author"]

    # Docker options.
    image = task["image"]
    parameters = task.get("parameters")

    # Files.
    inputs = task.get("inputs", [])
    outputs = task.get("outputs", [])

    time.sleep(5)

    def check_upstream(workflow_id: str, task_input_names: set) -> bool:
        statuses = []

        all_tasks_in_db = TaskManager().find(parent=workflow_id)
        if not all_tasks_in_db:
            raise ValueError(f"Tasks for workflow `{workflow_id}` not found")

        for task_in_db in all_tasks_in_db:
            if task_in_db.name in task_input_names:
                statuses.append(task_in_db.status)

        log.info("Upstream tasks statuses", statuses=statuses)

        if any([s == Status.STATUS_FAILED for s in statuses]):
            raise ValueError("Upstream task failed")

        return all([s == Status.STATUS_DONE for s in statuses])

    # For now, we are using the input file names to determine the upstream tasks
    # i.e. if the input file is `task1/file.txt`, then `task1` is an upstream task.
    task_input_names = {i["path"].split("/")[0] for i in inputs}

    log.info("Checking upstream tasks", task_input_names=task_input_names)

    if task_input_names:
        if not check_upstream(workflow_id, task_input_names):
            log.info("Upstream tasks are pending, re-enqueuing task")
            dramatiq.get_broker().enqueue(message)
            return

    # Otherwise, we can proceed with the execution.

    temp_dir = settings.data_dir

    local_dir = Path(temp_dir, task_author, workflow_id, task_name)
    (local_dir / "inputs").mkdir(parents=True, exist_ok=True)
    (local_dir / "outputs").mkdir(parents=True, exist_ok=True)

    # Get inputs from S3.
    s3_client = Minio(
        settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_use_ssl,
    )

    if not s3_client.bucket_exists(bucket_name=settings.minio_bucket):
        try:
            s3_client.make_bucket(bucket_name=settings.minio_bucket)
        except minio.S3Error as e:
            log.error("Unexpected error when creating bucket", error=e)
            raise

    # Download inputs from S3.
    for input_file in inputs:
        # `input_file["path"]` is the path of the input file relative to the workflow directory.
        object_name = str(Path(task_author, workflow_id, input_file["path"]))
        log.debug("Downloading input file from S3", bucket_name=settings.minio_bucket, object_name=object_name)
        s3_client.fget_object(
            bucket_name=settings.minio_bucket,
            object_name=object_name,
            file_path=str(local_dir / "inputs" / input_file["name"]),
        )

    log.debug("Running task", task_name=task_name)

    try:
        set_running(message)
        result: str = run_container(image=image, parameters=parameters, local_dir=str(local_dir))
    except Exception as e:
        log.error("Unexpected exception was raised by actor", error=e)
        raise

    # Upload outputs to S3.
    for output_file in outputs:
        object_name = str(Path(task_author, workflow_id, task_name, "outputs", output_file["name"]))
        log.debug("Uploading output file to S3", bucket_name=settings.minio_bucket, object_name=object_name)
        s3_client.fput_object(
            bucket_name=settings.minio_bucket,
            object_name=object_name,
            file_path=str(local_dir / "outputs" / output_file["name"]),
        )

    set_success(task_id, result)

    log.info("Task finished", result=result)

    if local_dir.exists() and task["options"]["on_finish_remove_local_dir"]:
        log.info("Deleting local directory", local_dir=local_dir)
        shutil.rmtree(local_dir)


def set_workflow_run_state(workflow_id: str):
    """
    Set workflow state based on task statuses.
    """
    workflow_in_db = WorkflowManager().find_one(id=workflow_id)
    if not workflow_in_db:
        log.error("Workflow not found. This should not happen.", workflow_id=workflow_id)
        raise ValueError(f"Workflow `{workflow_id}` not found")

    tasks = TaskManager().find(parent=workflow_id)

    task_status_only = []
    for task in tasks:
        status = task.status.upper()
        task_status_only.append(status)

    def check(comp: Callable, stats: list) -> bool:
        return comp([s in stats for s in task_status_only])

    if workflow_in_db.is_revoked:
        workflow_status = WorkflowStatus.STATUS_REVOKED
    elif check(all, [Status.STATUS_DONE]):
        workflow_status = WorkflowStatus.STATUS_DONE
    elif check(any, [Status.STATUS_FAILED]):
        workflow_status = WorkflowStatus.STATUS_FAILED
    elif check(all, [Status.STATUS_PENDING]):
        workflow_status = WorkflowStatus.STATUS_PENDING
    elif check(any, [Status.STATUS_PENDING]) and not check(any, [Status.STATUS_FAILED]):
        workflow_status = WorkflowStatus.STATUS_PENDING
    elif check(any, [Status.STATUS_RUNNING]) and not check(any, [Status.STATUS_FAILED]):
        workflow_status = WorkflowStatus.STATUS_RUNNING
    else:
        workflow_status = WorkflowStatus.STATUS_UNKNOWN

    WorkflowManager().create_or_update_from_id(
        workflow_id=workflow_id, updated_at=datetime.now(), status=workflow_status
    )


def set_running(message: MessageProxy):
    task_id = message.message_id
    TaskManager().create_or_update_from_id(
        message.message_id,
        status=Status.STATUS_RUNNING,
        updated_at=datetime.now(),
    )
    task_in_db = TaskManager().find_one(id=task_id)
    set_workflow_run_state(workflow_id=task_in_db.parent)


def set_success(message_id: str, result_data: str):
    task_id = message_id
    task_result = Result(log=result_data)
    TaskManager().create_or_update_from_id(
        message_id,
        status=Status.STATUS_DONE,
        updated_at=datetime.now(),
        result=task_result.dict(),
    )
    task_in_db = TaskManager().find_one(id=task_id)
    set_workflow_run_state(workflow_id=task_in_db.parent)


@dramatiq.actor(queue_name=settings.default_actor_opts.queue_name)
def set_failure(message: dict, exception_data):
    """
    Actor failure callback. Set task status to `FAILED` and append traceback.
    """
    task_id = message["message_id"]
    task_result = Result(message=exception_data)
    TaskManager().create_or_update_from_id(
        task_id,
        status=Status.STATUS_FAILED,
        updated_at=datetime.now(),
        result=task_result.dict(),
    )
    task_in_db = TaskManager().find_one(id=task_id)
    set_workflow_run_state(workflow_id=task_in_db.parent)
