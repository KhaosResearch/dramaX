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

retries = Retries(max_retries=0)
broker.add_middleware(retries)

dramatiq.set_broker(broker)

log.info("Connected to queue", queue_name=settings.default_actor_opts.queue_name)

s3_client = Minio(
    settings.minio_endpoint,
    access_key=settings.minio_access_key,
    secret_key=settings.minio_secret_key,
    secure=settings.minio_use_ssl,
)

log.info("Worker is ready")


@dramatiq.actor(**settings.default_actor_opts.dict())
def worker(task: dict, workflow_id: str):
    message = CurrentMessage.get_current_message()

    task_id = task["id"]
    task_author = task["metadata"]["author"]
    container_image = task["image"]
    container_parameters = task.get("parameters")
    container_environment = task.get("environment")
    inputs = task.get("inputs", [])
    outputs = task.get("outputs", [])

    log = get_logger()
    log = log.bind(
        message_id=message.message_id, task_id=task_id, workflow_id=workflow_id
    )

    log.info("Running task", task=task)

    time.sleep(1)

    # Check if upstream tasks have failed before running this task.
    depends_on = task["depends_on"]
    if depends_on:
        log.debug("Checking upstream tasks")
        tasks_in_db = TaskManager().find(parent=workflow_id)
        for task_in_db in tasks_in_db:
            if task_in_db.id in depends_on:
                if task_in_db.status == Status.STATUS_FAILED:
                    raise ValueError("Upstream task failed")
                elif (
                    task_in_db.status == Status.STATUS_PENDING
                    or task_in_db.status == Status.STATUS_RUNNING
                ):
                    # We abruptly stop the current task execution and enqueue it again.
                    log.debug(
                        "Re-enqueueing task because upstream task is not done yet",
                        depends_on=task_id,
                    )
                    dramatiq.get_broker().enqueue(message)
                    return
                else:
                    log.debug("Upstream task is done", upstream_task_id=task_in_db.id)

    # Create local directory in which to store input and output files.
    # This directory is mounted inside the container.
    task_dir = Path(settings.data_dir, task_author, workflow_id, task_id)
    task_dir.mkdir(parents=True, exist_ok=True)

    log.debug("Created local directory", task_dir=task_dir)

    if not s3_client.bucket_exists(bucket_name=settings.minio_bucket):
        try:
            s3_client.make_bucket(bucket_name=settings.minio_bucket)
        except minio.S3Error as e:
            log.error("Unexpected error when creating bucket", error=e)
            raise

    for artifact in inputs:
        object_name = (
            str(Path(task_author, workflow_id, artifact["source"]))
            + artifact["sourcePath"]
        )
        file_path = str(task_dir) + artifact["path"]

        log.debug(
            "Downloading input file", object_name=object_name, file_path=file_path
        )

        s3_client.fget_object(
            bucket_name=settings.minio_bucket,
            object_name=object_name,
            file_path=file_path,
        )

    log.debug(
        "Running container", image=container_image, parameters=container_parameters
    )

    try:
        set_running(task_id, workflow_id)
        result = run_container(
            image=container_image,
            parameters=container_parameters,
            environment=container_environment,
            local_dir=str(task_dir),
        )
        log.info("Result", result=result)
    except Exception as e:
        log.error("Unexpected exception was raised by actor", error=e)
        raise

    # Create and upload log file.
    now = datetime.now()
    dt_string = now.strftime("%d-%m-%Y-%H:%M:%S")
    log_file_name = dt_string + "-log.txt"
    file_path = str(task_dir / log_file_name)
    with open(file_path, "w") as f:
        f.write(result or "There were no logs produced for this task.")

    # Upload outputs to S3.
    object_name = str(Path(task_author, workflow_id, task_id, log_file_name))
    s3_client.fput_object(
        bucket_name=settings.minio_bucket,
        object_name=object_name,
        file_path=file_path,
    )

    for artifact in outputs:
        object_name = str(Path(task_author, workflow_id, task_id)) + artifact["path"]
        file_path = str(task_dir) + artifact["path"]

        if not Path(file_path).exists():
            log.warning("Output file not found in task folder", file_path=file_path)
            continue

        log.debug("Uploading output file", object_name=object_name, file_path=file_path)

        s3_client.fput_object(
            bucket_name=settings.minio_bucket,
            object_name=object_name,
            file_path=file_path,
        )

    set_success(task_id, workflow_id, result)

    log.info("Task finished successfully")

    if task_dir.exists() and task["options"]["on_finish_remove_local_dir"]:
        log.info("Deleting local directory", local_dir=task_dir)
        shutil.rmtree(task_dir)


def set_workflow_run_state(workflow_id: str):
    """
    Set workflow state based on task statuses.
    """
    workflow_in_db = WorkflowManager().find_one(id=workflow_id)
    if not workflow_in_db:
        log.error(
            "Workflow not found. This should not happen.", workflow_id=workflow_id
        )
        raise ValueError(f"Workflow `{workflow_id}` not found")

    tasks = TaskManager().find(parent=workflow_id)

    task_status_only = []
    for task in tasks:
        status = task.status
        task_status_only.append(status)

    def check(comp: Callable, stats: list) -> bool:
        return comp([s in stats for s in task_status_only])

    if workflow_in_db.is_revoked:
        workflow_status = WorkflowStatus.STATUS_REVOKED
    elif check(all, [Status.STATUS_DONE]):
        workflow_status = WorkflowStatus.STATUS_DONE
    elif check(all, [Status.STATUS_PENDING]):
        workflow_status = WorkflowStatus.STATUS_PENDING
    elif check(any, [Status.STATUS_FAILED]):
        workflow_status = WorkflowStatus.STATUS_FAILED
    elif check(any, [Status.STATUS_PENDING]) and not check(any, [Status.STATUS_FAILED]):
        workflow_status = WorkflowStatus.STATUS_PENDING
    elif check(any, [Status.STATUS_RUNNING]) and not check(any, [Status.STATUS_FAILED]):
        workflow_status = WorkflowStatus.STATUS_RUNNING
    else:
        workflow_status = WorkflowStatus.STATUS_PENDING

    WorkflowManager().create_or_update_from_id(
        workflow_id=workflow_id, updated_at=datetime.now(), status=workflow_status
    )


def set_running(task_id: str, workflow_id: str):
    TaskManager().create_or_update_from_id(
        task_id,
        workflow_id,
        updated_at=datetime.now(),
        status=Status.STATUS_RUNNING,
    )
    set_workflow_run_state(workflow_id=workflow_id)


def set_success(task_id: str, workflow_id: str, result_data: str):
    task_result = Result(log=result_data)
    TaskManager().create_or_update_from_id(
        task_id,
        workflow_id,
        updated_at=datetime.now(),
        result=task_result.dict(),
        status=Status.STATUS_DONE,
    )
    set_workflow_run_state(workflow_id=workflow_id)


@dramatiq.actor(queue_name=settings.default_actor_opts.queue_name)
def set_failure(message: MessageProxy, exception_data: str):
    actor_opts = message["options"]["options"]
    workflow_id = actor_opts["workflow_id"]
    task_result = Result(message=exception_data)
    TaskManager().create_or_update_from_id(
        actor_opts["task_id"],
        workflow_id,
        updated_at=datetime.now(),
        result=task_result.dict(),
        status=Status.STATUS_FAILED,
    )
    set_workflow_run_state(workflow_id=workflow_id)
