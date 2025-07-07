import shutil
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import dramatiq
from dramatiq import MessageProxy
from dramatiq.middleware import CurrentMessage
from structlog import get_logger

from dramax.manager import TaskManager, WorkflowManager
from dramax.models.executor.docker import DockerExecutor
from dramax.models.task import Result, Status, Task
from dramax.models.workflow import WorkflowStatus
from dramax.settings import settings
from dramax.worker.setup_worker import _setup_worker

log, broker, minio_client = _setup_worker()


@dramatiq.actor(**settings.default_actor_opts.dict())
def worker(task: dict, workflow_id: str) -> None:
    message = CurrentMessage.get_current_message()

    # Conversion to Task to work easier
    parsed_task = Task(**task)

    log = get_logger()
    log = log.bind(
        message_id=message.message_id,
        task_id=parsed_task.id,
        workflow_id=workflow_id,
    )

    log.info("Running task", task=parsed_task)  # ? Not sure about this  bef: .dict

    time.sleep(1)

    # Check if upstream tasks have failed before running this task.
    TaskManager().check_upstream(
        parsed_task,
        workflow_id,
        message,
        log,
    )  # * Moved to TaskManager Class

    if isinstance(parsed_task.executor, DockerExecutor):  # * Checked Docker Instance
        # Create local directory in which to store input and output files.
        # This directory is mounted inside the container.
        # TODO Check if it is necessary to move more things out
        task_dir = Path(
            settings.data_dir,
            parsed_task.metadata.author,
            workflow_id,
            parsed_task.id,
        )  # ? Actualizar metadata en pydantic
        task_dir.mkdir(parents=True, exist_ok=True)

        log.debug("Created local directory", task_dir=task_dir)

        for artifact in parsed_task.inputs:
            object_name = (
                str(Path(parsed_task.metadata.author, workflow_id, artifact["source"]))
                + artifact["sourcePath"]
            )
            file_path = str(task_dir) + artifact["path"]

            minio_client.get_object(
                object_name=object_name,
                file_path=file_path,
            )

        log.debug(
            "Running container",
            image=parsed_task.executor.image,
            parameters=parsed_task.parameters,
        )

        try:
            set_running(parsed_task.id, workflow_id)
            result = parsed_task.run(  # * NEW EXECUTE METHOD DONE
                image=parsed_task.executor.image,
                parameters=parsed_task.parameters,
                environment=parsed_task.executor.environment,
                local_dir=str(task_dir),
            )
            log.info("Result", result=result)
        except Exception as e:
            log.exception("Unexpected exception was raised by actor", error=e)
            raise

        # Create and upload log file.
        now = datetime.now(tz=settings.timezone)
        dt_string = now.strftime("%d-%m-%Y-%H:%M:%S")
        log_file_name = dt_string + "-log.txt"
        file_path = str(task_dir / log_file_name)
        with Path.open(file_path, "w") as f:
            f.write(result or "There were no logs produced for this task.")

        # Upload outputs to S3.
        object_name = str(
            Path(
                parsed_task.metadata.author,
                workflow_id,
                parsed_task.id,
                log_file_name,
            ),
        )
        minio_client.upload_object(
            object_name=object_name,
            file_path=file_path,
        )

        for artifact in parsed_task.outputs:
            object_name = (
                str(Path(parsed_task.metadata.author, workflow_id, parsed_task.id))
                + artifact["path"]
            )
            file_path = str(task_dir) + artifact["path"]

            if not Path(file_path).exists():
                log.warning("Output file not found in task folder", file_path=file_path)
                continue

            log.debug(
                "Uploading output file",
                object_name=object_name,
                file_path=file_path,
            )

            minio_client.upload_object(
                object_name=object_name,
                file_path=file_path,
            )

        set_success(parsed_task.id, workflow_id, result)

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
            "Workflow not found. This should not happen.",
            workflow_id=workflow_id,
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
        workflow_id=workflow_id,
        updated_at=datetime.now(),
        status=workflow_status,
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
    print("HA FALLADO TU TAREA", message["options"]["traceback"])
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
