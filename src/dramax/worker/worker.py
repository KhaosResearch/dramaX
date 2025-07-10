import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import dramatiq
from dramatiq import MessageProxy
from dramatiq.middleware import CurrentMessage
from structlog import get_logger

from dramax.exceptions import (
    TaskDeferredError,
    TaskFailedError,
)
from dramax.manager import TaskManager, WorkflowManager
from dramax.models.dramatiq.task import Result, Status, Task
from dramax.models.dramatiq.workflow import WorkflowStatus
from dramax.models.executor.api import APIExecutor
from dramax.models.executor.docker import DockerExecutor
from dramax.settings import settings
from dramax.worker.executor_service import run_task
from dramax.worker.setup_worker import setup_worker

broker, minio_client = setup_worker()

log = get_logger()


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
    try:
        TaskManager().check_upstream(parsed_task, workflow_id, message, broker)
    except TaskDeferredError:
        log.info("Task deferred due to upstream dependency not finished")
        return
    except TaskFailedError as e:
        log.exception("Task cannot proceed due to upstream failure", error=str(e))
        raise

    if isinstance(parsed_task.executor, DockerExecutor):  # * Checked Docker Instance
        # Create local directory in which to store input and output files.
        # This directory is mounted inside the container.
        log.info("Docker task")

        Path(parsed_task.workdir).mkdir(parents=True, exist_ok=True)

        parsed_task.executor.binding_dir = parsed_task.workdir
        parsed_task.executor.command = parsed_task.parameters

        log.debug("Created local directory", task_dir=parsed_task.workdir)

        try:
            set_running(parsed_task.id, workflow_id)
            result = run_task(parsed_task)
            log.info("Result", result=result)
        except Exception as e:
            log.exception("Unexpected exception was raised by actor", error=e)
            raise

    if isinstance(parsed_task.executor, APIExecutor):
        pass

    set_success(parsed_task.id, workflow_id, result)

    log.info("Task finished successfully")

    try:
        parsed_task.cleanup_workdir()
    except Exception as e:
        log.exception("Failed to clean up working directory", error=e)
        raise


def set_workflow_run_state(workflow_id: str) -> None:
    """
    Set workflow state based on task statuses.
    """
    workflow_in_db = WorkflowManager().find_one(id=workflow_id)
    if not workflow_in_db:
        log.error(
            "Workflow not found. This should not happen.",
            workflow_id=workflow_id,
        )
        msg = f"Workflow `{workflow_id}` not found"
        raise ValueError(msg)

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
        updated_at=datetime.now(tz=settings.timezone),
        status=workflow_status,
    )


def set_running(task_id: str, workflow_id: str) -> None:
    TaskManager().create_or_update_from_id(
        task_id,
        workflow_id,
        updated_at=datetime.now(tz=settings.timezone),
        status=Status.STATUS_RUNNING,
    )
    set_workflow_run_state(workflow_id=workflow_id)


def set_success(task_id: str, workflow_id: str, result_data: str) -> None:
    task_result = Result(log=result_data)
    TaskManager().create_or_update_from_id(
        task_id,
        workflow_id,
        updated_at=datetime.now(tz=settings.timezone),
        result=task_result.dict(),
        status=Status.STATUS_DONE,
    )
    set_workflow_run_state(workflow_id=workflow_id)


@dramatiq.actor(queue_name=settings.default_actor_opts.queue_name)
def set_failure(message: MessageProxy, exception_data: str) -> None:
    log.error(message["options"]["traceback"])
    actor_opts = message["options"]["options"]
    workflow_id = actor_opts["workflow_id"]
    task_result = Result(message=exception_data)
    TaskManager().create_or_update_from_id(
        actor_opts["task_id"],
        workflow_id,
        updated_at=datetime.now(tz=settings.timezone),
        result=task_result.dict(),
        status=Status.STATUS_FAILED,
    )
    set_workflow_run_state(workflow_id=workflow_id)
