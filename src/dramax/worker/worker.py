import time
from datetime import datetime
from pathlib import Path

import dramatiq
from dramatiq.middleware import CurrentMessage
from structlog import get_logger

from dramax.common.exceptions import (
    TaskDeferredError,
    TaskFailedError,
)
from dramax.common.settings import settings
from dramax.models.dramatiq.manager import TaskManager
from dramax.models.dramatiq.task import Result, Status, Task
from dramax.services.executor_service import execute_task
from dramax.worker.utils import set_success, set_workflow_run_state, setup_worker

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

    workdir = f"{Path(parsed_task.metadata['author'], workflow_id, parsed_task.id)}"

    log.info("Running task", task=parsed_task)

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

    try:
        log.info("Executing task")
        result = execute_task(parsed_task, workdir)

    except Exception as e:  # TODO : mejorar manejo de excepciones
        log.exception("Task not executed properly", error=str(e))
        raise

    set_success(parsed_task.id, workflow_id, result)

    log.info("Task finished successfully")

    try:
        parsed_task.cleanup_workdir(workdir)
    except Exception as e:
        log.exception("Failed to clean up working directory", error=e)
        raise


@dramatiq.actor(queue_name=settings.default_actor_opts.queue_name)
def set_failure(message: dramatiq.MessageProxy, exception_data: str) -> None:
    log = get_logger("dramax.worker")
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
