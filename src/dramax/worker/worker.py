import time

import dramatiq
from dramatiq.middleware import CurrentMessage
from structlog import get_logger

from dramax.common.exceptions import (
    TaskDeferredError,
    TaskExecutorError,
    TaskFailedError,
)
from dramax.common.settings import settings
from dramax.models.dramatiq.manager import TaskManager
from dramax.models.dramatiq.task import Task
from dramax.models.executor.api import APIExecutor
from dramax.models.executor.docker import DockerExecutor
from dramax.services.executor_service import run_docker_task
from dramax.worker.utils import set_success, setup_worker

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
        match parsed_task.executor:
            case DockerExecutor():
                result = run_docker_task(parsed_task)
            case APIExecutor():
                # Manejar APIExecutor
                pass
            case _:
                raise TaskExecutorError(  # noqa: TRY301
                    task_id=parsed_task.id,
                    workflow_id=parsed_task.workflow_id,
                    executor_type=type(parsed_task.executor).__name__,
                )
    except Exception as e:
        log.exception("Task not executed properly", error=str(e))
        raise

    set_success(parsed_task.id, workflow_id, result)

    log.info("Task finished successfully")

    try:
        parsed_task.cleanup_workdir()
    except Exception as e:
        log.exception("Failed to clean up working directory", error=e)
        raise
