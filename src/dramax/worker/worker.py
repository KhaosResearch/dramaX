import time
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
from dramax.models.dramatiq.task import Task
from dramax.models.executor.api import APIExecutor
from dramax.models.executor.docker import DockerExecutor
from dramax.services.executor_service import run_task
from dramax.worker.utils import set_running, set_success, setup_worker

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
