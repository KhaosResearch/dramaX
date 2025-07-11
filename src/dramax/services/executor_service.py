from pathlib import Path

from structlog import get_logger

from dramax.common.exceptions import FileNotFoundForUploadError, UploadError
from dramax.models.dramatiq.task import Task
from dramax.worker.utils import set_running


def run_docker_task(task: Task) -> str:
    # Create local directory in which to store input and output files.
    # This directory is mounted inside the container.
    log = get_logger()
    log.info("Docker task")

    Path(task.workdir).mkdir(parents=True, exist_ok=True)

    task.executor.binding_dir = task.workdir
    task.executor.command = task.parameters

    log.debug("Created local directory", task_dir=task.workdir)

    try:
        set_running(task.id, task.workflow_id)
        result = _common_run_task(task)
        log.info("Result", result=result)
    except Exception as e:
        log.exception("Unexpected exception was raised by actor", error=e)
        raise
    return result


def _common_run_task(task: Task) -> str:
    """Execute a task using the task provided executor.

    Parameters
    ----------
    - task: A Task instance containing all the necessary data for execution.

    Returns
    -------
    - A string containing the logs generated during the task execution.

    """
    log = get_logger()

    # First inputs are downloaded
    try:
        task.download_inputs()
    except Exception as e:
        log.exception("Input(s) download failed", error=e)
        raise

    # We execute the assigned task
    try:
        result = task.executor.execute()
    except Exception as e:
        log.exception("Unexpected exception was raised by executor", error=e)
        raise
    # Outputs and logs are uploaded
    try:
        task.upload_outputs()
        task.create_upload_logs(result)
    except FileNotFoundForUploadError as e:
        log.exception(
            "Output file not found in task folder",
            file_path=e.file_path,
        )
        raise
    except UploadError as e:
        log.exception(
            "Failed to upload output or log to MinIO",
            object_path=e.object_path,
            file_path=e.file_path,
            original_error=str(e.original_exception),
        )
        raise
    except Exception as e:
        log.exception("Unexpected error during output/logs upload", error=str(e))
        raise

    return result
