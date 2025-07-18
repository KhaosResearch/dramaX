from structlog import get_logger

from dramax.common.exceptions import FileNotFoundForUploadError, UploadError
from dramax.models.dramatiq.task import Task
from dramax.models.executor.api import api_execute
from dramax.models.executor.docker import docker_execute


def execute_task(task: Task, workdir: str) -> str:
    """Execute a task using the task provided executor.

    Parameters
    ----------
    - task: A Task instance containing all the necessary data for execution.

    Returns
    -------
    - A string containing the logs generated during the task execution.

    """
    log = get_logger()

    try:
        if len(task.inputs) > 0:
            task.download_inputs(workdir)

    except Exception as e:  #! Falta la excepcion de las carpetas
        log.exception("Input(s) download failed", error=e)
        raise

    try:
        if hasattr(task, "image") and task.image:
            log.info("Docker task")
            result = docker_execute(task, workdir)
        elif hasattr(task, "url") and task.url:
            log.info("API task")
            result = api_execute(task, workdir)
    except Exception as e:
        log.exception("Unexpected exception was raised by executor", error=e)
        raise
    try:
        task.upload_outputs(workdir)
        task.create_upload_logs(result, workdir)

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
