from structlog import get_logger

from dramax.common.exceptions import FileNotFoundForUploadError, UploadError
from dramax.models.dramatiq.task import Task


def run_task(task: Task) -> str:
    """
    Executes a task using the task provided executor.
    Parameters:
    - task: A Task instance containing all the necessary data for execution.

    Returns:
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
