class TaskError(Exception):
    """Base class for all custom exceptions in the Dramax system."""


class MinioError(Exception):
    """Base class for all custom exceptions in Minio."""


class FileError(Exception):
    """Base class for all custom exceptions in filesystem."""


class TaskDeferredError(TaskError):
    """Raised when a task cannot yet be executed due to pending upstream dependencies."""

    def __init__(self, task_id: str, depends_on: list[str]) -> None:
        self.task_id = task_id
        self.depends_on = depends_on
        super().__init__(f"Task '{task_id}' deferred. Waiting for: {depends_on}")


class TaskFailedError(TaskError):
    """Raised when an upstream task has failed and current task should not execute."""

    def __init__(self, task_id: str, failed_dependency: str) -> None:
        self.task_id = task_id
        self.failed_dependency = failed_dependency
        super().__init__(
            f"Task '{task_id}' failed due to upstream task '{failed_dependency}'.",
        )


class FileNotFoundForUploadError(FileError):
    """
    Exception raised when a file intended for upload to an external storage
    (e.g., MinIO) is not found on the local filesystem.

    Attributes:
        file_path (str): The path of the file that was expected but not found.
    """

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path
        super().__init__(f"File not found for upload: {file_path}")


class DockerExecutionError(Exception):
    """
    Exception raised when a Docker container execution fails.

    Attributes:
        message (str): Human-readable error message.
        status_code (int | None): Optional status code returned by the container.
    """

    def __init__(self, message: str, status_code: int | None = None) -> None:
        self.status_code: int | None = status_code
        super().__init__(message)


class InputDownloadError(MinioError):
    """
    Custom exception raised when an input file fails to download from the object storage (e.g., MinIO).

    Attributes:
        object_name (str): The name of the object that was attempted to be downloaded.
        file_path (str): The local path where the file was supposed to be saved.
        original_exception (Exception): The original exception raised by the storage client.
    """

    def __init__(
        self,
        object_name: str,
        file_path: str,
        original_exception: Exception,
    ) -> None:
        self.object_name = object_name
        self.file_path = file_path
        self.original_exception = original_exception
        super().__init__(
            f"Failed to download input '{self.object_name}' to '{self.file_path}': {original_exception}",
        )


class UploadError(MinioError):
    """
    Raised when an upload operation to object storage (e.g., MinIO) fails.

    Attributes:
        object_name (str): Path in the object store where the file was supposed to be uploaded.
        file_path (str): Local file path that was being uploaded.
        original_exception (Exception): The original exception that triggered the failure.
    """

    def __init__(
        self,
        object_name: str,
        file_path: str,
        original_exception: Exception,
    ) -> None:
        self.object_name = object_name
        self.file_path = file_path
        self.original_exception = original_exception
        message = f"Failed to upload file '{file_path}' to object storage path '{object_name}': {original_exception}"
        super().__init__(message)
