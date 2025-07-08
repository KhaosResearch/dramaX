class DramaxException(Exception):  # noqa: N818
    """Base class for all custom exceptions in the Dramax system."""


class TaskDeferredException(DramaxException):
    """Raised when a task cannot yet be executed due to pending upstream dependencies."""

    def __init__(self, task_id: str, depends_on: list[str]) -> None:
        self.task_id = task_id
        self.depends_on = depends_on
        super().__init__(f"Task '{task_id}' deferred. Waiting for: {depends_on}")


class TaskFailedException(DramaxException):
    """Raised when an upstream task has failed and current task should not execute."""

    def __init__(self, task_id: str, failed_dependency: str) -> None:
        self.task_id = task_id
        self.failed_dependency = failed_dependency
        super().__init__(
            f"Task '{task_id}' failed due to upstream task '{failed_dependency}'.",
        )


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
