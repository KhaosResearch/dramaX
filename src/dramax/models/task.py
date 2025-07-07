from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, validator

from dramax.models.executor import APIExecutor, DockerExecutor


class Status(str, Enum):
    STATUS_PENDING: str = "pending"
    STATUS_RUNNING: str = "running"
    STATUS_FAILED: str = "failure"
    STATUS_DONE: str = "success"


class Result(BaseModel):
    message: Any | None = None
    log: str = None


class Options(BaseModel):
    on_fail_force_interruption: bool = True
    on_fail_remove_local_dir: bool = True
    on_finish_remove_local_dir: bool = False
    queue_name: str | None = None


class Parameter(BaseModel):
    name: str
    value: Any


class File(BaseModel):
    name: str | None = None
    source: str | None = None
    sourcePath: str | None = None  # noqa: N815 - Already defined in previous versions
    path: str


class Task(BaseModel):
    id: str
    name: str
    executor: DockerExecutor | APIExecutor = Field(..., discriminator="type")
    inputs: list[File] = []
    outputs: list[File] = []
    options: Options = Options()
    metadata: dict = {}
    depends_on: list[str] = []

    def run(self) -> None:
        # Esta línea llamará a la implementación concreta según el tipo real del executor
        self.executor.execute(self)

    @validator("name")
    def name_validations(cls, name: str):  # noqa: ANN201, N805
        if " " in name:
            msg = "name must not contain spaces"
            raise ValueError(msg)
        if "." in name:
            msg = "name must not contain dots"
            raise ValueError(msg)
        return name


class TaskInDatabase(Task):
    """
    Represents a task in the database.
    """

    parent: str  # workflow id
    created_at: datetime | None = None
    updated_at: datetime | None = None
    result: Result | None = None
    status: Status = Status.STATUS_PENDING

    class Config:
        use_enum_values = True
