import uuid
from datetime import datetime
from enum import Enum
from typing import Any, List, Optional

from pydantic import BaseModel, validator
from pydantic.fields import Field


class Status(str, Enum):
    STATUS_PENDING: str = "pending"
    STATUS_RUNNING: str = "running"
    STATUS_FAILED: str = "failure"
    STATUS_DONE: str = "success"


class Result(BaseModel):
    message: Optional[Any] = None
    log: str = None


class Options(BaseModel):
    on_fail_force_interruption: bool = True
    on_fail_remove_local_dir: bool = True
    on_finish_remove_local_dir: bool = False
    queue_name: Optional[str] = None


class Parameter(BaseModel):
    name: str
    value: Any


class File(BaseModel):
    name: Optional[str] = None
    source: Optional[str] = None
    sourcePath: Optional[str] = None
    path: str


class Task(BaseModel):
    """
    Represents a task request.
    """

    id: str
    name: str
    label: str = "latest"
    image: str
    parameters: List[Parameter] = []
    inputs: List[File] = []
    outputs: List[File] = []
    options: Options = Options()
    metadata: dict = {}
    depends_on: List[str] = []

    @validator("name")
    def name_does_not_contain_spaces(cls, v):
        assert " " not in v, "name must not contain spaces"
        return v

    @validator("name")
    def name_does_not_contain_dots(cls, v):
        assert "." not in v, "name must not contain dots"
        return v


class TaskInDatabase(Task):
    """
    Represents a task in the database.
    """

    parent: str  # workflow id
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    result: Optional[Result] = None
    status: Status = Status.STATUS_PENDING

    class Config:
        use_enum_values = True
