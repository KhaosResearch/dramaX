from datetime import datetime
from enum import Enum
from typing import Any, List, Optional

from pydantic import BaseModel, validator


class Status(str, Enum):
    STATUS_UNKNOWN: str = "UNKNOWN"
    STATUS_PENDING: str = "PENDING"
    STATUS_RUNNING: str = "RUNNING"
    STATUS_FAILED: str = "FAILED"
    STATUS_DONE: str = "DONE"


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
    default_value: Any = None
    value: Any


class Task(BaseModel):
    """
    Represents a task request.
    """

    name: str
    label: str = ""
    image: str
    parameters: List[Parameter] = []
    inputs: list = []
    outputs: list = []
    options: Options = Options()
    metadata: dict = {}

    @validator("name")
    def name_does_not_contain_spaces(cls, v):
        assert " " not in v, "name must not contain spaces"
        return v

    @validator("name")
    def name_does_not_contain_dots(cls, v):
        assert "." not in v, "name must not contain dots"
        return v

    @validator("inputs")
    def input_values_does_not_form_valid_identifier(cls, v):
        assert all(["/" in v["path"] for v in v]), "inputs values must form valid identifier: taskName/rest/of/path"
        return v


class TaskInDatabase(Task):
    """
    Represents a task in the database.
    """

    id: str
    name: str
    parent: str
    image: str
    parameters: list = []
    inputs: list = []
    outputs: list = []
    options: Options = Options()
    metadata: dict = {}
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    result: Optional[Result] = None
    status: Status = Status.STATUS_UNKNOWN

    class Config:
        use_enum_values = True
