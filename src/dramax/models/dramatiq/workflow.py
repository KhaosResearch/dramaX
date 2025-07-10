import uuid
from datetime import datetime
from enum import Enum

from pydantic import BaseModel, validator
from pydantic.fields import Field

from dramax.models.dramatiq.task import Task, TaskInDatabase


class WorkflowStatus(str, Enum):
    STATUS_REVOKED: str = "revoked"
    STATUS_PENDING: str = "pending"
    STATUS_RUNNING: str = "running"
    STATUS_FAILED: str = "failure"
    STATUS_DONE: str = "success"


class WorkflowMetadata(BaseModel):
    author: str = "anonymous"

    class Config:
        extra = "allow"


class Workflow(BaseModel):
    id: str = Field(default_factory=lambda: "workflow-" + uuid.uuid4().hex[:8])
    label: str = ""
    tasks: list[Task] = []
    metadata: WorkflowMetadata = WorkflowMetadata()

    @validator("tasks")
    def task_ids_not_duplicated(cls, tasks: list[Task]) -> list[Task]:
        ids = [task.id for task in tasks]
        if len(set(ids)) != len(ids):
            msg = "Found duplicated IDs in workflow"
            raise ValueError(msg)
        return tasks


class WorkflowInDatabase(BaseModel):
    tasks: list[TaskInDatabase] = []
    created_at: datetime | None = None
    updated_at: datetime | None = None
    status: WorkflowStatus = WorkflowStatus.STATUS_PENDING
    is_revoked: bool = False

    class Config:
        use_enum_values = True


class ExecutionId(BaseModel):
    id: str
