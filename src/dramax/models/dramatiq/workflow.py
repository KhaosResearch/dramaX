import uuid
from datetime import datetime
from enum import Enum
from typing import List, Optional

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
    tasks: List[Task] = []
    metadata: WorkflowMetadata = WorkflowMetadata()

    @validator("tasks")
    def task_ids_not_duplicated(cls, v):
        ids = [v.id for v in v]
        assert len(set(ids)) == len(ids), "Found duplicated IDs in workflow"
        return v


class WorkflowInDatabase(BaseModel):
    tasks: List[TaskInDatabase] = []
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    status: WorkflowStatus = WorkflowStatus.STATUS_PENDING
    is_revoked: bool = False

    class Config:
        use_enum_values = True


class ExecutionId(BaseModel):
    id: str
