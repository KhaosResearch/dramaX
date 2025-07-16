import shutil
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from dramax.common.exceptions import (
    FileNotFoundForUploadError,
    FolderPreparationError,
    InputDownloadError,
    UploadError,
)
from dramax.common.settings import settings
from dramax.models.executor import APIExecutor, DockerExecutor
from dramax.services.minio import MinioService
from pydantic import BaseModel, Field, validator


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
    parameters: list[dict[str, str]] | None
    inputs: list[File] = []
    outputs: list[File] = []
    options: Options = Options()
    metadata: dict = {}
    depends_on: list[str] = []
    workflow_id: str = None  # ? ESTOS DOS CAMPOS TIENEN SENTIDO AQUI?
    workdir: str = None

    @validator("name")
    def name_validations(cls, name: str) -> str:
        if " " in name:
            msg = "name must not contain spaces"
            raise ValueError(msg)
        if "." in name:
            msg = "name must not contain dots"
            raise ValueError(msg)
        return name

    def download_inputs(self) -> None:
        for artifact in self.inputs:
            object_name = (
                f"{Path(self.metadata['author'], self.workflow_id, artifact.source)}"
                f"{artifact.sourcePath}"
            )
            file_path = f"{self.workdir}{artifact.path}"
            try:
                MinioService.get_instance().get_object(
                    object_name=object_name,
                    file_path=file_path,
                )  # Singleton service
            except Exception as e:
                raise InputDownloadError(object_name, file_path, e) from e

    def upload_outputs(self) -> None:
        for artifact in self.outputs:
            object_name = (
                f"{Path(self.metadata['author'], self.workflow_id, self.id)}"
                f"{artifact.path}"
            )
            file_path = f"{self.workdir}{artifact.path}"
            if not Path(file_path).exists():
                raise FileNotFoundForUploadError(file_path)

            try:
                MinioService.get_instance().upload_object(
                    object_path=object_name,
                    file_path=file_path,
                )  # Singleton service
            except Exception as e:
                raise UploadError(object_name, file_path, e) from e

    def create_upload_logs(self, result: str) -> None:
        log_file_name = (
            datetime.now(tz=settings.timezone).strftime("%d-%m-%Y-%H:%M:%S")
            + "-log.txt"
        )
        file_path = f"{Path(self.workdir, log_file_name)}"
        with Path.open(file_path, "w") as f:
            f.write(result)
        object_name = (
            f"{Path(self.metadata['author'], self.workflow_id, self.id)}{log_file_name}"
        )
        try:
            MinioService.get_instance().upload_object(
                object_path=object_name,
                file_path=file_path,
            )  # Singleton service
        except Exception as e:
            raise UploadError(object_name, file_path, e) from e

    def cleanup_workdir(self) -> None:
        """Delete the task's working directory if configured to do so."""
        if Path(self.workdir).exists() and self.options.on_finish_remove_local_dir:
            shutil.rmtree(Path(self.workdir))

    def prepare_input_paths(self) -> None:
        try:
            relative_path = self.inputs[0].path.lstrip("/")
            relative_input_path = Path(self.inputs[0].source) / relative_path
            task_input_path = (
                Path(self.workdir).parent / relative_input_path
            )  # need past task to retrieve file
            task_input_path.parent.mkdir(parents=True, exist_ok=True)

            self.executor.input_dir = task_input_path  # Download minio file here
        except Exception as e:
            msg = (f"Failed to prepare input directories for {self.id}",)
            raise FolderPreparationError(msg, e) from e

    def prepare_output_paths(self) -> None:
        try:
            relative_output_path = self.outputs[0].path.lstrip("/")
            task_output_path = Path(self.workdir) / relative_output_path
            task_output_path.parent.mkdir(parents=True, exist_ok=True)

            self.executor.output_dir = task_output_path
        except Exception as e:
            msg = (f"Failed to prepare output directories for {self.id}",)
            raise FolderPreparationError(msg, e) from e


class TaskInDatabase(Task):
    """Represents a task in the database."""

    parent: str  # workflow id
    created_at: datetime | None = None
    updated_at: datetime | None = None
    result: Result | None = None
    status: Status = Status.STATUS_PENDING

    class Config:
        use_enum_values = True
