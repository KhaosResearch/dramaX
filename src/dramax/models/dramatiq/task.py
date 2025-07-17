import shutil
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, validator

from dramax.common.exceptions import (
    FileNotFoundForUploadError,
    InputDownloadError,
    UploadError,
)
from dramax.common.settings import settings
from dramax.services.minio import MinioService


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


class UnpackedParams(BaseModel):
    method: str | None
    headers: dict[str, str] | None
    auth: list[str] | None
    body: dict[str, Any]
    timeout: int = 10


class File(BaseModel):
    name: str | None = None
    source: str | None = None
    sourcePath: str | None = None  # noqa: N815 - Already defined in previous versions
    path: str
    base: str | None = None

    def get_full_path(self) -> str:
        """Construye el path absoluto local del archivo."""
        return str(Path(self.base) / self.path)

    def get_object_name(self) -> str:
        """Construye el nombre del objeto."""
        if self.source and self.sourcePath:
            return str(Path(self.base, self.source) / self.sourcePath)
        return str(Path(self.base) / self.path)


class Task(BaseModel):
    id: str
    name: str
    url: str | None
    image: str | None
    parameters: list[dict[str, str | dict]] | None
    environment: dict[str, Any] | None
    inputs: list[File] = []
    outputs: list[File] = []
    options: Options = Options()
    metadata: dict = {}
    depends_on: list[str] = []

    @validator("name")
    def name_validations(cls, name: str) -> str:
        if " " in name:
            msg = "name must not contain spaces"
            raise ValueError(msg)
        if "." in name:
            msg = "name must not contain dots"
            raise ValueError(msg)
        return name

    def download_inputs(self, workdir: str) -> None:
        for artifact in self.inputs:
            artifact.base = str(workdir)
            object_name = artifact.get_object_name()
            file_path = artifact.get_full_path()

            try:
                MinioService.get_instance().get_object(
                    object_name=object_name,
                    file_path=file_path,
                )
            except Exception as e:
                raise InputDownloadError(object_name, file_path, e) from e

    def upload_outputs(self, workdir: str) -> None:
        for artifact in self.outputs:
            artifact.base = workdir
            object_name = artifact.get_object_name()
            file_path = artifact.get_full_path()

            if not Path(file_path).exists():
                raise FileNotFoundForUploadError(file_path)

            try:
                MinioService.get_instance().upload_object(
                    object_path=object_name,
                    file_path=file_path,
                )
            except Exception as e:
                raise UploadError(object_name, workdir, e) from e

    def create_upload_logs(self, result: str, workdir: str) -> None:
        log_file_name = (
            datetime.now(tz=settings.timezone).strftime("%d-%m-%Y-%H:%M:%S")
            + "-log.txt"
        )
        log_file_path = Path(workdir) / log_file_name

        # Escribir el contenido en el archivo log
        with Path.open(log_file_path, "w") as f:
            f.write(result)

        try:
            MinioService.get_instance().upload_object(
                object_path=str(log_file_path),  # nombre del objeto remoto en MinIO
                file_path=str(log_file_path),  # ruta local del archivo
            )
        except Exception as e:
            raise UploadError(str(log_file_path), workdir, e) from e

    def cleanup_workdir(self, workdir: str) -> None:
        """Delete the task's working directory if configured to do so."""
        if Path(workdir).exists() and self.options.on_finish_remove_local_dir:
            shutil.rmtree(Path(workdir))


class TaskInDatabase(Task):
    """Represents a task in the database."""

    parent: str  # workflow id
    created_at: datetime | None = None
    updated_at: datetime | None = None
    result: Result | None = None
    status: Status = Status.STATUS_PENDING

    class Config:
        use_enum_values = True
