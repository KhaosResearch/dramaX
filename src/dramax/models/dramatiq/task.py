import shutil
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, validator
from structlog import get_logger

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
    on_finish_remove_local_dir: bool = False #TODO Check production True
    queue_name: str | None = None


class Parameter(BaseModel):
    name: str
    value: Any


class UnpackedParams(BaseModel):
    method: str | None
    headers: dict[str, str] | None
    auth: tuple[str, str] | None
    body: dict[str, Any] | None
    timeout: int


class File(BaseModel):
    name: str | None = None
    source: str | None = None
    sourcePath: str | None = None  # noqa: N815 - Already defined in previous versions
    path: str

    @staticmethod
    def _ensure_relative(p: str) -> Path:
        return Path(p).relative_to(Path(p).anchor) if Path(p).is_absolute() else Path(p)

    @staticmethod
    def _skip_first_dir(path: str) -> Path:
        p = Path(path)
        return p.relative_to("/tmp")  # noqa: S108

    def get_full_path(self, base: str) -> str:
        """Construye el path absoluto local del archivo."""
        path = self.path
        if self.path.startswith("/"):
            path = self.path.lstrip("/")
        return str(Path(base, self._ensure_relative(path)))

    def get_object_name(self, base: str) -> str:
        """Construye el nombre del objeto remoto para MinIO."""
        base_path = self._skip_first_dir(base)
        if self.source and self.sourcePath:
            base_path = base_path.parent  # Pointing old task dir to retrieve input
            return str(
                base_path
                / self._ensure_relative(self.source)
                / self._ensure_relative(self.sourcePath)
            )
        return str(base_path / self._ensure_relative(self.path))


class Task(BaseModel):
    id: str
    name: str
    url: str | None
    image: str | None
    parameters: list[dict[str, str | dict | list[str | dict]]] | None
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
        log = get_logger()
        for artifact in self.inputs:
            object_name = artifact.get_object_name(workdir)
            file_path = artifact.get_full_path(workdir)
            log.info("Object_name", object_name=object_name)
            log.info("file_path", file_path=file_path)

            try:
                MinioService.get_instance().get_object(
                    object_name=object_name,
                    file_path=file_path,
                )
            except Exception as e:
                raise InputDownloadError(object_name, file_path, e) from e

    def upload_outputs(self, workdir: str) -> None:
        for artifact in self.outputs:
            object_name = artifact.get_object_name(workdir)
            file_path = artifact.get_full_path(workdir)
            if not Path(file_path).parent.exists():
                raise FileNotFoundForUploadError(file_path)

            try:
                MinioService.get_instance().upload_object(
                    object_path=object_name,
                    file_path=file_path,
                )
            except Exception as e:
                raise UploadError(object_name, workdir, e) from e

    def create_upload_logs(self, result: str, workdir: str) -> None:
        log = get_logger()
        log_file_name = (
            datetime.now(tz=settings.timezone).strftime("%d-%m-%Y-%H:%M:%S")
            + "-log.txt"
        )

        try:
            log_file_path = Path(workdir) / log_file_name
            log_file_path.parent.mkdir(parents=True, exist_ok=True)
            with log_file_path.open("w") as f:
                f.write(result)
        except PermissionError as e:
            log.info("Permission error: ", error=e)
            log.info("Attempting to write to path: ", path=log_file_path)
            raise

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
