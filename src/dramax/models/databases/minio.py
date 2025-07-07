from minio import Minio
from structlog import get_logger

from dramax.settings import settings

log = get_logger("dramax.minio")


class MinioService:
    _instance: Minio | None = None

    def __init__(self) -> None:
        self.client = Minio(
            endpoint=settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=False,
        )
        self.bucket = settings.minio_bucket
        self._ensure_bucket_exists()
        log.debug("MinIO client initialized")

    @classmethod
    def get_instance(cls) -> Minio:
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _ensure_bucket_exists(self) -> None:
        found = self.client.bucket_exists(self.bucket)
        if not found:
            self.client.make_bucket(self.bucket)
            msg = f"Bucket '{self.bucket}' created."
            log.info(msg)
        else:
            msg = f"Bucket '{self.bucket}' already exists."
            log.debug(msg)

    def upload_object(self, file_path: str, object_path: str) -> None:
        self.client.fput_object(
            bucket_name=self.bucket,
            object_name=object_path,
            file_path=file_path,
        )
        log.debug("File uploaded to MinIO", path=object_path)

    def get_object(self, file_path: str, object_path: str) -> bytes:
        """
        Download an object from MinIO and return its content as bytes.
        """
        try:
            response = self.client.fget_object(
                bucket_name=self.bucket,
                object_name=object_path,
                file_path=file_path,
            )
            msg = f"Object '{object_path}' downloaded from MinIO."
            log.debug(msg)
        except Exception as e:
            msg = f"Error getting object '{object_path}' from MinIO: {e}"
            log.exception(msg)
            raise
        return response
