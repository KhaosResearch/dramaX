from pathlib import Path
from typing import Any, Literal

import requests
from structlog import get_logger

from .base import Executor


class APIExecutor(Executor):
    type: Literal["api"] = "api"
    url: str
    method: str = "POST"
    headers: dict[str, str]
    auth: tuple | None
    body: dict[str, Any] | None
    timeout: int = 10
    input_dir: str | None
    output_dir: str | None

    def execute(
        self,
    ) -> str:
        method = self.method.upper()
        if method == "GET":
            result = self.get()
        elif method == "POST":
            result = self.post()
        return result

    def get(self) -> str:
        # ! De momento unica hace un get a un csv, estudiar mas casuisticas
        log = get_logger("dramax.api_executor.get")
        log.bind(url=self.url, method="GET")
        try:
            if self.auth:  # self.auth debería ser una tupla (usuario, contraseña)
                response = requests.get(
                    self.url,
                    headers=self.headers,
                    timeout=self.timeout,
                    auth=self.auth,
                )
                response.raise_for_status()

                if self.output_dir:
                    with Path.open(self.output_dir, "wb") as f:
                        f.write(response.content)

                    message = (
                        f"[SUCCESS] File downloaded with status {response.status_code} "
                        f"({response.reason}) and saved to {self.output_dir}"
                    )
                    log.info(message)
                    return message
                message = (
                    f"[WARNING] File downloaded with status {response.status_code} "
                    f"({response.reason}), but no local_dir specified. File not saved."
                )
                log.warning(message)
                return message
        except requests.RequestException as e:
            message = f"[ERROR] Failed to download file from {self.url}: {e!s}"
            log.exception(message)
        return message

    def post(self) -> str:
        log = get_logger("dramax.api_executor.post")
        log = log.bind(url=self.url, method="POST")
        file_path = Path(self.output_dir)
        try:
            if self.auth:
                with Path.open(file_path, "rb") as f:
                    files = {"data_file": f}
                    response = requests.post(
                        self.url,
                        files=files,
                        auth=self.auth,
                        timeout=30,
                    )
                    response.raise_for_status()
                message = (
                    f"[SUCCESS] File posted with status {response.status_code} "
                    f"({response.reason})"
                )
                log.info(message)
            else:
                message = f"[ERROR] Failed to authenticate to {self.url}"
                log.exception(message)
        except requests.RequestException as e:
            message = f"[ERROR] Failed to post to {self.url}: {e!s}"
            log.exception(message)
            raise
        return message
