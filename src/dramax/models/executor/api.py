from typing import Any, Literal

import requests

from .base import Executor


class APIExecutor(Executor):
    type: Literal["api"] = "api"
    url: str
    method: str = "POST"
    headers: dict[str, str]
    body: dict[str, Any]
    timeout: int = 10

    def execute(self, files) -> Any:
        if self.method == "POST":  # TODO hacer enums para validaciones del proyecto
            response = requests.post(
                url=self.url,
                headers=self.headers,
                data=self.body,
                files={"data_file": open("/path/to/cultivo.csv", "rb")},
                timeout=self.timeout,
            )
            return response
        return None
