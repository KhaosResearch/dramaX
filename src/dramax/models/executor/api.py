from typing import Any

from .base import Executor


class APIExecutor(Executor):
    url: str
    method: str = "POST"
    headers: dict[str, str]
    body: dict[str, Any]
    type: str = "api"

    def execute(self) -> None:
        pass  # lógica específica
