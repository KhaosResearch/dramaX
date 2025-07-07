from typing import Any, Literal

from .base import Executor


class APIExecutor(Executor):
    type: Literal["api"] = "api"
    url: str
    method: str = "POST"
    headers: dict[str, str]
    body: dict[str, Any]

    def execute(self) -> None:
        pass  # lógica específica
