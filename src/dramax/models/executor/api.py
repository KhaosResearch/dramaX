from typing import Any, Dict

from .base import Executor


class APIExecutor(Executor):
    url: str
    method: str = "POST"
    headers: Dict[str, str] = {}
    body: Dict[str, Any] = {}
    type: str = "api"

    def execute(self):
        pass  # lógica específica
