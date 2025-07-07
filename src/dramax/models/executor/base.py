from abc import ABC, abstractmethod

from pydantic import BaseModel


class Executor(BaseModel, ABC):
    type: str

    @abstractmethod
    def execute(self) -> None: ...
