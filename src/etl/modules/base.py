"""Module ABC — all ETL modules implement this interface."""

from abc import ABC, abstractmethod
from enum import Enum


class WriteMode(Enum):
    OVERWRITE = "Overwrite"
    APPEND = "Append"


class Module(ABC):
    @abstractmethod
    def execute(self, shared_state: dict[str, object]) -> dict[str, object]:
        ...
