"""Module ABC — all ETL modules implement this interface."""

from abc import ABC, abstractmethod


class Module(ABC):
    @abstractmethod
    def execute(self, shared_state: dict[str, object]) -> dict[str, object]:
        ...
