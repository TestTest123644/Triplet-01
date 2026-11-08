# create manger interface for all managers to implement

from abc import ABC, abstractmethod
from typing import Any

from guardian_layer.utils.schema import Ack, Buffer, Cluster, Sensor


class ManagerInterface(ABC):
    @abstractmethod
    def add(self, sensor: Sensor) -> Ack: ...
    @abstractmethod
    def delete(self, id: str) -> Ack: ...
    @abstractmethod
    def validate(self, value: str) -> Ack: ...
    @abstractmethod
    @abstractmethod
    def create(self, value: Any) -> Buffer | Cluster | Ack | None: ...
    @abstractmethod
    def get(self, value: str) -> Buffer | Cluster | Ack: ...
