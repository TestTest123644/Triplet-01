from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import Any

from guardian_layer.utils.schema import Sensor


class TransformerInterface(ABC):
    @abstractmethod
    def transform(self, payload: Mapping[str, Any]) -> Sensor:
        """The payload as a Sensor, or None when it is filtered out or invalid."""
