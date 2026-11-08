# the interface every dataset loader implements (lead's review on #87)

from abc import ABC, abstractmethod
from collections.abc import Iterator

from guardian_layer.utils.dataset_config import DatasetSettings, get_dataset_settings
from guardian_layer.utils.schema import LabeledReading


class DatasetInterface(ABC):
    def __init__(self, settings: DatasetSettings | None = None) -> None:
        # reads no dataset file, load() does; the first call reads config/datasets.yaml
        self.settings = settings or get_dataset_settings()

    @abstractmethod
    def load(self) -> Iterator[LabeledReading]: ...
