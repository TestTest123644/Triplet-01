"""Registry of the dataset loaders by name, and the factory that builds them (#87).

A loader joins the registry with ``@DatasetFactory.register("<name>")`` on its class,
so adding a source is one new module: the factory itself does not change.
"""

from collections.abc import Callable, Iterator
from importlib import import_module
from typing import ClassVar

from guardian_layer.ai.interface.dataset_interface import DatasetInterface
from guardian_layer.utils.dataset_config import DatasetSettings
from guardian_layer.utils.schema import LabeledReading


class DatasetFactory:
    registry: ClassVar[dict[str, type[DatasetInterface]]] = {}

    def __init__(self, settings: DatasetSettings | None = None) -> None:
        # None passes through: the loader resolves the shared settings once
        self.settings = settings

    @classmethod
    def register[Loader: DatasetInterface](
        cls, name: str
    ) -> Callable[[type[Loader]], type[Loader]]:
        """A class decorator adding the loader under ``name``; a name is taken once."""

        def add(loader: type[Loader]) -> type[Loader]:
            taken = cls.registry.get(name)
            # the same class again is a module reload (Jupyter autoreload): replace it
            if taken is not None and (taken.__module__, taken.__qualname__) != (
                loader.__module__,
                loader.__qualname__,
            ):
                raise ValueError(f"dataset already registered: {name}")
            cls.registry[name] = loader
            return loader

        return add

    @classmethod
    def _discover(cls) -> None:
        # the loader modules import this one to register, so they are imported here,
        # when first needed, not at the top of this module
        import_module("guardian_layer.ai.datasets")

    @classmethod
    def names(cls) -> list[str]:
        """Every registered dataset name, sorted."""
        cls._discover()
        return sorted(cls.registry)

    def create(self, name: str) -> DatasetInterface:
        """The named loader, built with this factory's settings."""
        self._discover()
        loader = self.registry.get(name)
        if loader is None:
            raise ValueError(f"unknown dataset: {name}")
        return loader(self.settings)

    def load(self, name: str) -> Iterator[LabeledReading]:
        """The named dataset's readings."""
        return self.create(name).load()
