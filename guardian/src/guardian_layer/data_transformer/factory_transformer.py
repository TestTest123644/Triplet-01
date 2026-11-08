"""Registry of the twin transformers by name, and the factory that builds them.

A transformer joins the registry with ``@TransformerFactory.register("<name>")`` on its
class, so adding a twin schema version is one new module: the factory does not change.
"""

from collections.abc import Callable
from importlib import import_module
from typing import ClassVar, TypeVar

from guardian_layer.data_transformer.interface.factory_transformer_interface import (
    TransformerInterface,
)
from guardian_layer.utils.logger import get_logger

logger = get_logger(__name__)

T = TypeVar("T", bound=TransformerInterface)


class TransformerFactory:
    registry: ClassVar[dict[str, type[TransformerInterface]]] = {}

    @classmethod
    def register(cls, name: str) -> Callable[[type[T]], type[T]]:
        """A class decorator adding the transformer under ``name``, taken once."""

        def add(transformer: type[T]) -> type[T]:
            taken = cls.registry.get(name)
            # the same class again is a module reload (Jupyter autoreload): replace it
            if taken is not None and (taken.__module__, taken.__qualname__) != (
                transformer.__module__,
                transformer.__qualname__,
            ):
                raise ValueError(f"transformer already registered: {name}")
            cls.registry[name] = transformer
            return transformer

        return add

    @classmethod
    def _discover(cls) -> None:
        # the transformer modules import this one to register, so they are imported
        # here, when first needed, not at the top of this module
        import_module("guardian_layer.data_transformer.transformer")

    @classmethod
    def names(cls) -> list[str]:
        """Every registered transformer name, sorted."""
        cls._discover()
        return sorted(cls.registry)

    @classmethod
    def get(cls, name: str) -> type[TransformerInterface]:
        """The transformer class registered under ``name``."""
        cls._discover()
        transformer = cls.registry.get(name)
        if transformer is None:
            logger.error(
                "unknown transformer: %s (known: %s)", name, sorted(cls.registry)
            )
            raise ValueError(f"unknown transformer: {name}")
        return transformer

    @classmethod
    def create(cls, name: str) -> TransformerInterface:
        """A new instance of the transformer registered under ``name``."""
        transformer = cls.get(name)()
        logger.info("transformer created: %s -> %s", name, type(transformer).__name__)
        return transformer
