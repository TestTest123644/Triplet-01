"""Centralized logger configuration for guardian_layer."""

import logging
import os
import sys
import threading
from collections.abc import MutableMapping
from typing import Any

from guardian_layer.utils.config import get_settings
from guardian_layer.utils.elastic import AsyncElasticSearchHandler

PROJECT_LOGGER_NAME = "guardian_layer"
ENV_VAR_NAME = "GUARDIAN_LAYER_LOG"
DISABLED_LOG_LEVEL = 9001
DEFAULT_LOG_LEVEL = "WARNING"
DEFAULT_FORMAT = (
    "%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d - %(message)s"
)
_configured = False
_configure_lock = threading.RLock()
LOG_RECORD_RESERVED_KEYS = {
    key: f"_{key}" for key in logging.makeLogRecord({}).__dict__
}


def _parse_log_setting(value: str | None) -> tuple[bool, str]:
    if not value:
        return True, DEFAULT_LOG_LEVEL
    value = value.lower().strip()
    if value in ("off", "false", "0", "disabled", "none"):
        return False, DEFAULT_LOG_LEVEL
    levels = {"1": "ERROR", "2": "WARNING", "3": "INFO", "4": "DEBUG"}
    if value in levels:
        return True, levels[value]
    if value.upper() in ("ERROR", "WARNING", "INFO", "DEBUG"):
        return True, value.upper()
    return True, DEFAULT_LOG_LEVEL


class LoggerAdapter(logging.LoggerAdapter):
    """Adapter accepting structured keyword arguments in log calls."""

    def process(
        self,
        msg: str,
        kwargs: MutableMapping[str, Any],
    ) -> tuple[str, MutableMapping[str, Any]]:
        if not kwargs:
            return msg, kwargs
        extra_data: dict[str, Any] = {}
        valid_kwargs: dict[str, Any] = {}
        for key, value in kwargs.items():
            if key in {"exc_info", "extra", "stack_info", "stacklevel"}:
                valid_kwargs[key] = value
            else:
                extra_data[LOG_RECORD_RESERVED_KEYS.get(key, key)] = value
        if extra_data:
            existing = valid_kwargs.setdefault("extra", {})
            existing.update(extra_data)
        return msg, valid_kwargs


def _configure_default() -> None:
    global _configured
    with _configure_lock:
        if _configured:
            return
        log_setting = os.getenv(ENV_VAR_NAME)
        if log_setting == "unconfigured":
            _configured = True
            return
        configure(log_setting)


def get_logger(module_name: str) -> LoggerAdapter:
    """Get a configured hierarchical logger for a module."""
    _configure_default()
    return LoggerAdapter(logging.getLogger(module_name), {})


def configure(level: str | None = None, format: str | None = None) -> None:
    """Configure local and optional asynchronous Elasticsearch handlers."""
    global _configured
    enabled, log_level = _parse_log_setting(level)
    with _configure_lock:
        logger = logging.getLogger(PROJECT_LOGGER_NAME)
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            handler.close()
        logger.propagate = False
        if not enabled:
            logger.addHandler(logging.NullHandler())
            logger.setLevel(DISABLED_LOG_LEVEL)
            _configured = True
            return

        log_format = format or DEFAULT_FORMAT
        formatter = logging.Formatter(log_format)
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setLevel(getattr(logging, log_level))
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        elastic_settings = get_settings().elasticsearch
        endpoint = os.getenv("GUARDIAN_ELASTICSEARCH_URL", elastic_settings.url)
        es_password = os.getenv("ELASTICSEARCH_PASSWORD", "")
        if endpoint and es_password:
            try:
                elastic_handler = AsyncElasticSearchHandler(
                    elastic_settings,
                    endpoint=endpoint,
                )
                elastic_handler.setLevel(getattr(logging, log_level))
                logger.addHandler(elastic_handler)
            except Exception as e:
                logger.warning("Failed to initialize Elasticsearch log handler: %s", e)

        logger.setLevel(getattr(logging, log_level))
        _configured = True


def disable() -> None:
    """Disable all guardian_layer logging."""
    configure("off")


def enable(level: str = "INFO") -> None:
    """Enable guardian_layer logging at the requested level."""
    configure(level)


def is_enabled() -> bool:
    """Return whether guardian_layer logging is enabled."""
    return logging.getLogger(PROJECT_LOGGER_NAME).level < DISABLED_LOG_LEVEL


__all__ = [
    "get_logger",
    "configure",
    "disable",
    "enable",
    "is_enabled",
    "AsyncElasticSearchHandler",
]
