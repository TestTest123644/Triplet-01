"""Backward-compatibility re-export for telemetry_generator."""

from mocks.data_generator.telemetry_generator import (
    LABELS,
    SENSOR_TYPES,
    SPOOF_SIGMAS,
    VALUE_ANOMALIES,
    generate,
    main,
    noise_std,
    type_signal,
)

__all__ = [
    "LABELS",
    "SENSOR_TYPES",
    "SPOOF_SIGMAS",
    "VALUE_ANOMALIES",
    "generate",
    "main",
    "noise_std",
    "type_signal",
]
