"""ORM schema for the four Guardian data stages.

Incoming ISO 8601 timestamps are parsed into timezone-aware ``datetime`` values
before persistence. PostgreSQL stores them as ``TIMESTAMP WITH TIME ZONE``.
"""

from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


# SQLite uses INTEGER for in-memory schema tests; PostgreSQL uses BIGINT.
record_id_type = BigInteger().with_variant(Integer, "sqlite")
json_type = JSON().with_variant(JSONB, "postgresql")


class RawReading(Base):
    """A sensor reading before validation or cleaning."""

    __tablename__ = "raw_readings"
    __table_args__ = (
        Index("ix_raw_readings_sensor_timestamp", "sensor_id", "timestamp"),
    )

    id: Mapped[int] = mapped_column(record_id_type, primary_key=True)
    sensor_id: Mapped[str] = mapped_column(String(255), nullable=False)
    cluster_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    value: Mapped[float] = mapped_column(Float, nullable=False)
    unit: Mapped[str] = mapped_column(String(64), nullable=False)
    sensor_metadata: Mapped[dict[str, Any] | None] = mapped_column(
        "metadata", json_type, nullable=True
    )
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class ProcessedReading(Base):
    """A reading after validation, cleaning, or imputation."""

    __tablename__ = "processed_readings"
    __table_args__ = (
        Index("ix_processed_readings_sensor_timestamp", "sensor_id", "timestamp"),
    )

    id: Mapped[int] = mapped_column(record_id_type, primary_key=True)
    sensor_id: Mapped[str] = mapped_column(String(255), nullable=False)
    cluster_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    value: Mapped[float] = mapped_column(Float, nullable=False)
    unit: Mapped[str] = mapped_column(String(64), nullable=False)
    sensor_metadata: Mapped[dict[str, Any] | None] = mapped_column(
        "metadata", json_type, nullable=True
    )
    quality_flags: Mapped[list[str] | None] = mapped_column(json_type, nullable=True)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class TransformedWindow(Base):
    """Features extracted from one sensor's time window."""

    __tablename__ = "transformed_windows"
    __table_args__ = (
        Index("ix_transformed_windows_sensor_end", "sensor_id", "window_end"),
    )

    id: Mapped[int] = mapped_column(record_id_type, primary_key=True)
    sensor_id: Mapped[str] = mapped_column(String(255), nullable=False)
    window_start: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    window_end: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    features: Mapped[dict[str, float]] = mapped_column(json_type, nullable=False)
    transformed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class ModelOutput(Base):
    """A model verdict for a sensor window."""

    __tablename__ = "model_outputs"
    __table_args__ = (Index("ix_model_outputs_sensor_end", "sensor_id", "window_end"),)

    id: Mapped[int] = mapped_column(record_id_type, primary_key=True)
    sensor_id: Mapped[str] = mapped_column(String(255), nullable=False)
    window_end: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    score: Mapped[float] = mapped_column(Float, nullable=False)
    is_anomaly: Mapped[bool] = mapped_column(Boolean, nullable=False)
    model_version: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
