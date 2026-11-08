"""Connection setup, schema creation, and batched writes."""

import os
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Engine, create_engine, insert

from guardian_layer.storage.models import Base
from guardian_layer.utils import get_logger

logger = get_logger(__name__)


def parse_iso_timestamp(value: str) -> datetime:
    """Parse an ISO 8601 timestamp with an explicit offset and normalize to UTC."""
    try:
        timestamp = datetime.fromisoformat(value)
    except TypeError as exc:
        raise TypeError("timestamp must be an ISO 8601 string") from exc
    except ValueError as exc:
        raise ValueError("timestamp must be an ISO 8601 string") from exc
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("timestamp must include a timezone offset")
    return timestamp.astimezone(UTC)


def make_engine(database_url: str | None = None) -> Engine:
    """Build a pooled SQLAlchemy engine for the configured PostgreSQL database."""
    url = database_url or os.getenv("GUARDIAN_DATABASE_URL")
    if not url:
        raise ValueError("GUARDIAN_DATABASE_URL is required")
    return create_engine(url, pool_pre_ping=True)


def create_schema(engine: Engine) -> None:
    """Create missing stage tables without changing existing tables."""
    Base.metadata.create_all(engine)


def recreate_schema(engine: Engine) -> None:
    """Drop and rebuild all stage tables, deleting their existing data."""
    with engine.begin() as connection:
        Base.metadata.drop_all(connection)
        Base.metadata.create_all(connection)


def insert_batch(
    engine: Engine,
    model: type[Base],
    rows: Sequence[Mapping[str, Any]],
) -> int:
    """Insert one batch in one transaction instead of committing each reading.

    Rows are keyed by column name (``metadata``, not ``sensor_metadata``).
    Timestamps must already be ``datetime`` values from ``parse_iso_timestamp``.
    """
    if not rows:
        return 0
    table = Base.metadata.tables[model.__tablename__]
    # SQLAlchemy silently ignores unknown keys in a batch insert
    unknown = {key for row in rows for key in row} - set(table.columns.keys())
    if unknown:
        raise ValueError(f"unknown columns for {table.name}: {sorted(unknown)}")
    try:
        with engine.begin() as connection:
            connection.execute(insert(table), [dict(row) for row in rows])
    except Exception:
        logger.exception(
            "Failed to persist reading batch",
            table=table.name,
            row_count=len(rows),
        )
        raise
    logger.info(
        "Persisted reading batch",
        table=table.name,
        row_count=len(rows),
    )
    return len(rows)
