import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest
from sqlalchemy import create_engine, event, inspect, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Session
from sqlalchemy.schema import CreateTable

from guardian_layer.storage import (
    Base,
    ModelOutput,
    ProcessedReading,
    RawReading,
    TransformedWindow,
    create_schema,
    insert_batch,
    parse_iso_timestamp,
    recreate_schema,
)

# mocks/ isn't an installed package; put the repo root on sys.path to import it
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from mocks.telemetry_generator import generate  # noqa: E402


def test_timestamp_is_iso_8601_with_timezone() -> None:
    assert parse_iso_timestamp("2026-10-02T13:30:00+01:00") == datetime(
        2026, 10, 2, 12, 30, tzinfo=UTC
    )
    assert parse_iso_timestamp("2026-10-02T12:30:00Z") == datetime(
        2026, 10, 2, 12, 30, tzinfo=UTC
    )
    with pytest.raises(ValueError, match="timezone offset"):
        parse_iso_timestamp("2026-10-02T12:30:00")
    with pytest.raises(TypeError, match="ISO 8601 string"):
        parse_iso_timestamp(cast(str, 123))


def test_four_stage_schema_and_batch_writes() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    create_schema(engine)
    assert set(inspect(engine).get_table_names()) == {
        "raw_readings",
        "processed_readings",
        "transformed_windows",
        "model_outputs",
    }

    start = parse_iso_timestamp("2026-10-02T12:00:00Z")
    end = parse_iso_timestamp("2026-10-02T12:00:20Z")
    raw_rows = [
        {
            "sensor_id": "water-1",
            "timestamp": end,
            "value": 1.5,
            "unit": "m",
            "metadata": {"lat": 36.78, "lon": 10.18, "type": "water_level"},
        }
        for _ in range(100)
    ]
    insert_statements: list[str] = []

    def count_inserts(
        _connection: object,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        if statement.startswith("INSERT INTO raw_readings"):
            insert_statements.append(statement)

    event.listen(engine, "before_cursor_execute", count_inserts)
    assert insert_batch(engine, RawReading, raw_rows) == 100
    assert len(insert_statements) == 1
    assert (
        insert_batch(
            engine,
            ProcessedReading,
            [
                {
                    "sensor_id": "water-1",
                    "timestamp": end,
                    "value": 1.5,
                    "unit": "m",
                    "quality_flags": ["validated"],
                }
            ],
        )
        == 1
    )
    assert (
        insert_batch(
            engine,
            TransformedWindow,
            [
                {
                    "sensor_id": "water-1",
                    "window_start": start,
                    "window_end": end,
                    "features": {"rate_of_change": 0.1, "rolling_variance": 0.02},
                }
            ],
        )
        == 1
    )
    assert (
        insert_batch(
            engine,
            ModelOutput,
            [
                {
                    "sensor_id": "water-1",
                    "window_end": end,
                    "score": 0.8,
                    "is_anomaly": True,
                    "model_version": "baseline-1",
                }
            ],
        )
        == 1
    )

    with Session(engine) as session:
        assert len(session.scalars(select(RawReading)).all()) == 100
        processed = session.scalars(select(ProcessedReading)).one()
        transformed = session.scalars(select(TransformedWindow)).one()
        output = session.scalars(select(ModelOutput)).one()
        assert processed.quality_flags == ["validated"]
        assert transformed.features["rate_of_change"] == 0.1
        assert output.is_anomaly is True


def test_generator_messages_are_stored_intact() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    create_schema(engine)
    telemetry, _ = generate(duration=2)
    rows = [
        {**message, "timestamp": parse_iso_timestamp(message["timestamp"])}
        for message in telemetry
    ]

    insert_batch(engine, RawReading, rows)

    with Session(engine) as session:
        stored = session.scalars(select(RawReading).order_by(RawReading.id)).all()
    assert [(r.cluster_id, r.sensor_metadata) for r in stored] == [
        (m["cluster_id"], m["metadata"]) for m in telemetry
    ]


def test_insert_batch_rejects_unknown_columns() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    create_schema(engine)
    row = {
        "sensor_id": "water-1",
        "timestamp": parse_iso_timestamp("2026-10-02T12:00:00Z"),
        "value": 1.5,
        "unit": "m",
        "sensor_metadata": {"lat": 36.78},
    }
    with pytest.raises(ValueError, match="sensor_metadata"):
        insert_batch(engine, RawReading, [row])


def test_postgresql_schema_uses_timezone_and_jsonb() -> None:
    raw_ddl = str(
        CreateTable(Base.metadata.tables["raw_readings"]).compile(
            dialect=postgresql.dialect()
        )
    )
    transformed_ddl = str(
        CreateTable(Base.metadata.tables["transformed_windows"]).compile(
            dialect=postgresql.dialect()
        )
    )
    assert "TIMESTAMP WITH TIME ZONE" in raw_ddl
    assert "JSONB" in raw_ddl
    assert "JSONB" in transformed_ddl


def test_recreate_schema_replaces_an_existing_table() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE TABLE raw_readings (id INTEGER PRIMARY KEY)")
        connection.exec_driver_sql("INSERT INTO raw_readings (id) VALUES (1)")

    recreate_schema(engine)

    schema = inspect(engine)
    assert set(schema.get_table_names()) == {
        "raw_readings",
        "processed_readings",
        "transformed_windows",
        "model_outputs",
    }
    assert {column["name"] for column in schema.get_columns("raw_readings")} >= {
        "sensor_id",
        "timestamp",
        "value",
        "unit",
    }
    with Session(engine) as session:
        assert session.scalars(select(RawReading)).all() == []
