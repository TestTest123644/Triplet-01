from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from psycopg import sql
from psycopg.types.json import Jsonb
from pydantic import ValidationError

from guardian_layer.storage.connections.postgress import Postgress
from guardian_layer.utils.connection import DatabaseConnection
from guardian_layer.utils.schema import (
    Anomalies,
    Metadata,
    Sensor,
    SensorSummary,
    Verdict,
)


@pytest.fixture
def storage():
    """Create storage without starting the database pool."""
    return Postgress()


@pytest.fixture
def sensor():
    """Create a valid Sensor."""
    return Sensor(
        sensor_id="sensor-1",
        cluster_id="temperature",
        value=25.0,
        unit="C",
        timestamp=datetime(2026, 10, 15, 9, 0),
        metadata=Metadata(
            lat=36.8,
            lon=10.18,
            packet_order=1,
            type="temperature",
            lower_range=-20,
            upper_range=60,
        ),
    )


@pytest.fixture
def summary():
    """Create a valid SensorSummary."""
    return SensorSummary(
        sensor_id="sensor-1",
        cluster="temperature",
        lat=36.8,
        lon=10.18,
        mean=25.0,
        last=26.0,
    )


@pytest.fixture
def verdict():
    """Create a valid Verdict."""
    return Verdict(
        sensor_id="sensor-1",
        score=0.9,
        label=Anomalies.OUT_OF_RANGE,
    )


@pytest.fixture
def mock_pool():
    """Mock asynchronous PostgreSQL pool, connection, and cursor."""
    pool = MagicMock()
    conn = MagicMock()
    cursor = MagicMock()

    # pool.connection() returns an async context manager.
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=None)

    # conn.cursor() is synchronous and returns an async
    # context manager, not a coroutine.
    conn.cursor.return_value.__aenter__ = AsyncMock(return_value=cursor)
    conn.cursor.return_value.__aexit__ = AsyncMock(return_value=None)

    # Cursor database operations are asynchronous.
    cursor.execute = AsyncMock()
    cursor.fetchone = AsyncMock()
    cursor.fetchall = AsyncMock()
    cursor.rowcount = 0

    with patch.object(
        Postgress,
        "pool",
        property(lambda self: pool),
    ):
        yield pool, cursor


# --------------------------------------------------
# TABLE RESOLUTION
# --------------------------------------------------


@pytest.mark.parametrize(
    ("model", "expected"),
    [
        (Sensor, "sensors"),
        (SensorSummary, "sensor_summaries"),
        (Verdict, "verdicts"),
    ],
)
def test_get_table_class(storage, model, expected):
    assert storage._get_table(model) == expected


def test_get_table_instance(storage, sensor, summary, verdict):
    assert storage._get_table(sensor) == "sensors"
    assert storage._get_table(summary) == "sensor_summaries"
    assert storage._get_table(verdict) == "verdicts"


def test_get_table_subclass(storage):
    class TemperatureSensor(Sensor):
        pass

    assert storage._get_table(TemperatureSensor) == "sensors"


def test_get_table_subclass_instance(storage, sensor):
    class TemperatureSensor(Sensor):
        pass

    instance = TemperatureSensor(**sensor.model_dump())

    assert storage._get_table(instance) == "sensors"


def test_get_table_invalid_model(storage):
    with pytest.raises(ValueError, match="Unsupported model"):
        storage._get_table(str)


def test_get_table_invalid_instance(storage):
    with pytest.raises(ValueError, match="Unsupported model"):
        storage._get_table("invalid")


# --------------------------------------------------
# SERIALIZATION
# --------------------------------------------------


def test_serialize_sensor(storage, sensor):
    result = storage._serialize(sensor)

    assert result["sensor_id"] == "sensor-1"
    assert result["value"] == 25.0
    assert result["timestamp"] == "2026-10-15T09:00:00"
    assert result["metadata"]["lat"] == 36.8


def test_deserialize_sensor(storage, sensor):
    payload = storage._serialize(sensor)

    result = storage._deserialize(Sensor, payload)

    assert isinstance(result, Sensor)
    assert isinstance(result.timestamp, datetime)
    assert isinstance(result.metadata, Metadata)
    assert result == sensor


def test_summary_roundtrip(storage, summary):
    payload = storage._serialize(summary)
    result = storage._deserialize(SensorSummary, payload)

    assert isinstance(result, SensorSummary)
    assert result == summary


def test_verdict_roundtrip(storage, verdict):
    payload = storage._serialize(verdict)
    result = storage._deserialize(Verdict, payload)

    assert payload["label"] == "out_of_range"
    assert result == verdict
    assert result.label is Anomalies.OUT_OF_RANGE


def test_verdict_without_anomaly(storage):
    verdict = Verdict(
        sensor_id="sensor-1",
        score=0.1,
        label=None,
    )

    payload = storage._serialize(verdict)
    result = storage._deserialize(Verdict, payload)

    assert result.label is None


def test_deserialize_missing_field(storage):
    with pytest.raises(ValidationError):
        storage._deserialize(
            Verdict,
            {
                "sensor_id": "sensor-1",
                "score": 0.9,
            },
        )


def test_deserialize_invalid_enum(storage):
    with pytest.raises(ValidationError):
        storage._deserialize(
            Verdict,
            {
                "sensor_id": "sensor-1",
                "score": 0.9,
                "label": "invalid",
            },
        )


def test_serialize_invalid_type(storage):
    with pytest.raises(ValueError, match="Unsupported model"):
        storage._serialize("invalid")


# --------------------------------------------------
# SHARED CONNECTION POOL
# --------------------------------------------------


def test_storage_uses_singleton():
    first = Postgress()
    second = Postgress()

    assert first._database is second._database


def test_storage_pool_property(storage):
    fake_pool = MagicMock()

    with patch.object(
        DatabaseConnection,
        "pool",
        property(lambda self: fake_pool),
    ):
        assert storage.pool is fake_pool


# --------------------------------------------------
# ASYNC EXECUTION
# --------------------------------------------------


@pytest.mark.asyncio
async def test_execute_with_fetch(storage, mock_pool):
    pool, cursor = mock_pool

    cursor.fetchone.return_value = (42,)
    cursor.rowcount = 1

    query = sql.SQL("SELECT id FROM sensors WHERE id = %s")

    row, count = await storage._execute(
        query,
        (42,),
        fetch=True,
    )

    assert row == (42,)
    assert count == 1
    cursor.execute.assert_awaited_once_with(query, (42,))
    cursor.fetchone.assert_awaited_once()
    pool.connection.assert_called_once()


@pytest.mark.asyncio
async def test_execute_without_fetch(storage, mock_pool):
    _, cursor = mock_pool
    cursor.rowcount = 1

    query = sql.SQL("DELETE FROM sensors WHERE id = %s")

    row, count = await storage._execute(query, (42,))

    assert row is None
    assert count == 1
    cursor.fetchone.assert_not_awaited()


@pytest.mark.asyncio
async def test_execute_failure(storage, mock_pool):
    _, cursor = mock_pool
    cursor.execute.side_effect = RuntimeError("SQL failed")

    with pytest.raises(RuntimeError, match="SQL failed"):
        await storage._execute(sql.SQL("SELECT 1"))


def test_check_affected_rows_success(storage):
    storage._check_affected_rows(1, 42, "sensors")


def test_check_affected_rows_not_found(storage):
    with pytest.raises(
        LookupError,
        match="Record 42 not found in sensors",
    ):
        storage._check_affected_rows(0, 42, "sensors")


# --------------------------------------------------
# CREATE
# --------------------------------------------------


@pytest.mark.asyncio
async def test_create_success(storage, sensor):
    with patch.object(
        storage,
        "_execute",
        new_callable=AsyncMock,
        return_value=((42,), 1),
    ) as execute:
        result = await storage.create(sensor)

    assert result == 42
    execute.assert_awaited_once()

    args, kwargs = execute.call_args

    assert kwargs["fetch"] is True
    assert isinstance(args[1][0], Jsonb)
    assert args[1][0].obj["sensor_id"] == "sensor-1"


@pytest.mark.asyncio
async def test_create_missing_id(storage, sensor):
    with patch.object(
        storage,
        "_execute",
        new_callable=AsyncMock,
        return_value=(None, 0),
    ):
        with pytest.raises(
            RuntimeError,
            match="Insert returned no ID",
        ):
            await storage.create(sensor)


@pytest.mark.asyncio
async def test_create_database_failure(storage, sensor):
    with patch.object(
        storage,
        "_execute",
        new_callable=AsyncMock,
        side_effect=RuntimeError("Insert failed"),
    ):
        with pytest.raises(RuntimeError, match="Insert failed"):
            await storage.create(sensor)


# --------------------------------------------------
# GET
# --------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fixture_name",
    ["sensor", "summary", "verdict"],
)
async def test_get_success(storage, fixture_name, request):
    model = request.getfixturevalue(fixture_name)
    payload = storage._serialize(model)

    with patch.object(
        storage,
        "_execute",
        new_callable=AsyncMock,
        return_value=((payload,), 1),
    ):
        result = await storage.get(type(model), 42)

    assert result == model


@pytest.mark.asyncio
async def test_get_not_found(storage):
    with patch.object(
        storage,
        "_execute",
        new_callable=AsyncMock,
        return_value=(None, 0),
    ):
        with pytest.raises(LookupError, match="not found"):
            await storage.get(Sensor, 999)


@pytest.mark.asyncio
async def test_get_invalid_data(storage):
    with patch.object(
        storage,
        "_execute",
        new_callable=AsyncMock,
        return_value=(({"invalid": True},), 1),
    ):
        with pytest.raises(ValidationError):
            await storage.get(Sensor, 42)


@pytest.mark.asyncio
async def test_get_database_failure(storage):
    with patch.object(
        storage,
        "_execute",
        new_callable=AsyncMock,
        side_effect=RuntimeError("Select failed"),
    ):
        with pytest.raises(RuntimeError, match="Select failed"):
            await storage.get(Sensor, 42)


# --------------------------------------------------
# UPDATE
# --------------------------------------------------


@pytest.mark.asyncio
async def test_update_success(storage, sensor):
    with patch.object(
        storage,
        "_execute",
        new_callable=AsyncMock,
        return_value=(None, 1),
    ) as execute:
        result = await storage.update(sensor, 42)

    assert result is None

    args, _ = execute.call_args
    assert isinstance(args[1][0], Jsonb)
    assert args[1][1] == 42


@pytest.mark.asyncio
async def test_update_not_found(storage, sensor):
    with patch.object(
        storage,
        "_execute",
        new_callable=AsyncMock,
        return_value=(None, 0),
    ):
        with pytest.raises(LookupError, match="not found"):
            await storage.update(sensor, 999)


@pytest.mark.asyncio
async def test_update_database_failure(storage, sensor):
    with patch.object(
        storage,
        "_execute",
        new_callable=AsyncMock,
        side_effect=RuntimeError("Update failed"),
    ):
        with pytest.raises(RuntimeError, match="Update failed"):
            await storage.update(sensor, 42)


# --------------------------------------------------
# DELETE
# --------------------------------------------------


@pytest.mark.asyncio
async def test_delete_success(storage):
    with patch.object(
        storage,
        "_execute",
        new_callable=AsyncMock,
        return_value=(None, 1),
    ) as execute:
        result = await storage.delete(Sensor, 42)

    assert result is None

    args, _ = execute.call_args
    assert args[1] == (42,)


@pytest.mark.asyncio
async def test_delete_not_found(storage):
    with patch.object(
        storage,
        "_execute",
        new_callable=AsyncMock,
        return_value=(None, 0),
    ):
        with pytest.raises(LookupError, match="not found"):
            await storage.delete(Sensor, 999)


@pytest.mark.asyncio
async def test_delete_database_failure(storage):
    with patch.object(
        storage,
        "_execute",
        new_callable=AsyncMock,
        side_effect=RuntimeError("Delete failed"),
    ):
        with pytest.raises(RuntimeError, match="Delete failed"):
            await storage.delete(Sensor, 42)


# --------------------------------------------------
# BATCH INSERTION
# --------------------------------------------------


@pytest.mark.asyncio
async def test_create_batch_empty(storage, mock_pool):
    pool, _ = mock_pool

    assert await storage.create_batch([]) == []
    pool.connection.assert_not_called()


@pytest.mark.asyncio
async def test_create_batch_success(storage, sensor, mock_pool):
    pool, cursor = mock_pool

    cursor.fetchall.return_value = [
        (1,),
        (2,),
        (3,),
    ]

    result = await storage.create_batch([sensor, sensor, sensor])

    assert result == [1, 2, 3]
    pool.connection.assert_called_once()
    cursor.execute.assert_awaited_once()
    cursor.fetchall.assert_awaited_once()


@pytest.mark.asyncio
async def test_create_batch_mixed_models(
    storage,
    sensor,
    verdict,
    mock_pool,
):
    pool, _ = mock_pool

    with pytest.raises(ValueError, match="same table"):
        await storage.create_batch([sensor, verdict])

    pool.connection.assert_not_called()


@pytest.mark.asyncio
async def test_create_batch_large(storage, sensor, mock_pool):
    pool, cursor = mock_pool

    cursor.fetchall.side_effect = [
        [(i,) for i in range(1, 501)],
        [(501,), (502,)],
    ]

    result = await storage.create_batch([sensor] * 502)

    assert result == list(range(1, 503))
    assert cursor.execute.await_count == 2
    assert cursor.fetchall.await_count == 2
    pool.connection.assert_called_once()


@pytest.mark.asyncio
async def test_create_batch_wrong_id_count(
    storage,
    sensor,
    mock_pool,
):
    _, cursor = mock_pool
    cursor.fetchall.return_value = [(1,)]

    with pytest.raises(
        RuntimeError,
        match="unexpected number of IDs",
    ):
        await storage.create_batch([sensor, sensor])


@pytest.mark.asyncio
async def test_create_batch_database_failure(
    storage,
    sensor,
    mock_pool,
):
    _, cursor = mock_pool

    cursor.execute.side_effect = RuntimeError("Insert failed")

    with pytest.raises(RuntimeError, match="Insert failed"):
        await storage.create_batch([sensor])


@pytest.mark.asyncio
async def test_create_batch_propagates_error_to_transaction(
    storage,
    sensor,
    mock_pool,
):
    pool, cursor = mock_pool
    cursor.execute.side_effect = RuntimeError("Insert failed")

    with pytest.raises(RuntimeError, match="Insert failed"):
        await storage.create_batch([sensor])

    exit_args = pool.connection.return_value.__aexit__.call_args.args

    assert exit_args[0] is RuntimeError
    assert isinstance(exit_args[1], RuntimeError)


@pytest.mark.asyncio
async def test_create_batch_logging(
    storage,
    sensor,
    mock_pool,
):
    _, cursor = mock_pool
    cursor.fetchall.return_value = [(1,), (2,)]

    with patch("guardian_layer.storage.connections.postgress.logger") as mock_logger:
        result = await storage.create_batch([sensor, sensor])

    assert result == [1, 2]

    mock_logger.info.assert_called_once_with(
        "Successfully inserted %d records into %s",
        2,
        "sensors",
    )
