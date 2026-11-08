from collections.abc import Sequence
from typing import Any, TypeVar, cast

from psycopg import sql
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool
from pydantic import TypeAdapter

from guardian_layer.utils.connection import DatabaseConnection
from guardian_layer.utils.logger import get_logger
from guardian_layer.utils.schema import (
    Sensor,
    SensorSummary,
    Verdict,
)

logger = get_logger(__name__)
T = TypeVar("T", Sensor, SensorSummary, Verdict)


class Postgress:
    """Manage asynchronous PostgreSQL CRUD operations."""

    TABLES = {
        Sensor: "sensors",
        SensorSummary: "sensor_summaries",
        Verdict: "verdicts",
    }

    def __init__(self) -> None:
        """Use the shared asynchronous database manager."""
        self._database = DatabaseConnection()

    @property
    def pool(self) -> AsyncConnectionPool[Any]:
        """Access the shared asynchronous connection pool."""
        return self._database.pool

    def _get_table(self, model_or_instance: type | object) -> str:
        """Resolve a table for a model class or instance."""
        model_type = (
            model_or_instance
            if isinstance(model_or_instance, type)
            else type(model_or_instance)
        )

        for model, table in self.TABLES.items():
            if issubclass(model_type, model):
                return table

        raise ValueError(f"Unsupported model: {model_type}")

    def _serialize(self, data: T) -> dict[str, Any]:
        """Serialize a model into JSON-compatible data."""
        self._get_table(data)
        adapter = TypeAdapter(type(data))
        return adapter.dump_python(data, mode="json")

    def _deserialize(
        self,
        model_type: type[T],
        data: dict[str, Any],
    ) -> T:
        """Validate and reconstruct a model from JSONB."""
        self._get_table(model_type)
        adapter = TypeAdapter(model_type)
        return cast(T, adapter.validate_python(data))

    async def _execute(
        self,
        query: sql.Composable,
        params: Sequence[Any] = (),
        fetch: bool = False,
    ) -> tuple[tuple[Any, ...] | None, int]:
        """Execute a query with an asynchronous pooled connection."""
        async with self.pool.connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, params)

                row = await cursor.fetchone() if fetch else None

                return row, cursor.rowcount

    def _check_affected_rows(
        self,
        rowcount: int,
        record_id: int,
        table: str,
    ) -> None:
        """Raise an error if no database record was affected."""
        if rowcount == 0:
            raise LookupError(f"Record {record_id} not found in {table}")

    async def create(self, data: T) -> int:
        """Insert one record asynchronously and return its ID."""
        table = self._get_table(data)
        payload = self._serialize(data)

        query = sql.SQL("""
            INSERT INTO {} (data)
            VALUES (%s)
            RETURNING id
        """).format(sql.Identifier(table))

        row, _ = await self._execute(
            query,
            (Jsonb(payload),),
            fetch=True,
        )

        if row is None:
            raise RuntimeError("Insert returned no ID")

        logger.debug("Created record in %s", table)
        return cast(int, row[0])

    async def create_batch(self, data: Sequence[T]) -> list[int]:
        """Insert records asynchronously in one transaction."""
        if not data:
            return []

        table = self._get_table(data[0])
        payloads: list[dict[str, Any]] = []

        for item in data:
            if self._get_table(item) != table:
                raise ValueError("All records must belong to the same table")

            payloads.append(self._serialize(item))

        batch_size = 500
        ids: list[int] = []

        async with self.pool.connection() as conn:
            async with conn.cursor() as cursor:
                for start in range(0, len(payloads), batch_size):
                    batch = payloads[start : start + batch_size]

                    placeholders = sql.SQL(", ").join(sql.SQL("(%s)") for _ in batch)

                    query = sql.SQL("""
                        INSERT INTO {} (data)
                        VALUES {}
                        RETURNING id
                    """).format(
                        sql.Identifier(table),
                        placeholders,
                    )

                    await cursor.execute(
                        query,
                        tuple(Jsonb(item) for item in batch),
                    )

                    rows = await cursor.fetchall()

                    if len(rows) != len(batch):
                        raise RuntimeError(
                            "Batch insertion returned an unexpected number of IDs"
                        )

                    ids.extend(cast(int, row[0]) for row in rows)

        logger.info(
            "Successfully inserted %d records into %s",
            len(ids),
            table,
        )
        return ids

    async def get(
        self,
        model_type: type[T],
        record_id: int,
    ) -> T:
        """Retrieve and reconstruct one record asynchronously."""
        table = self._get_table(model_type)

        query = sql.SQL("""
            SELECT data
            FROM {}
            WHERE id = %s
        """).format(sql.Identifier(table))

        row, _ = await self._execute(
            query,
            (record_id,),
            fetch=True,
        )

        if row is None:
            raise LookupError(f"Record {record_id} not found in {table}")

        return self._deserialize(model_type, row[0])

    async def update(self, data: T, record_id: int) -> None:
        """Update an existing record asynchronously."""
        table = self._get_table(data)
        payload = self._serialize(data)

        query = sql.SQL("""
            UPDATE {}
            SET data = %s
            WHERE id = %s
        """).format(sql.Identifier(table))

        _, rowcount = await self._execute(
            query,
            (Jsonb(payload), record_id),
        )

        self._check_affected_rows(rowcount, record_id, table)
        logger.debug("Updated record %s in %s", record_id, table)

    async def delete(
        self,
        model_type: type[T],
        record_id: int,
    ) -> None:
        """Delete an existing record asynchronously."""
        table = self._get_table(model_type)

        query = sql.SQL("""
            DELETE FROM {}
            WHERE id = %s
        """).format(sql.Identifier(table))

        _, rowcount = await self._execute(
            query,
            (record_id,),
        )

        self._check_affected_rows(rowcount, record_id, table)
        logger.debug("Deleted record %s from %s", record_id, table)
