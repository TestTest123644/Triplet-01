from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from threading import Lock
from typing import Any

from psycopg_pool import AsyncConnectionPool

from guardian_layer.utils.logger import get_logger

logger = get_logger(__name__)


class DatabaseConnection:
    """Manage one shared asynchronous PostgreSQL connection pool."""

    _instance: DatabaseConnection | None = None
    _instance_lock = Lock()
    _pool: AsyncConnectionPool[Any] | None
    _lock: asyncio.Lock
    _loop: asyncio.AbstractEventLoop | None

    def __new__(cls) -> DatabaseConnection:
        """Create or return the singleton database manager."""
        with cls._instance_lock:
            if cls._instance is None:
                instance = super().__new__(cls)
                instance._pool = None
                instance._lock = asyncio.Lock()
                instance._loop = None
                cls._instance = instance

                logger.debug("DatabaseConnection singleton created")

            return cls._instance

    @classmethod
    async def _reset_for_tests(cls) -> None:
        """Reset singleton state between isolated async tests."""
        with cls._instance_lock:
            instance = cls._instance

        if instance is not None:
            try:
                await instance.shutdown()
            finally:
                with cls._instance_lock:
                    if cls._instance is instance:
                        cls._instance = None

    async def startup(
        self,
        connection_string: str,
        min_size: int = 1,
        max_size: int = 10,
        timeout: float = 10.0,
    ) -> None:
        """Initialize the asynchronous PostgreSQL connection pool."""
        async with self._lock:
            if self._pool is not None:
                raise RuntimeError("Database connection pool already initialized")

            pool = AsyncConnectionPool(
                conninfo=connection_string,
                min_size=min_size,
                max_size=max_size,
                timeout=timeout,
                check=AsyncConnectionPool.check_connection,
                name="guardian-layer-pool",
                open=False,
            )

            try:
                await pool.open(wait=True)
            except Exception as exc:
                try:
                    await pool.close()
                except Exception:
                    logger.exception("Failed to clean up connection pool")

                raise RuntimeError(
                    "Failed to initialize PostgreSQL connection pool"
                ) from exc

            self._pool = pool
            self._loop = asyncio.get_running_loop()
            logger.info("Async PostgreSQL connection pool initialized")

    @property
    def pool(self) -> AsyncConnectionPool[Any]:
        """Return the active pool owned by the current event loop."""
        pool = self._pool

        if pool is None:
            raise RuntimeError("Database pool is not initialized")

        if self._loop is not asyncio.get_running_loop():
            raise RuntimeError("Database pool belongs to a different event loop")

        return pool

    async def shutdown(self) -> None:
        """Close the asynchronous PostgreSQL connection pool."""
        async with self._lock:
            pool = self._pool

            if pool is None:
                return

            if self._loop is not asyncio.get_running_loop():
                raise RuntimeError(
                    "Database pool must be closed on its owning event loop"
                )

            try:
                await pool.close()
            finally:
                self._pool = None
                self._loop = None

            logger.info("Async PostgreSQL connection pool closed")


@asynccontextmanager
async def lifespan(
    connection_string: str,
    min_size: int = 1,
    max_size: int = 10,
    timeout: float = 10.0,
) -> AsyncIterator[None]:
    """Start and stop the database pool with the application."""
    database = DatabaseConnection()

    await database.startup(
        connection_string=connection_string,
        min_size=min_size,
        max_size=max_size,
        timeout=timeout,
    )

    try:
        logger.info("Application database lifespan started")
        yield
    finally:
        try:
            await database.shutdown()
        finally:
            logger.info("Application database lifespan exited")
