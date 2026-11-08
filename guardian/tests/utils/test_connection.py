import asyncio
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from guardian_layer.utils.connection import (
    DatabaseConnection,
    lifespan,
)

DATABASE_URL = "postgresql://localhost:5432/test_db"


@pytest_asyncio.fixture
async def database():
    """Provide a clean Singleton for each test."""
    await DatabaseConnection._reset_for_tests()

    db = DatabaseConnection()

    yield db

    await DatabaseConnection._reset_for_tests()


@pytest.fixture
def mock_pool():
    """Mock AsyncConnectionPool without PostgreSQL."""
    with patch("guardian_layer.utils.connection.AsyncConnectionPool") as pool_class:
        pool = pool_class.return_value
        pool.open = AsyncMock()
        pool.close = AsyncMock()

        yield pool_class, pool


# --------------------------------------------------
# SINGLETON
# --------------------------------------------------


@pytest.mark.asyncio
async def test_singleton(database):
    """Verify all instances share the same manager."""
    second = DatabaseConnection()

    assert database is second


@pytest.mark.asyncio
async def test_singleton_reset(database):
    """Verify reset creates a new instance."""
    first = database

    await DatabaseConnection._reset_for_tests()

    second = DatabaseConnection()

    assert first is not second


# --------------------------------------------------
# STARTUP
# --------------------------------------------------


@pytest.mark.asyncio
async def test_startup_success(database, mock_pool):
    """Verify pool initialization."""
    pool_class, pool = mock_pool

    await database.startup(DATABASE_URL)

    pool_class.assert_called_once()
    pool.open.assert_awaited_once_with(wait=True)

    assert database.pool is pool
    assert database._loop is asyncio.get_running_loop()


@pytest.mark.asyncio
async def test_startup_custom_settings(database, mock_pool):
    """Verify custom pool configuration."""
    pool_class, _ = mock_pool

    await database.startup(
        DATABASE_URL,
        min_size=2,
        max_size=20,
        timeout=5.0,
    )

    kwargs = pool_class.call_args.kwargs

    assert kwargs["conninfo"] == DATABASE_URL
    assert kwargs["min_size"] == 2
    assert kwargs["max_size"] == 20
    assert kwargs["timeout"] == 5.0
    assert kwargs["name"] == "guardian-layer-pool"
    assert kwargs["open"] is False
    assert kwargs["check"] is pool_class.check_connection


@pytest.mark.asyncio
async def test_startup_twice(database, mock_pool):
    """Reject duplicate pool initialization."""
    pool_class, _ = mock_pool

    await database.startup(DATABASE_URL)

    with pytest.raises(
        RuntimeError,
        match="already initialized",
    ):
        await database.startup(DATABASE_URL)

    pool_class.assert_called_once()


@pytest.mark.asyncio
async def test_startup_failure(database, mock_pool):
    """Verify failed startup closes the temporary pool."""
    _, pool = mock_pool

    pool.open.side_effect = RuntimeError("Connection failed")

    with pytest.raises(
        RuntimeError,
        match="Failed to initialize",
    ) as exc_info:
        await database.startup(DATABASE_URL)

    pool.close.assert_awaited_once()

    assert isinstance(exc_info.value.__cause__, RuntimeError)
    assert database._pool is None
    assert database._loop is None


@pytest.mark.asyncio
async def test_startup_cleanup_failure(database, mock_pool):
    """Preserve startup error if pool cleanup fails."""
    _, pool = mock_pool

    pool.open.side_effect = RuntimeError("Open failed")
    pool.close.side_effect = RuntimeError("Close failed")

    with patch("guardian_layer.utils.connection.logger") as mock_logger:
        with pytest.raises(RuntimeError) as exc_info:
            await database.startup(DATABASE_URL)

    assert str(exc_info.value.__cause__) == "Open failed"

    mock_logger.exception.assert_called_once_with("Failed to clean up connection pool")


# --------------------------------------------------
# POOL ACCESS
# --------------------------------------------------


@pytest.mark.asyncio
async def test_pool_before_startup(database):
    """Reject access to an uninitialized pool."""
    with pytest.raises(
        RuntimeError,
        match="not initialized",
    ):
        _ = database.pool


@pytest.mark.asyncio
async def test_pool_after_startup(database, mock_pool):
    """Return the active async connection pool."""
    _, pool = mock_pool

    await database.startup(DATABASE_URL)

    assert database.pool is pool


@pytest.mark.asyncio
async def test_pool_wrong_event_loop(database, mock_pool):
    """Reject access from a different event loop."""
    await database.startup(DATABASE_URL)

    other_loop = asyncio.new_event_loop()

    try:
        database._loop = other_loop

        with pytest.raises(
            RuntimeError,
            match="different event loop",
        ):
            _ = database.pool
    finally:
        database._loop = asyncio.get_running_loop()
        other_loop.close()


# --------------------------------------------------
# SHUTDOWN
# --------------------------------------------------


@pytest.mark.asyncio
async def test_shutdown_success(database, mock_pool):
    """Verify asynchronous pool closure."""
    _, pool = mock_pool

    await database.startup(DATABASE_URL)
    await database.shutdown()

    pool.close.assert_awaited_once()

    assert database._pool is None
    assert database._loop is None


@pytest.mark.asyncio
async def test_shutdown_without_startup(database, mock_pool):
    """Allow shutdown without an active pool."""
    pool_class, _ = mock_pool

    await database.shutdown()

    pool_class.assert_not_called()


@pytest.mark.asyncio
async def test_shutdown_twice(database, mock_pool):
    """Prevent closing the pool multiple times."""
    _, pool = mock_pool

    await database.startup(DATABASE_URL)

    await database.shutdown()
    await database.shutdown()

    pool.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_shutdown_failure(database, mock_pool):
    """Clear pool state when close raises an exception."""
    _, pool = mock_pool

    await database.startup(DATABASE_URL)

    pool.close.side_effect = RuntimeError("Close failed")

    with pytest.raises(RuntimeError, match="Close failed"):
        await database.shutdown()

    assert database._pool is None
    assert database._loop is None


@pytest.mark.asyncio
async def test_shutdown_wrong_event_loop(database, mock_pool):
    """Reject shutdown from a non-owning event loop."""
    _, pool = mock_pool

    await database.startup(DATABASE_URL)

    other_loop = asyncio.new_event_loop()

    try:
        database._loop = other_loop

        with pytest.raises(
            RuntimeError,
            match="owning event loop",
        ):
            await database.shutdown()

        pool.close.assert_not_awaited()
    finally:
        database._loop = asyncio.get_running_loop()
        other_loop.close()


@pytest.mark.asyncio
async def test_restart_after_shutdown(database, mock_pool):
    """Allow initialization after shutdown."""
    pool_class, pool = mock_pool

    await database.startup(DATABASE_URL)
    await database.shutdown()
    await database.startup(DATABASE_URL)

    assert pool_class.call_count == 2
    assert pool.open.await_count == 2


# --------------------------------------------------
# APPLICATION LIFESPAN
# --------------------------------------------------


@pytest.mark.asyncio
async def test_lifespan_success(database, mock_pool):
    """Initialize and close the pool automatically."""
    _, pool = mock_pool

    async with lifespan(DATABASE_URL):
        assert database.pool is pool
        pool.open.assert_awaited_once()
        pool.close.assert_not_awaited()

    pool.close.assert_awaited_once()

    assert database._pool is None


@pytest.mark.asyncio
async def test_lifespan_custom_settings(database, mock_pool):
    """Forward configuration through the lifecycle."""
    pool_class, _ = mock_pool

    async with lifespan(
        DATABASE_URL,
        min_size=3,
        max_size=15,
        timeout=7.0,
    ):
        kwargs = pool_class.call_args.kwargs

        assert kwargs["min_size"] == 3
        assert kwargs["max_size"] == 15
        assert kwargs["timeout"] == 7.0


@pytest.mark.asyncio
async def test_lifespan_application_failure(database, mock_pool):
    """Always close the pool after application failure."""
    _, pool = mock_pool

    with pytest.raises(RuntimeError, match="Application failed"):
        async with lifespan(DATABASE_URL):
            raise RuntimeError("Application failed")

    pool.close.assert_awaited_once()

    assert database._pool is None
    assert database._loop is None


@pytest.mark.asyncio
async def test_lifespan_startup_failure(database, mock_pool):
    """Prevent entering lifespan when startup fails."""
    _, pool = mock_pool

    pool.open.side_effect = RuntimeError("Connection failed")

    with pytest.raises(RuntimeError, match="Failed to initialize"):
        async with lifespan(DATABASE_URL):
            pytest.fail("Lifespan should not start")

    pool.close.assert_awaited_once()


# --------------------------------------------------
# LOGGING
# --------------------------------------------------


@pytest.mark.asyncio
async def test_startup_logging(database, mock_pool):
    """Verify successful startup logging."""
    with patch("guardian_layer.utils.connection.logger") as mock_logger:
        await database.startup(DATABASE_URL)

    mock_logger.info.assert_any_call("Async PostgreSQL connection pool initialized")


@pytest.mark.asyncio
async def test_shutdown_logging(database, mock_pool):
    """Verify successful shutdown logging."""
    await database.startup(DATABASE_URL)

    with patch("guardian_layer.utils.connection.logger") as mock_logger:
        await database.shutdown()

    mock_logger.info.assert_any_call("Async PostgreSQL connection pool closed")


@pytest.mark.asyncio
async def test_lifespan_logging(database, mock_pool):
    """Verify lifecycle log messages."""
    with patch("guardian_layer.utils.connection.logger") as mock_logger:
        async with lifespan(DATABASE_URL):
            pass

    mock_logger.info.assert_any_call("Application database lifespan started")

    mock_logger.info.assert_any_call("Application database lifespan exited")
