import asyncio
import logging
from pathlib import Path

from mcp import ClientSession
from mcp.client.sse import sse_client
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from guardian_layer.consumer_producer.kafka_producer.synchronizer import (
    DeviceMetadataSynchronizer,
)
from guardian_layer.consumer_producer.kafka_producer.utils.logger import get_logger
from guardian_layer.consumer_producer.kafka_producer.utils.settings import get_settings

settings = get_settings()


logger = get_logger(__name__)


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(Exception),
    before_sleep=before_sleep_log(logger, logging.WARNING),
)
async def main() -> None:
    mcp_url = settings.mcp.url

    bootstrap_servers = settings.kafka.bootstrap_servers
    cache_path = Path(settings.data_path)

    logger.info("Starting device metadata synchronizer")
    try:
        async with sse_client(mcp_url) as (
            read_stream,
            write_stream,
        ):
            async with ClientSession(
                read_stream,
                write_stream,
            ) as session:
                await session.initialize()

                synchronizer = DeviceMetadataSynchronizer(
                    call_tool=session.call_tool,
                    bootstrap_servers=bootstrap_servers,
                    cache_path=str(cache_path),
                )

                await synchronizer.run()
    except BaseExceptionGroup as group:
        for sub in group.exceptions:
            logger.error("Underlying error: %r", sub)
        raise
    except Exception:
        logger.exception("Error in metadata synchronizer")
        raise


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Metadata synchronizer stopped")
