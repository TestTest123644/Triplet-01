import asyncio
import logging
from pathlib import Path

from mcp import ClientSession
from mcp.client.sse import sse_client

from guardian_layer.consumer_producer.kafka_producer.synchronizer import (
    SYNC_INTERVAL,
    DeviceMetadataSynchronizer,
)
from guardian_layer.consumer_producer.kafka_producer.utils.settings import get_settings
from guardian_layer.utils.logger import enable, get_logger

enable("INFO")
logger = get_logger("guardian_layer.consumer_producer.kafka_producer.main")

# Suppress noisy internal tracebacks from HTTP / SSE clients when twin MCP is stopped
logging.getLogger("mcp.client.sse").setLevel(logging.CRITICAL)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

RETRY_DELAY_SECONDS = 3


def _extract_error_message(exc: BaseException) -> str:
    if hasattr(exc, "exceptions"):  # Python 3.11+ ExceptionGroup
        sub_msgs = [_extract_error_message(e) for e in exc.exceptions]
        return "; ".join(sub_msgs)
    msg = str(exc).strip()
    return msg if msg else exc.__class__.__name__


async def run_producer_loop() -> None:
    settings = get_settings()
    mcp_url = settings.mcp.url
    bootstrap_servers = settings.kafka.bootstrap_servers
    cache_path = Path(settings.data_path)

    logger.info("Starting device metadata synchronizer (target MCP: %s)", mcp_url)

    synchronizer = DeviceMetadataSynchronizer(
        bootstrap_servers=bootstrap_servers,
        cache_path=str(cache_path),
    )

    while True:
        try:
            async with sse_client(mcp_url) as (read_stream, write_stream):
                async with ClientSession(read_stream, write_stream) as session:
                    await session.initialize()
                    logger.info("Connected to MCP server at %s", mcp_url)
                    await synchronizer.sync(session.call_tool)
        except asyncio.CancelledError:
            logger.info("Metadata synchronizer stopped")
            break
        except Exception as exc:
            err_msg = _extract_error_message(exc)
            logger.error(
                "Twin MCP unavailable at %s (%s). Retrying in %ds...",
                mcp_url,
                err_msg,
                RETRY_DELAY_SECONDS,
            )
            await asyncio.sleep(RETRY_DELAY_SECONDS)
            continue

        await asyncio.sleep(SYNC_INTERVAL)


def main() -> None:
    try:
        asyncio.run(run_producer_loop())
    except KeyboardInterrupt:
        logger.info("Metadata synchronizer interrupted and stopped")


if __name__ == "__main__":
    main()
