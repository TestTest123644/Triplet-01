import asyncio
import threading
from typing import Any

from fastmcp import Client

from guardian_layer.consumer_producer.kafka_consumer.utils.logger import get_logger

logger = get_logger(module_name=__name__)


class McpError(RuntimeError):
    """MCP server unreachable, timed out, or the tool returned an error."""


class McpToolClient:
    """Blocking facade: one background event loop, one short session per call."""

    def __init__(self, url: str, timeout_s: float = 3.0):
        self._url = url
        self._timeout = timeout_s
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever, daemon=True, name="mcp-client"
        )
        self._thread.start()

    async def _call(self, name: str, args: dict[str, Any]) -> Any:
        async with Client(self._url, timeout=self._timeout) as client:
            result = await asyncio.wait_for(client.call_tool(name, args), self._timeout)
            return result.data

    def call_tool(self, name: str, args: dict[str, Any]) -> Any:
        fut = asyncio.run_coroutine_threadsafe(self._call(name, args), self._loop)
        try:
            return fut.result(self._timeout * 2)
        except Exception as e:  # noqa: BLE001
            fut.cancel()
            raise McpError(f"call to '{name}' failed: {e!r}") from e

    def get_device_key(self, device_id: str) -> str | None:
        value = self.call_tool("get_device_key", {"device_id": device_id})
        if value is None or str(value).strip() in ("", "null", "None"):
            return None
        return str(value).strip()

    def close(self) -> None:
        self._loop.call_soon_threadsafe(self._loop.stop)
