"""MCP client helper for external mocks (Layer 2 Brain and Layer 3 Guardian).

Strict isolation: zero imports from Layer 1 internal packages.
Source of truth is INTERFACE.md and context.md.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import json
import logging
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger("mocks.mcp_client")


def run_sync(coro):
    """Run an async coroutine synchronously, whether or not an event loop is running."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop is None:
        return asyncio.run(coro)
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(asyncio.run, coro).result()


class McpCaller:
    """Invokes Model Context Protocol (MCP) tools over SSE or in-process session."""

    def __init__(
        self,
        base_url: str = "http://localhost:8000",
        token: Optional[str] = None,
        session: Optional[Any] = None,
        call_tool_fn: Optional[Callable[[str, Dict[str, Any]], Any]] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self._session = session
        self._call_tool_fn = call_tool_fn
        self._exit_stack: Optional[contextlib.AsyncExitStack] = None

    async def connect(self) -> None:
        """Establish connection via official MCP SDK over /sse transport."""
        if self._session is not None or self._call_tool_fn is not None:
            return

        from mcp.client.session import ClientSession
        from mcp.client.sse import sse_client

        headers = {}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        self._exit_stack = contextlib.AsyncExitStack()
        read_stream, write_stream = await self._exit_stack.enter_async_context(
            sse_client(f"{self.base_url}/sse", headers=headers)
        )
        self._session = await self._exit_stack.enter_async_context(
            ClientSession(read_stream, write_stream)
        )
        await self._session.initialize()

    async def __aenter__(self) -> "McpCaller":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def close(self) -> None:
        """Close MCP client session.

        NOTE: AnyIO cancel scopes require that multiple SSE client sessions opened
        within the same asyncio task MUST be closed in reverse order of opening (LIFO).
        If closed out of order, anyio may raise RuntimeError: Cancel scope stack corrupted.
        This method catches such exceptions to prevent crashing teardown.
        """
        if self._exit_stack:
            try:
                await self._exit_stack.aclose()
            except Exception as e:
                logger.debug(f"Exception during McpCaller.close() (note anyio LIFO rule): {e}")
            self._session = None
            self._exit_stack = None

    async def call_tool_async(self, tool_name: str, args: Dict[str, Any]) -> Any:
        """Call MCP tool asynchronously."""
        if self._call_tool_fn is not None:
            res = self._call_tool_fn(tool_name, args)
            if asyncio.iscoroutine(res):
                return await res
            return res

        if self._session is None:
            await self.connect()

        call_res = await self._session.call_tool(tool_name, args)
        if hasattr(call_res, "content") and call_res.content:
            if len(call_res.content) == 1:
                first = call_res.content[0]
                if hasattr(first, "text"):
                    text = first.text
                    try:
                        return json.loads(text)
                    except Exception:
                        return text
            else:
                items = []
                for c in call_res.content:
                    t = getattr(c, "text", "")
                    try:
                        items.append(json.loads(t))
                    except Exception:
                        items.append(t)
                return items
        if hasattr(call_res, "structured_content"):
            return call_res.structured_content
        return call_res

    def call_tool(self, tool_name: str, args: Dict[str, Any]) -> Any:
        """Call MCP tool synchronously."""
        return run_sync(self.call_tool_async(tool_name, args))
