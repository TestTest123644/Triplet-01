"""In-process bus for standalone runs: one ordered lane shared by every logical stream."""

from collections import deque


class MemoryBus:
    def __init__(self) -> None:
        self._messages: deque[tuple[str, bytes]] = deque()

    def publish(self, stream: str, raw: bytes) -> None:
        self._messages.append((stream, raw))

    def drain(self) -> list[tuple[str, bytes]]:
        """Everything published since the last drain, in publish order."""
        out = list(self._messages)
        self._messages.clear()
        return out
