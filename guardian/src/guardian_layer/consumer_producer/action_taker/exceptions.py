class InvalidMessage(ValueError):
    """A Kafka message that cannot be parsed; it is logged, committed and skipped."""


class McpUnavailable(RuntimeError):
    """MCP still unreachable after the retries; the offset must not be committed."""
