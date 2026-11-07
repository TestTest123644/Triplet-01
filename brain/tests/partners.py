"""Partner endpoints for tests that need a live Twin or Kafka."""
import os
import socket
from dataclasses import dataclass
from urllib.parse import urlsplit


@dataclass(frozen=True)
class PartnerEndpoint:
    marker: str
    env_var: str
    default: str

    def value(self) -> str:
        return os.environ.get(self.env_var, self.default)


_TWIN = PartnerEndpoint("twin_live", "BRAIN_TWIN_MCP", "http://localhost:8000/sse")
ENDPOINTS: dict[str, PartnerEndpoint] = {
    "twin_live": _TWIN,
    "triplet": PartnerEndpoint("triplet", _TWIN.env_var, _TWIN.default),
    "kafka": PartnerEndpoint("kafka", "BRAIN_KAFKA_BOOTSTRAP", "localhost:19092"),
}

_LABELS = {"twin_live": "Twin", "triplet": "Twin", "kafka": "Kafka"}


def _host_port(target: str) -> tuple[str, int]:
    parsed = urlsplit(target if "://" in target else f"//{target}")
    port = parsed.port or {"https": 443}.get(parsed.scheme, 80)
    return parsed.hostname or "localhost", port


def reachable(url_or_host_port: str, timeout_s: float = 0.5) -> bool:
    """TCP connect probe. Never raises."""
    try:
        host, port = _host_port(url_or_host_port)
        with socket.create_connection((host, port), timeout=timeout_s):
            return True
    except (OSError, ValueError):
        return False


def skip_reason(ep: PartnerEndpoint, value: str) -> str:
    return f"{_LABELS[ep.marker]} not reachable at {value} (${ep.env_var})"
