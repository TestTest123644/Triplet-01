import time
from collections.abc import Callable


class CachedMetadataProvider:
    """TTL cache around any lookup (Redis, Postgres, HTTP...)."""

    def __init__(self, fetch: Callable[[str], dict | None], ttl_s: int = 300):
        self._fetch = fetch
        self._ttl = ttl_s
        self._cache: dict[str, tuple[float, dict | None]] = {}

    def get(self, sensor_id: str) -> dict | None:
        now = time.monotonic()
        hit = self._cache.get(sensor_id)
        if hit and now - hit[0] < self._ttl:
            return hit[1]
        value = self._fetch(sensor_id)
        self._cache[sensor_id] = (now, value)
        return value


def fetch_metadata(sensor_id: str) -> dict | None:
    # TODO: replace with a real lookup (Redis / DB / REST)
    return {"site": "unknown"}
