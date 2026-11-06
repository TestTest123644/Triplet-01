"""Shared test wiring that builds edge components from a mock Twin."""

from city_brain.edge.guard import ReadingGuard
from city_brain.edge.signatures import KeyRing


def guard_for_mock(twin) -> ReadingGuard:
    """A ReadingGuard whose registry and key ring come from the mock Twin."""
    registry = {}
    keyring = KeyRing()
    for raw in twin.list_devices():
        profile = twin.profile.map_device(raw)
        registry[profile.device_id] = profile
        keyring.set(profile.device_id, twin.get_device_key(profile.device_id))
    return ReadingGuard(twin.profile, keyring, registry)
