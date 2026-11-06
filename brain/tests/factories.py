"""Valid-by-default builders for domain objects, shared by later tasks."""

from datetime import datetime, timedelta, timezone

from city_brain.domain.types import (
    CanonicalUnit,
    DeviceStatus,
    Observation,
    SensorKind,
    TrustState,
    Verdict,
)

T0 = datetime(2026, 10, 17, 12, 0, 0, tzinfo=timezone.utc)


def make_observation(**overrides) -> Observation:
    fields = dict(
        reading_id="r-1",
        sig_hash="0123456789abcdef",
        device_id="z1-wl1",
        zone_id="zone-1",
        kind=SensorKind.WATER_LEVEL,
        seq=1,
        sim_time=T0,
        value=5.0,
        unit=CanonicalUnit.M,
        device_status=DeviceStatus.OK,
        flags=frozenset(),
    )
    fields.update(overrides)
    return Observation(**fields)


def make_verdict(**overrides) -> Verdict:
    fields = dict(
        verdict_id="v-1",
        reading_id="r-1",
        sig_hash="0123456789abcdef",
        device_id="z1-wl1",
        state=TrustState.TRUSTED,
        trust=1.0,
        reasons=frozenset(),
        issued_at=T0,
        expires_at=T0 + timedelta(seconds=30),
    )
    fields.update(overrides)
    return Verdict(**fields)
