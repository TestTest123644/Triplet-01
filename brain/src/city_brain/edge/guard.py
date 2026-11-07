"""Edge guard: reason-coded validation of raw readings and Guardian verdicts (spec 5.1, 5.3, 7.3)."""

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from jsonschema import Draft7Validator, FormatChecker

from city_brain.domain.ids import sig_hash
from city_brain.domain.types import (
    DeviceProfile,
    GuardianContainment,
    GuardianHeartbeat,
    GuardianNotice,
    GuardianRevocation,
    Observation,
    Verdict,
)
from city_brain.edge.profiles import (
    BadValue,
    GuardianProfile,
    ProfileError,
    TwinProfile,
    UnknownVerdictState,
)
from city_brain.edge.signatures import KeyRing, verify

MAX_BYTES = 4096
MAX_SEQ = 2**53  # the largest count every JSON reader keeps exact
MIN_YEAR, MAX_YEAR = 1900, 9000  # far from datetime's limits: sim-time arithmetic must not overflow


@dataclass(frozen=True)
class Accepted:
    observation: Observation


@dataclass(frozen=True)
class Rejected:
    code: str
    detail: str
    device_id: str | None = None


@dataclass(frozen=True)
class AcceptedVerdict:
    verdict: Verdict


@dataclass(frozen=True)
class AcceptedGuardianEvent:
    notice: GuardianNotice


def _device_id(obj: Any, field: str = "device_id") -> str | None:
    if isinstance(obj, Mapping) and isinstance(obj.get(field), str):
        return obj[field]
    return None


def _first_schema_error(validator: Draft7Validator, obj: Any) -> str | None:
    errors = sorted(validator.iter_errors(obj), key=lambda e: [str(p) for p in e.absolute_path])
    if not errors:
        return None
    err = errors[0]
    path = ".".join(str(p) for p in err.absolute_path) or "$"
    return f"{path}: {err.message}"


def _finite(x: Any) -> bool:
    """True for a number that converts to a finite float (huge ints and NaN/inf are not)."""
    try:
        return math.isfinite(float(x))
    except (OverflowError, ValueError, TypeError):
        return False


def _bad_position(fields: Mapping[str, Any]) -> str | None:
    """Why `seq` or the mapped timestamp cannot be placed on the sim timeline, or None."""
    seq = fields.get("seq")
    if type(seq) is not int or not 0 <= seq <= MAX_SEQ:
        return f"seq: {seq!r:.40} is not an integer in 0..2**53"
    sim_time = fields.get("sim_time")
    if not isinstance(sim_time, datetime) or sim_time.tzinfo is None:
        return f"sim_time: {sim_time!r:.40} is not an aware timestamp"
    if not MIN_YEAR <= sim_time.year <= MAX_YEAR:
        return f"sim_time: {sim_time!r:.40} is out of range"
    return None


def _parse(raw: bytes) -> tuple[Any, str | None]:
    try:
        return json.loads(raw), None
    except (ValueError, UnicodeDecodeError, RecursionError) as exc:
        return None, str(exc)


class ReadingGuard:
    """Validates one raw `twin.sensors` message. Holds (does not copy) its keyring and registry."""

    def __init__(self, profile: TwinProfile, keyring: KeyRing, registry: Mapping[str, DeviceProfile]):
        self._profile = profile
        self._keyring = keyring
        self._registry = registry
        self._validator = Draft7Validator(profile.schema, format_checker=FormatChecker())

    def check(self, raw: bytes) -> Accepted | Rejected:
        if len(raw) > MAX_BYTES:
            return Rejected("OVERSIZED", f"{len(raw)} bytes exceeds {MAX_BYTES}")
        obj, err = _parse(raw)
        if err is not None:
            return Rejected("MALFORMED_JSON", err)
        device_id = _device_id(obj, self._profile.device_id_field)
        problem = _first_schema_error(self._validator, obj)
        if problem is not None:
            return Rejected("SCHEMA_VIOLATION", problem, device_id)
        if not isinstance(obj, Mapping):
            return Rejected("SCHEMA_VIOLATION", "reading is not an object", device_id)
        mapping_error = None
        try:
            fields = self._profile.map_reading(obj)
        except BadValue as exc:
            return Rejected("SCHEMA_VIOLATION", str(exc), device_id)
        except ProfileError as exc:
            mapping_error = exc
        device = self._registry.get(device_id)
        if device is None:
            return Rejected("UNKNOWN_DEVICE", f"device '{device_id}' is not in the registry", device_id)
        if mapping_error is not None:
            return Rejected("TYPE_MISMATCH", str(mapping_error), device_id)
        problem = _bad_position(fields)
        if problem is not None:
            return Rejected("SCHEMA_VIOLATION", problem, device_id)
        if fields["kind"] != device.kind or fields["unit"] != device.unit:
            return Rejected(
                "TYPE_MISMATCH",
                f"{fields['kind']}/{fields['unit']} does not match registry {device.kind}/{device.unit}",
                device_id)
        key = self._keyring.get(device_id)
        if key is None or not verify(obj, key):
            return Rejected("BAD_SIGNATURE", "signature does not verify with the device key", device_id)
        flags = set()
        value = fields["value"]
        if value >= device.max_val:
            flags.add("AT_CEILING")
        if value < device.min_val or value > device.max_val:
            flags.add("OUT_OF_RANGE")
        try:
            observation = Observation(sig_hash=sig_hash(obj["sig"]), flags=frozenset(flags), **fields)
        except ValueError as exc:  # fields that mapped but do not form an Observation
            return Rejected("SCHEMA_VIOLATION", str(exc), device_id)
        return Accepted(observation)


class VerdictGuard:
    """Validates one raw `guardian.verdicts` message. Expiry is the ledger's job, not the guard's."""

    def __init__(self, profile: GuardianProfile):
        self._profile = profile
        schema = dict(profile.verdict_schema)
        # An unsigned verdict is a BAD_SIGNATURE (checked last), not a SCHEMA_VIOLATION.
        schema["required"] = [f for f in schema.get("required", []) if f != "sig"]
        self._validator = Draft7Validator(schema, format_checker=FormatChecker())

    def check(self, raw: bytes) -> AcceptedVerdict | Rejected:
        if len(raw) > MAX_BYTES:
            return Rejected("OVERSIZED", f"{len(raw)} bytes exceeds {MAX_BYTES}")
        obj, err = _parse(raw)
        if err is not None:
            return Rejected("MALFORMED_JSON", err)
        device_id = _device_id(obj)
        problem = _first_schema_error(self._validator, obj)
        if problem is not None:
            return Rejected("SCHEMA_VIOLATION", problem, device_id)
        if obj.get("trust") is not None and not _finite(obj["trust"]):
            return Rejected("SCHEMA_VIOLATION", f"trust: {obj['trust']!r:.40} is not finite", device_id)
        try:
            state = self._profile.map_state(obj["state"])
        except UnknownVerdictState as exc:
            return Rejected("UNKNOWN_VERDICT_STATE", str(exc), device_id)
        try:
            key = self._profile.public_key_b64
        except ProfileError as exc:
            return Rejected("BAD_SIGNATURE", str(exc), device_id)
        if not verify(obj, key):
            return Rejected("BAD_SIGNATURE", "verdict signature missing or invalid", device_id)
        try:
            verdict = Verdict(
                verdict_id=obj["verdict_id"], reading_id=obj["reading_id"], sig_hash=obj["sig_hash"],
                device_id=obj["device_id"], state=state, trust=obj.get("trust"),
                reasons=frozenset(obj["reasons"]),
                issued_at=datetime.fromisoformat(obj["issued_at"]),
                expires_at=datetime.fromisoformat(obj["expires_at"]))
        except (ValueError, TypeError) as exc:  # e.g. RFC 3339 forms fromisoformat cannot read
            return Rejected("SCHEMA_VIOLATION", f"invalid verdict: {exc}", device_id)
        return AcceptedVerdict(verdict)


class GuardianEventGuard:
    """Validates one raw `guardian.events` message: heartbeat, containment or revocation (spec 8.9)."""

    def __init__(self, profile: GuardianProfile):
        schema = dict(profile.event_schema)
        self._profile = profile
        self._validator = Draft7Validator(schema, format_checker=FormatChecker())

    def check(self, raw: bytes) -> AcceptedGuardianEvent | Rejected:
        if len(raw) > MAX_BYTES:
            return Rejected("OVERSIZED", f"{len(raw)} bytes exceeds {MAX_BYTES}")
        obj, err = _parse(raw)
        if err is not None:
            return Rejected("MALFORMED_JSON", err)
        device_id = _device_id(obj)
        problem = _first_schema_error(self._validator, obj)
        if problem is not None:
            return Rejected("SCHEMA_VIOLATION", problem, device_id)
        try:
            key = self._profile.public_key_b64
        except ProfileError as exc:
            return Rejected("BAD_SIGNATURE", str(exc), device_id)
        if not verify(obj, key):
            return Rejected("BAD_SIGNATURE", "event signature missing or invalid", device_id)
        try:
            notice = _notice(obj)
        except (ValueError, TypeError) as exc:  # timestamps the schema's format checker let through
            return Rejected("SCHEMA_VIOLATION", f"invalid guardian event: {exc}", device_id)
        return AcceptedGuardianEvent(notice)


def _notice(obj: Mapping[str, Any]) -> GuardianNotice:
    issued_at = datetime.fromisoformat(obj["issued_at"])
    kind = obj["kind"]
    if kind == "heartbeat":
        return GuardianHeartbeat(
            evaluated_through=datetime.fromisoformat(obj["evaluated_through"]), issued_at=issued_at)
    if kind == "containment":
        return GuardianContainment(
            device_id=obj["device_id"], action=obj["action"],
            effective_at=datetime.fromisoformat(obj["effective_at"]),
            reasons=frozenset(obj["reasons"]), issued_at=issued_at)
    return GuardianRevocation(verdict_id=obj["verdict_id"], issued_at=issued_at)
