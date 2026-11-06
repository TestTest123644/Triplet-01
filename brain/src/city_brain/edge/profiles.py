"""Partner profiles: the anti-corruption layer between partner wire formats and canonical types.

Every partner-specific fact is read from a YAML profile. The core only ever sees canonical
values (enum members, canonical units, canonical actuator states).
"""

import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any

import yaml

from city_brain.domain.types import (
    ActuatorCapability,
    CanonicalUnit,
    DeviceProfile,
    DeviceStatus,
    SensorKind,
    TrustState,
)

OBSERVED_KEYS = ("commit", "date", "by", "method")
OBSERVED_METHODS = ("code_read", "recording", "assumed")
_BRAIN_ROOT = Path(__file__).resolve().parents[3]
_RATIO = re.compile(r"ratio:(0|[1-9]\d{0,2})")
_PCT = re.compile(r"(\d{1,3})%")


class ProfileError(Exception):
    """A profile is malformed, or a partner value cannot be mapped."""


class StateNotAllowed(ProfileError):
    """A canonical actuator state is outside the capability's allowlist."""


class UnknownVerdictState(ProfileError):
    """A Guardian verdict state is not in the vocabulary (UNKNOWN_VERDICT_STATE)."""


@dataclass(frozen=True)
class ToolCall:
    tool: str
    args: dict[str, Any]


def _find(path_like: str, profile_path: Path) -> Path | None:
    """Resolve a profile-relative path against brain/ (the profile's grandparent), then the repo's brain/."""
    for base in (profile_path.parent.parent, _BRAIN_ROOT):
        candidate = base / path_like
        if candidate.is_file():
            return candidate
    return None


def _read_yaml(path: str | Path) -> tuple[dict[str, Any], Path]:
    p = Path(path)
    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ProfileError(f"cannot read profile {p}: {exc}") from exc
    if not isinstance(data, dict):
        raise ProfileError(f"profile {p} is not a mapping")
    return data, p


def _observed(data: Mapping[str, Any]) -> dict[str, Any]:
    block = data.get("observed")
    if not isinstance(block, Mapping):
        raise ProfileError("profile is missing the 'observed' block")
    for key in OBSERVED_KEYS:
        if block.get(key) in (None, ""):
            raise ProfileError(f"observed block is missing key '{key}'")
    if block["method"] not in OBSERVED_METHODS:
        raise ProfileError(f"observed.method '{block['method']}' is not one of {OBSERVED_METHODS}")
    assumed = list(block.get("assumed_groups") or [])
    for group in assumed:
        if group not in data:
            raise ProfileError(f"observed.assumed_groups names unknown group '{group}'")
    out = {k: (v.isoformat() if isinstance(v, date) else str(v)) for k, v in block.items()
           if k in OBSERVED_KEYS}
    out["assumed_groups"] = tuple(assumed)
    return out


def _unverified(data: Mapping[str, Any], observed: Mapping[str, Any]) -> tuple[str, ...]:
    if observed["method"] == "assumed":
        return tuple(k for k in data if k not in ("observed", "profile"))
    return tuple(observed["assumed_groups"])


def _require_groups(data: Mapping[str, Any], *groups: str) -> None:
    for g in groups:
        if not isinstance(data.get(g), Mapping):
            raise ProfileError(f"profile is missing group '{g}'")


def _enum(enum_cls, name: str, what: str):
    try:
        return enum_cls[name]
    except KeyError:
        pass
    try:
        return enum_cls(name)
    except ValueError as exc:
        raise ProfileError(f"{what} '{name}' is not a canonical {enum_cls.__name__}") from exc


def _frozen(mapping: Mapping[str, Any]) -> Mapping[str, Any]:
    return MappingProxyType(dict(mapping))


class TwinProfile:
    """Maps one Twin's wire format to canonical types and canonical commands to its MCP tools."""

    def __init__(self, data: dict[str, Any], schema: dict[str, Any]):
        self._data = data
        self.name = str(data.get("profile", ""))
        self.observed = _observed(data)
        _require_groups(data, "fields", "kinds", "units", "statuses", "actuators")
        self.schema = schema
        self.fields = _frozen(data["fields"])
        self.kinds = _frozen(data["kinds"])
        self.units = _frozen(data["units"])
        self.statuses = _frozen(data["statuses"])
        self.unit_scale = _frozen(data.get("unit_scale") or {})
        self.actuators = _frozen(data["actuators"])
        self.inventory = _frozen(data.get("inventory") or {})
        self.registry = _frozen(data.get("registry") or {})
        self.unverified_groups = _unverified(data, self.observed)
        # canonical value of each partner field name, for validation of field maps
        self._canon_to_partner = {v: k for k, v in self.fields.items()}
        for needed in ("device_id", "kind", "value", "unit", "zone_id", "sim_time", "seq", "device_status"):
            if needed not in self._canon_to_partner:
                raise ProfileError(f"fields map has no partner field for '{needed}'")

    @classmethod
    def load(cls, path: str | Path) -> "TwinProfile":
        data, p = _read_yaml(path)
        _observed(data)  # fail on a missing or bad observed block before anything else
        schema: dict[str, Any] = {}
        if data.get("schema"):
            sp = _find(str(data["schema"]), p)
            if sp is None:
                raise ProfileError(f"schema file '{data['schema']}' not found for profile {p}")
            schema = json.loads(sp.read_text(encoding="utf-8"))
        return cls(data, schema)

    # ----- allowlist -----

    @property
    def allowlist(self) -> dict[str, frozenset[str]]:
        """Canonical states per capability (`ratio` stands for `ratio:<0..100>`); dispatch has none."""
        out: dict[str, frozenset[str]] = {}
        for cap, spec in self.actuators.items():
            if "states" in spec:
                out[cap] = frozenset(spec["states"])
        return out

    def _states(self, capability: str) -> frozenset[str]:
        out: set[str] = set()
        for s in self.allowlist.get(capability, ()):
            if s == "ratio":
                out.update(f"ratio:{n}" for n in range(101))
            else:
                out.add(s)
        return frozenset(out)

    # ----- readings and devices -----

    def _lookup(self, table: Mapping[str, Any], key: Any, what: str) -> Any:
        try:
            return table[key]
        except (KeyError, TypeError) as exc:
            raise ProfileError(f"unknown {what} '{key}'") from exc

    def map_reading(self, raw: Mapping[str, Any]) -> dict[str, Any]:
        """Canonical fields and values for one partner reading. Never touches the signature."""
        out: dict[str, Any] = {}
        for partner, canon in self.fields.items():
            if partner in raw:
                out[canon] = raw[partner]
        for needed in ("kind", "unit", "value", "zone_id", "device_status"):
            if needed not in out:
                raise ProfileError(f"reading is missing '{self._canon_to_partner[needed]}'")
        partner_unit = out["unit"]
        out["kind"] = _enum(SensorKind, self._lookup(self.kinds, out["kind"], "device kind"), "kind")
        out["unit"] = _enum(CanonicalUnit, self._lookup(self.units, partner_unit, "unit"), "unit")
        out["device_status"] = _enum(
            DeviceStatus, self._lookup(self.statuses, out["device_status"], "status"), "status")
        out["value"] = float(out["value"]) * float(self.unit_scale.get(partner_unit, 1.0))
        loc = out["zone_id"]
        if isinstance(loc, Mapping):
            if "zone_id" not in loc:
                raise ProfileError("location object has no zone_id")
            loc = loc["zone_id"]
        out["zone_id"] = loc
        if "sim_time" in out and isinstance(out["sim_time"], str):
            try:
                out["sim_time"] = datetime.fromisoformat(out["sim_time"])
            except ValueError as exc:
                raise ProfileError(f"bad timestamp '{out['sim_time']}'") from exc
        return out

    def _capability_for(self, device_type: str) -> str | None:
        for cap, spec in self.actuators.items():
            if spec.get("device_type") == device_type:
                return cap
        return None

    def actuator_state_with_flag(self, device_raw: Mapping[str, Any]) -> tuple[str | None, bool]:
        """(canonical actuator state, flagged). Valves mirror the Twin: "NN%" strings are float-parsed and clamped; other unknowns are `closed`, flagged."""
        cap = self._capability_for(device_raw.get("device_type"))
        if cap is None:
            return None, False
        spec = self.actuators[cap]
        partner = (device_raw.get("actuator_state") or {}).get(spec["state_field"])
        for canon, wire in spec["states"].items():
            if canon != "ratio" and wire == partner:
                return canon, False
        if cap == "valve" and "ratio" in spec["states"]:
            # Mirror the Twin (handle_set_valve): a string ending in '%' is parsed with float()
            # and clamped to 0..1; any other unknown string leaves the valve closed.
            if isinstance(partner, str) and partner.endswith("%"):
                try:
                    pct = float(partner.rstrip("%"))
                except ValueError:
                    pct = math.nan
                if not math.isnan(pct):
                    ratio = max(0.0, min(1.0, pct / 100.0))
                    exact = _PCT.fullmatch(partner) is not None and int(partner[:-1]) <= 100
                    return f"ratio:{round(100 * ratio)}", not exact
            return "closed", True
        return None, True

    def canonical_actuator_state(self, device_raw: Mapping[str, Any]) -> str | None:
        return self.actuator_state_with_flag(device_raw)[0]

    def map_device(self, raw: Mapping[str, Any]) -> DeviceProfile:
        """Canonical DeviceProfile from one registry (list_devices) entry."""
        try:
            device_id, zone_id = raw["device_id"], raw["zone_id"]
            partner_unit = raw["unit"]
            min_val, max_val = raw["min_val"], raw["max_val"]
        except KeyError as exc:
            raise ProfileError(f"device entry is missing {exc}") from exc
        scale = float(self.unit_scale.get(partner_unit, 1.0))
        cap = self._capability_for(raw.get("device_type"))
        actuator = None
        if cap is not None:
            actuator = ActuatorCapability(
                capability=cap, target=device_id, states=self._states(cap),
                current_state=self.canonical_actuator_state(raw))
        return DeviceProfile(
            device_id=device_id,
            zone_id=zone_id,
            kind=_enum(SensorKind, self._lookup(self.kinds, raw.get("device_type"), "device kind"), "kind"),
            unit=_enum(CanonicalUnit, self._lookup(self.units, partner_unit, "unit"), "unit"),
            min_val=float(min_val) * scale,
            max_val=float(max_val) * scale,
            status=_enum(DeviceStatus, self._lookup(self.statuses, raw.get("status"), "status"), "status"),
            actuator=actuator,
        )

    # ----- commands -----

    def to_tool_call(self, capability: str, target: str, state: str) -> ToolCall:
        """The partner MCP call for a canonical (capability, target, state)."""
        spec = self.actuators.get(capability)
        if spec is None:
            raise ProfileError(f"unknown capability '{capability}'")
        if "states" not in spec:  # dispatch: the state is the incident type
            args = {"zone_id": target}
            args.update({k: v.format(incident_type=state) for k, v in spec.get("args", {}).items()})
            return ToolCall(spec["tool"], args)
        states = spec["states"]
        m = _RATIO.fullmatch(state)
        if m and "ratio" in states and int(m.group(1)) <= 100:
            wire = states["ratio"].format(pct=int(m.group(1)))
        elif state in states and state != "ratio":
            wire = states[state]
        else:
            raise StateNotAllowed(f"state '{state}' is not allowed for capability '{capability}'")
        return ToolCall(spec["tool"], {"device_id": target, "target_state": wire})

    def accepts_partner_state(self, capability: str, partner_state: str) -> bool:
        """The contract: whether this wire state is a documented, valid state for the capability.

        The real Twin is looser (any '%' string, even "150%", is accepted and clamped), and the
        mock's `accept_unknown_states` defect reproduces that; this method deliberately does not.
        """
        spec = self.actuators.get(capability)
        if spec is None:
            return False
        if "states" not in spec:
            return True
        states = spec["states"]
        if partner_state in [w for c, w in states.items() if c != "ratio"]:
            return True
        return "ratio" in states and _PCT.fullmatch(partner_state) is not None


class GuardianProfile:
    """Maps the Guardian's verdict vocabulary to TrustState and holds its public key source."""

    def __init__(self, data: dict[str, Any], path: Path, public_key_b64: str | None):
        self._data = data
        self._path = path
        self._key_override = public_key_b64
        self.name = str(data.get("profile", ""))
        self.observed = _observed(data)
        _require_groups(data, "vocabulary")
        self.vocabulary = _frozen(data["vocabulary"])
        self.unverified_groups = _unverified(data, self.observed)
        for partner, canon in self.vocabulary.items():
            _enum(TrustState, canon, f"vocabulary entry '{partner}'")

    @classmethod
    def load(cls, path: str | Path, public_key_b64: str | None = None) -> "GuardianProfile":
        data, p = _read_yaml(path)
        return cls(data, p, public_key_b64)

    def map_state(self, partner_state: str | None) -> TrustState:
        if partner_state is None:
            return TrustState.UNKNOWN
        try:
            return TrustState[self.vocabulary[partner_state]]
        except (KeyError, TypeError) as exc:
            raise UnknownVerdictState(f"UNKNOWN_VERDICT_STATE: '{partner_state}'") from exc

    @property
    def public_key_b64(self) -> str:
        if self._key_override is not None:
            return self._key_override
        name = self._data.get("public_key_file")
        found = _find(str(name), self._path) if name else None
        if found is None:
            raise ProfileError(
                f"Guardian public key unavailable: no override given and key file '{name}' not found")
        return found.read_text(encoding="utf-8").strip()
