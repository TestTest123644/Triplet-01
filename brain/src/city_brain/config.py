"""Configuration loader: YAML file + BRAIN__A__B env overrides, validated strictly."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Literal, Mapping

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError

ENV_PREFIX = "BRAIN__"


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class KafkaConfig(_Strict):
    bootstrap: str = ""
    group: str = "city-brain"
    topics: dict[str, str] = Field(default_factory=dict)


class TwinConfig(_Strict):
    profile: str = "twin-triplet07-v1"
    mcp_url: str = ""
    call_timeout_s: float = 2.0


class GuardianConfig(_Strict):
    profile: str = "guardian-v1"
    public_key_file: str | None = None
    verdict_wait_s: float = 2.0


class BrainConfig(_Strict):
    mode: Literal["standalone", "triplet"]
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    twin: TwinConfig = Field(default_factory=TwinConfig)
    guardian: GuardianConfig = Field(default_factory=GuardianConfig)
    policy: dict = Field(default_factory=dict)
    params: dict = Field(default_factory=dict)
    logs_dir: Path = Path("logs")
    # Base64 Ed25519 private key the Brain signs `brain.events` with (spec 11.1). Usually set
    # through BRAIN__SIGNING_KEY; standalone runs derive one from the scenario seed instead.
    signing_key: str | None = None


def _format_errors(exc: ValidationError) -> str:
    parts = []
    for err in exc.errors():
        path = ".".join(str(p) for p in err["loc"]) or "<root>"
        parts.append(f"{path}: {err['msg']}")
    return "invalid configuration: " + "; ".join(parts)


def _read_yaml(path: Path) -> dict:
    with open(path, encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(f"{path}: top level must be a mapping")
    return data


def _apply_env(data: dict, env: Mapping[str, str]) -> None:
    for key, value in env.items():
        if not key.startswith(ENV_PREFIX):
            continue
        parts = [p.lower() for p in key[len(ENV_PREFIX):].split("__") if p]
        if not parts:
            continue
        node = data
        for part in parts[:-1]:
            child = node.get(part)
            if not isinstance(child, dict):
                child = {}
                node[part] = child
            node = child
        node[parts[-1]] = value


def load_config_from_dict(data: dict, base_dir: str | Path | None = None) -> BrainConfig:
    """Validate a raw mapping. `policy_file`/`params_file` (relative to `base_dir`)
    are loaded into `policy`/`params`; relative `logs_dir` is resolved against it."""
    data = dict(data)
    base = Path(base_dir) if base_dir is not None else Path.cwd()
    policy_file = data.pop("policy_file", None)
    params_file = data.pop("params_file", None)
    if policy_file is not None:
        data["policy"] = _read_yaml(base / policy_file)
    if params_file is not None:
        data["params"] = _read_yaml(base / params_file)
    if "logs_dir" in data:
        data["logs_dir"] = (base / str(data["logs_dir"])).resolve()
    try:
        return BrainConfig.model_validate(data)
    except ValidationError as exc:
        raise ValueError(_format_errors(exc)) from None


def load_config(path: str | Path, env: Mapping[str, str] | None = None) -> BrainConfig:
    path = Path(path)
    data = _read_yaml(path)
    _apply_env(data, env if env is not None else {})
    return load_config_from_dict(data, base_dir=path.resolve().parent)
