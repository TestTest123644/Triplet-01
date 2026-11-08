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
    offset_reset: str = "earliest"
    topics: dict[str, str] = Field(default_factory=dict)
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str = "PLAIN"
    sasl_username: str | None = None
    sasl_password: str | None = None
    sasl_password_file: str | None = None
    ssl_ca_location: str | None = None
    acks: int | str = "all"

    def client_security_config(self) -> dict[str, Any]:
        """Build confluent-kafka client security configuration dictionary."""
        protocol = (self.security_protocol or "PLAINTEXT").upper()
        if protocol == "PLAINTEXT":
            return {"security.protocol": "PLAINTEXT"}

        cfg: dict[str, Any] = {
            "security.protocol": protocol,
            "sasl.mechanism": (self.sasl_mechanism or "PLAIN").upper(),
        }
        if self.sasl_username:
            cfg["sasl.username"] = self.sasl_username

        password = self.sasl_password
        if not password and self.sasl_password_file:
            p = Path(self.sasl_password_file)
            if p.is_file():
                password = p.read_text(encoding="utf-8").strip()
        elif password and Path(password).is_file():
            password = Path(password).read_text(encoding="utf-8").strip()

        if password:
            cfg["sasl.password"] = password

        if protocol == "SASL_SSL" and self.ssl_ca_location:
            cfg["ssl.ca.location"] = self.ssl_ca_location

        return cfg


class TwinConfig(_Strict):
    profile: str = "twin-triplet07-v1"
    mcp_url: str = ""
    call_timeout_s: float = 2.0
    auth_token: str | None = None
    auth_token_file: str | None = None

    def resolve_auth_token(self) -> str | None:
        if self.auth_token:
            return self.auth_token
        if self.auth_token_file:
            p = Path(self.auth_token_file)
            if p.is_file():
                return p.read_text(encoding="utf-8").strip()
        return None


class GuardianConfig(_Strict):
    profile: str = "guardian-v1"
    public_key_file: str | None = None
    verdict_wait_s: float = 2.0
    optional: bool = False


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


def _apply_standard_kafka_env(data: dict, env: Mapping[str, str]) -> None:
    """Map standard KAFKA_* and MCP_* environment variables into config."""
    kafka = data.setdefault("kafka", {})
    mapping = {
        "KAFKA_BOOTSTRAP_SERVERS": "bootstrap",
        "KAFKA_AUTO_OFFSET_RESET": "offset_reset",
        "KAFKA_SECURITY_PROTOCOL": "security_protocol",
        "KAFKA_SASL_MECHANISM": "sasl_mechanism",
        "KAFKA_USERNAME": "sasl_username",
        "KAFKA_PASSWORD": "sasl_password",
        "KAFKA_PASSWORD_FILE": "sasl_password_file",
        "KAFKA_SSL_CA_LOCATION": "ssl_ca_location",
        "KAFKA_ACKS": "acks",
    }
    for env_key, conf_key in mapping.items():
        if env_key in env:
            kafka[conf_key] = env[env_key]

    twin = data.setdefault("twin", {})
    if "MCP_AUTH_TOKEN" in env:
        twin["auth_token"] = env["MCP_AUTH_TOKEN"]
    elif "MCP_AUTH_TOKENS" in env:
        # Parse 'brain:<tok>,guardian:<tok>'
        for entry in env["MCP_AUTH_TOKENS"].split(","):
            entry = entry.strip()
            if ":" in entry:
                principal, token = entry.split(":", 1)
                if principal.strip().lower() == "brain":
                    twin["auth_token"] = token.strip()
                    break
    elif "MCP_AUTH_TOKENS_FILE" in env:
        p = Path(env["MCP_AUTH_TOKENS_FILE"])
        if p.is_file():
            for entry in p.read_text(encoding="utf-8").split(","):
                entry = entry.strip()
                if ":" in entry:
                    principal, token = entry.split(":", 1)
                    if principal.strip().lower() == "brain":
                        twin["auth_token"] = token.strip()
                        break


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
    guardian_opt = data.get("guardian", {}).get("optional", False)
    if isinstance(guardian_opt, str):
        guardian_opt = guardian_opt.lower() in ("true", "1", "yes")
    if guardian_opt:
        policy = data.setdefault("policy", {})
        policy.setdefault("modes", {})["guardian_optional"] = True
        policy.setdefault("trust", {})["implicit_ok"] = True
    if "logs_dir" in data:
        data["logs_dir"] = (base / str(data["logs_dir"])).resolve()
    try:
        return BrainConfig.model_validate(data)
    except ValidationError as exc:
        raise ValueError(_format_errors(exc)) from None


def load_config(path: str | Path, env: Mapping[str, str] | None = None) -> BrainConfig:
    import os

    path = Path(path)
    data = _read_yaml(path)
    env_map = dict(os.environ) if env is None else dict(env)
    _apply_env(data, env_map)
    _apply_standard_kafka_env(data, env_map)
    return load_config_from_dict(data, base_dir=path.resolve().parent)
