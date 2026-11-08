"""Configuration loader: YAML file + BRAIN__A__B env overrides, validated strictly."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Literal, Mapping

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError

ENV_PREFIX = "BRAIN__"

logger = logging.getLogger("city_brain.config")


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

        password = self.resolve_password()
        if password:
            cfg["sasl.password"] = password
        elif self.sasl_username:
            # The silent failure: the client goes out with a user and no secret, and the
            # broker's rejection says nothing about the empty variable behind it.
            logger.warning(
                "%s is configured with user %r but no password could be resolved. Set "
                "KAFKA_PASSWORD or KAFKA_PASSWORD_FILE (an empty value counts as unset); "
                "the broker will reject this client.", protocol, self.sasl_username)

        if protocol == "SASL_SSL" and self.ssl_ca_location:
            cfg["ssl.ca.location"] = self.ssl_ca_location

        return cfg

    def resolve_password(self) -> str | None:
        """The SASL password, from the file first.

        The triplet's `.env.example` states that `KAFKA_PASSWORD_FILE` takes precedence
        over `KAFKA_PASSWORD`, so a stale literal left in an `.env` cannot quietly send
        the Brain to the broker with a different secret than its partners use.
        """
        if self.sasl_password_file:
            found = _read_secret(self.sasl_password_file)
            if found:
                return found
        literal = (self.sasl_password or "").strip()
        if not literal:
            return None
        # A literal that is itself a path is read, as it was before.
        return _read_secret(literal) or literal


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


def _read_secret(path: str) -> str | None:
    """A secret from a file, or `None` if there is no file there."""
    try:
        p = Path(path)
        if p.is_file():
            return p.read_text(encoding="utf-8").strip() or None
    except (OSError, ValueError):
        pass  # not a usable path: the caller falls back to treating it as a value
    return None


def _brain_token(text: str) -> str | None:
    """The Brain's bearer token out of a shared `brain:<tok>,guardian:<tok>` list.

    A file or variable holding nothing but the token is accepted too, since that is the
    usual shape of a Docker secret.
    """
    text = text.strip()
    if not text:
        return None
    for entry in text.split(","):
        entry = entry.strip()
        if ":" in entry:
            principal, token = entry.split(":", 1)
            if principal.strip().lower() == "brain":
                return token.strip() or None
    if ":" not in text and "," not in text and len(text.split()) == 1:
        return text
    return None


def _apply_standard_kafka_env(data: dict, env: Mapping[str, str]) -> None:
    """Map standard KAFKA_* and MCP_* environment variables into config.

    An empty variable is not a value. Compose files carry defaults like
    `KAFKA_PASSWORD: ${KAFKA_PASSWORD:-}`, which arrive as empty strings; treating those
    as settings would wipe what `config/triplet.yaml` says and hide the matching
    `*_FILE` variable behind nothing at all.
    """
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
        value = (env.get(env_key) or "").strip()
        if value:
            kafka[conf_key] = value

    # MCP_AUTH_TOKEN is the Brain's own override and stays the most specific thing anyone
    # can set. The shared list follows, the file ahead of the variable: `.env.example`
    # says MCP_AUTH_TOKENS_FILE "takes priority over MCP_AUTH_TOKENS".
    twin = data.setdefault("twin", {})
    token = (env.get("MCP_AUTH_TOKEN") or "").strip()
    if not token:
        tokens_file = (env.get("MCP_AUTH_TOKENS_FILE") or "").strip()
        if tokens_file:
            contents = _read_secret(tokens_file)
            token = (_brain_token(contents) or "") if contents else ""
    if not token:
        token = _brain_token(env.get("MCP_AUTH_TOKENS") or "") or ""
    if token:
        twin["auth_token"] = token


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
