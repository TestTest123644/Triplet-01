"""Key lookup helpers that stay compatible across rotations (issue #111)."""

import hashlib
import time
from typing import Any

import hvac


def _read(
    client: hvac.Client, path: str, version: int | None = None
) -> tuple[dict[str, Any], int]:
    response = client.secrets.kv.v2.read_secret_version(
        path=path, version=version, mount_point="secret", raise_on_deleted_version=True
    )
    return response["data"]["data"], response["data"]["metadata"]["version"]


def _checked(data: dict[str, Any]) -> str:
    """Return the public key PEM, or raise if it does not match its fingerprint."""
    digest = hashlib.sha256(data["public_key"].encode()).hexdigest()
    if digest != data.get("fingerprint"):
        raise ValueError(f"public key {data['kid']} failed integrity check")
    return data["public_key"]


def signing_key(
    client: hvac.Client, owner: str, peer: str, now: float | None = None
) -> tuple[str, str]:
    """(kid, private PEM) that `owner` should use now towards `peer`."""
    now = time.time() if now is None else now
    path = f"comm/{owner}/{peer}/private"
    data, version = _read(client, path)
    if now < data["active_after"] and version > 1:
        data, _ = _read(client, path, version - 1)  # new key not yet adopted by peers
    return data["kid"], data["private_key"]


def verification_keys(client: hvac.Client, owner: str, peer: str) -> dict[str, str]:
    """{kid: public PEM} that `peer` accepts for messages from `owner`.

    Covers the latest and the previous key version.
    """
    path = f"comm/{owner}/{peer}/public"
    data, version = _read(client, path)
    keys = {data["kid"]: _checked(data)}
    if version > 1:
        previous, _ = _read(client, path, version - 1)
        keys[previous["kid"]] = _checked(previous)
    return keys
