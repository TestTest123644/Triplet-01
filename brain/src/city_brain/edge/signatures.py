"""Ed25519 signatures over the Twin's canonical payload form, plus a device key ring."""

import base64
import binascii
import json
from collections.abc import Mapping
from typing import Any

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519


def canonical_bytes(payload: Mapping[str, Any]) -> bytes:
    """Sorted-key compact JSON of the payload without `sig`, as UTF-8 (matches the Twin)."""
    clean = {k: v for k, v in payload.items() if k != "sig"}
    return json.dumps(clean, sort_keys=True, separators=(",", ":")).encode("utf-8")


def generate_keypair() -> tuple[str, str]:
    """Return (private_key_b64, public_key_b64), raw Ed25519 keys."""
    private_key = ed25519.Ed25519PrivateKey.generate()
    priv = private_key.private_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PrivateFormat.Raw,
        encryption_algorithm=serialization.NoEncryption(),
    )
    pub = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    return base64.b64encode(priv).decode("ascii"), base64.b64encode(pub).decode("ascii")


def sign(payload: Mapping[str, Any], private_key_b64: str) -> str:
    """Return the base64 Ed25519 signature of the canonical payload."""
    key = ed25519.Ed25519PrivateKey.from_private_bytes(base64.b64decode(private_key_b64))
    return base64.b64encode(key.sign(canonical_bytes(payload))).decode("ascii")


def verify(payload: Mapping[str, Any], public_key_b64: str) -> bool:
    """True only if payload["sig"] is a valid signature under the key. Never raises."""
    try:
        sig_b64 = payload.get("sig")
        if not sig_b64:
            return False
        public_key = ed25519.Ed25519PublicKey.from_public_bytes(base64.b64decode(public_key_b64))
        public_key.verify(base64.b64decode(sig_b64), canonical_bytes(payload))
        return True
    except (InvalidSignature, ValueError, TypeError, binascii.Error):
        return False


class KeyRing:
    """device_id -> base64 public key."""

    def __init__(self) -> None:
        self._keys: dict[str, str] = {}

    def set(self, device_id: str, pub_b64: str) -> None:
        self._keys[device_id] = pub_b64

    def get(self, device_id: str) -> str | None:
        return self._keys.get(device_id)

    def clear(self) -> None:
        self._keys.clear()
