"""Cryptographic utilities for Twin Layer: Ed25519 signing and verification.

All sensor payloads emitted to Kafka are deterministically signed using
per-device Ed25519 private keys.
"""

from __future__ import annotations

import base64
import json
from typing import Any, Dict, Tuple

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519


def generate_keypair() -> Tuple[str, str]:
    """Generate an Ed25519 keypair encoded as base64 strings.

    Returns:
        Tuple of (private_key_b64, public_key_b64)
    """
    private_key = ed25519.Ed25519PrivateKey.generate()
    public_key = private_key.public_key()

    priv_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PrivateFormat.Raw,
        encryption_algorithm=serialization.NoEncryption(),
    )
    pub_bytes = public_key.public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )

    return (
        base64.b64encode(priv_bytes).decode("ascii"),
        base64.b64encode(pub_bytes).decode("ascii"),
    )


def canonicalize_for_signing(payload: Dict[str, Any]) -> bytes:
    """Produce deterministic UTF-8 bytes of payload excluding the 'sig' field.

    Keys are sorted to guarantee reproducibility.
    """
    clean_dict = {k: v for k, v in payload.items() if k != "sig"}
    canonical_json = json.dumps(clean_dict, sort_keys=True, separators=(",", ":"))
    return canonical_json.encode("utf-8")


def sign_payload(payload: Dict[str, Any], private_key_b64: str) -> str:
    """Sign payload data using private key (Ed25519) and return base64 signature."""
    priv_bytes = base64.b64decode(private_key_b64)
    private_key = ed25519.Ed25519PrivateKey.from_private_bytes(priv_bytes)
    data = canonicalize_for_signing(payload)
    signature = private_key.sign(data)
    return base64.b64encode(signature).decode("ascii")


def verify_payload_signature(payload: Dict[str, Any], public_key_b64: str) -> bool:
    """Verify Ed25519 signature of payload against public key."""
    sig_b64 = payload.get("sig")
    if not sig_b64:
        return False
    try:
        pub_bytes = base64.b64decode(public_key_b64)
        public_key = ed25519.Ed25519PublicKey.from_public_bytes(pub_bytes)
        sig_bytes = base64.b64decode(sig_b64)
        data = canonicalize_for_signing(payload)
        public_key.verify(sig_bytes, data)
        return True
    except (InvalidSignature, ValueError, KeyError):
        return False
