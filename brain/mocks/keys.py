"""Seed-derived Ed25519 keys, so two runs of a mock produce byte-identical signed evidence."""

import base64
import hashlib

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519


def derive_keypair(label: str) -> tuple[str, str]:
    """(private_b64, public_b64), raw Ed25519 keys; the private key is SHA-256 of `label`."""
    seed = hashlib.sha256(label.encode("utf-8")).digest()
    key = ed25519.Ed25519PrivateKey.from_private_bytes(seed)
    pub = key.public_key().public_bytes(
        encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw)
    return base64.b64encode(seed).decode("ascii"), base64.b64encode(pub).decode("ascii")
