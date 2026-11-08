#!/usr/bin/env python3
"""Rotate every communication key pair. Run every 24 h. Exit code 1 on any failure."""

import hashlib
import logging
import os
import secrets
import sys
import time
from pathlib import Path

import httpx
import hvac
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from pydantic_settings import BaseSettings, SettingsConfigDict

from guardian_layer.utils.helpers import find_project_root

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("key-rotation")

SERVICES = ("guardian", "twin", "brain")
PAIRS = [(a, b) for a in SERVICES for b in SERVICES if a != b]
KEY_SIZE = int(os.getenv("ROTATION_KEY_SIZE", "4096"))
OVERLAP = int(os.getenv("ROTATION_OVERLAP_SECONDS", "3600"))


def new_keypair() -> tuple[str, str]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=KEY_SIZE)
    priv = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    pub = (
        key.public_key()
        .public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        .decode()
    )
    return priv, pub


def rotate_pair(client: hvac.Client, owner: str, peer: str, overlap: int) -> None:
    priv, pub = new_keypair()
    now = int(time.time())
    kid = f"{owner}-{peer}-{now}-{secrets.token_hex(4)}"
    base = f"comm/{owner}/{peer}"
    kv = client.secrets.kv.v2
    # 1) public key first: peers can verify the new key immediately
    kv.create_or_update_secret(
        f"{base}/public",
        {
            "public_key": pub,
            "kid": kid,
            "created_at": now,
            "fingerprint": hashlib.sha256(pub.encode()).hexdigest(),
        },
        mount_point="secret",
    )
    # 2) private key: the owner switches to it only after the overlap window
    kv.create_or_update_secret(
        f"{base}/private",
        {"private_key": priv, "kid": kid, "active_after": now + overlap},
        mount_point="secret",
    )


_ROOT = find_project_root(Path(__file__).parent)


class RotationSettings(BaseSettings):
    """Trigger URLs, one per service, read from .env (or the environment)."""

    model_config = SettingsConfigDict(
        env_file=_ROOT / ".env" if _ROOT else ".env",
        extra="ignore",  # .env also holds settings for other components
    )

    guardian_rotation_url: str = ""
    twin_rotation_url: str = ""
    brain_rotation_url: str = ""


def alert_urls(settings: RotationSettings | None = None) -> dict[str, str]:
    """One trigger URL per service (guardian, twin, brain)."""
    settings = settings or RotationSettings()
    urls = {s: getattr(settings, f"{s}_rotation_url") for s in SERVICES}
    return {service: url for service, url in urls.items() if url}


def alert() -> None:
    """POST {"rotation": true} to the trigger URL of every service."""
    for service, url in alert_urls().items():
        try:
            httpx.post(url, json={"rotation": True}, timeout=10).raise_for_status()
        except httpx.HTTPError:
            log.exception("trigger for %s failed", service)


def rotate_all(client: hvac.Client, overlap: int | None = None) -> int:
    """Rotate every pair and return the number of failures."""
    overlap = OVERLAP if overlap is None else overlap
    failures = 0
    for owner, peer in PAIRS:
        try:
            rotate_pair(client, owner, peer, overlap)
            log.info("rotated %s->%s", owner, peer)
        except Exception:  # one failing pair must not block the others
            failures += 1
            log.exception("rotation failed for %s->%s", owner, peer)
    if failures:
        alert()
    return failures


def main() -> int:
    try:
        client = hvac.Client(
            url=os.environ["VAULT_ADDR"], verify=os.getenv("VAULT_CACERT", True)
        )
        client.auth.approle.login(
            role_id=os.environ["ROTATOR_ROLE_ID"],
            secret_id=os.environ["ROTATOR_SECRET_ID"],
        )
    except Exception:
        log.exception("could not authenticate to Vault")
        alert()
        return 1
    return 1 if rotate_all(client) else 0


if __name__ == "__main__":
    sys.exit(main())
