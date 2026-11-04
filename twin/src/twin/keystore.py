"""Persistent Keystore for Twin Layer Device Identities.

Maintains stable cryptographic Ed25519 keypairs across simulator restarts.
Keys are stored atomically in a protected JSON keystore (mode 600) and are
NEVER derived from public simulation seeds.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from typing import Any, Dict, List, Optional, Tuple

from twin.crypto_utils import generate_keypair

logger = logging.getLogger("twin.keystore")


def get_default_keystore_path() -> str:
    """Determine default path for device keystore JSON file."""
    env_path = os.getenv("KEYSTORE_PATH")
    if env_path:
        return env_path
    db_path = os.getenv("GROUND_TRUTH_DB_PATH", "ground_truth.db")
    db_dir = os.path.dirname(os.path.abspath(db_path)) or "."
    return os.path.join(db_dir, "device_keys.json")


class DeviceKeystore:
    """Manages persistent Ed25519 keypairs for city sensors."""

    def __init__(self, keystore_path: Optional[str] = None):
        self.path = os.path.abspath(keystore_path or get_default_keystore_path())
        self.keys: Dict[str, Dict[str, str]] = {}
        self.load()

    def load(self) -> None:
        """Load stored device keypairs from disk if file exists."""
        if os.path.isfile(self.path):
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    self.keys = json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load keystore from {self.path}: {e}")
                self.keys = {}
        else:
            self.keys = {}

    def save(self) -> None:
        """Atomically persist device keypairs with mode 600 permissions."""
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        temp_path = f"{self.path}.tmp.{uuid.uuid4().hex[:8]}"
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
        fd = os.open(temp_path, flags, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(self.keys, f, indent=2)
        try:
            os.chmod(temp_path, 0o600)
        except OSError:
            pass
        os.replace(temp_path, self.path)

    def get_or_create_keypair(self, device_id: str) -> Tuple[str, str]:
        """Fetch existing keypair or generate a new random Ed25519 keypair."""
        if device_id in self.keys:
            entry = self.keys[device_id]
            return entry["private_key"], entry["public_key"]

        priv, pub = generate_keypair()
        self.keys[device_id] = {"private_key": priv, "public_key": pub}
        self.save()
        return priv, pub

    def ensure_device_keys(self, device_ids: List[str]) -> Dict[str, Tuple[str, str]]:
        """Ensure all specified devices have keys; generates only for missing devices."""
        changed = False
        result: Dict[str, Tuple[str, str]] = {}
        for dev_id in device_ids:
            if dev_id in self.keys:
                entry = self.keys[dev_id]
                result[dev_id] = (entry["private_key"], entry["public_key"])
            else:
                priv, pub = generate_keypair()
                self.keys[dev_id] = {"private_key": priv, "public_key": pub}
                result[dev_id] = (priv, pub)
                changed = True

        if changed:
            self.save()
        return result
