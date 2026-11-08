import base64
import binascii
import json
import time

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from cryptography.hazmat.primitives.serialization import load_der_public_key

from guardian_layer.consumer_producer.kafka_consumer.mcp_client.client import (
    McpToolClient,
)


class DeviceKeyProvider:
    """TTL cache of device public keys, fetched through MCP get_device_key."""

    def __init__(
        self,
        client: McpToolClient,
        ttl_s: float = 300,
        negative_ttl_s: float = 30,
        min_refresh_s: float = 30,
    ):
        self._client = client
        self._ttl = ttl_s
        self._neg_ttl = negative_ttl_s
        self._min_refresh = min_refresh_s
        self._cache: dict[str, tuple[float, Ed25519PublicKey | None]] = {}

    @staticmethod
    def _parse(b64: str) -> Ed25519PublicKey:
        raw = base64.b64decode(b64, validate=True)
        if len(raw) == 32:
            return Ed25519PublicKey.from_public_bytes(raw)
        key = load_der_public_key(raw)  # SPKI DER fallback
        if not isinstance(key, Ed25519PublicKey):
            raise ValueError("not an Ed25519 key")
        return key

    def get(self, device_id: str, refresh: bool = False) -> Ed25519PublicKey | None:
        now = time.monotonic()
        hit = self._cache.get(device_id)
        if hit:
            age = now - hit[0]
            limit = self._ttl if hit[1] is not None else self._neg_ttl
            if refresh:
                if age < self._min_refresh:  # throttle forced refreshes
                    return hit[1]
            elif age < limit:
                return hit[1]

        b64 = self._client.get_device_key(device_id)  # may raise McpError
        try:
            key = self._parse(b64) if b64 else None
        except (binascii.Error, ValueError):
            key = None
        self._cache[device_id] = (now, key)
        return key


class Ed25519SignatureVerifier:
    def __init__(
        self,
        keys: DeviceKeyProvider,
        id_field: str = "device_id",
        sig_field: str = "sig",
    ):
        self._keys = keys
        self.id_field = id_field
        self.sig_field = sig_field

    def _canonical(self, payload: dict) -> bytes:
        body = {k: v for k, v in payload.items() if k != self.sig_field}
        return json.dumps(body, sort_keys=True, separators=(",", ":")).encode()

    def has_key(self, payload: dict) -> bool:
        device_id = payload.get(self.id_field)
        return isinstance(device_id, str) and self._keys.get(device_id) is not None

    def verify(self, payload: dict) -> bool:
        device_id = payload.get(self.id_field)
        sig_b64 = payload.get(self.sig_field)
        if not isinstance(device_id, str) or not isinstance(sig_b64, str):
            return False
        try:
            sig = base64.b64decode(sig_b64, validate=True)
        except (binascii.Error, ValueError):
            return False

        message = self._canonical(payload)
        for refresh in (False, True):  # second pass handles a rotated key
            key = self._keys.get(device_id, refresh=refresh)
            if key is None:
                return False
            try:
                key.verify(sig, message)
                return True
            except InvalidSignature:
                continue
        return False
