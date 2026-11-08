"""HashiCorp Vault inter-layer communication key synchronization for Twin Layer.

Fetches and maintains dynamic communication keys from HashiCorp Vault KV v2.
Supports AppRole authentication, automatic lease renewal, PEM and kid formats,
automatic algorithm detection (X25519, Ed25519, RSA), and background synchronization.
"""

from __future__ import annotations

import datetime
import hashlib
import json
import logging
import os
import ssl
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional, Tuple, Union

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, rsa, x25519

from twin.secrets import get_secret

logger = logging.getLogger("twin.vault_keys")

DEFAULT_PATHS = (
    "comm/twin/guardian/private",
    "comm/twin/brain/private",
    "comm/guardian/twin/public",
    "comm/brain/twin/public",
)


def _is_future_timestamp(active_after: Any) -> bool:
    """Return True if active_after is in the future relative to UTC now."""
    if not active_after:
        return False
    now = datetime.datetime.now(datetime.timezone.utc)
    if isinstance(active_after, (int, float)):
        return active_after > now.timestamp()
    if isinstance(active_after, str):
        try:
            s = active_after.replace("Z", "+00:00")
            dt = datetime.datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            return dt > now
        except Exception:
            pass
    return False


def load_and_detect_key(pem_str: str) -> Tuple[Any, str]:
    """Parse PEM key string and determine key algorithm.

    Returns:
        (key_object, algorithm_name) where algorithm is 'X25519', 'Ed25519',
        'RSA-<bits>', or 'unknown'.
    """
    if not pem_str or not isinstance(pem_str, str):
        return None, "unknown"
    pem_bytes = pem_str.strip().encode("utf-8")
    key_obj = None
    try:
        key_obj = serialization.load_pem_private_key(pem_bytes, password=None)
    except Exception:
        try:
            key_obj = serialization.load_pem_public_key(pem_bytes)
        except Exception:
            key_obj = None

    if key_obj is None:
        return None, "unknown"

    if isinstance(key_obj, (x25519.X25519PrivateKey, x25519.X25519PublicKey)):
        return key_obj, "X25519"
    elif isinstance(key_obj, (ed25519.Ed25519PrivateKey, ed25519.Ed25519PublicKey)):
        return key_obj, "Ed25519"
    elif isinstance(key_obj, (rsa.RSAPrivateKey, rsa.RSAPublicKey)):
        return key_obj, f"RSA-{key_obj.key_size}"
    return key_obj, "unknown"


class VaultKeyManager:
    """Thread-safe manager for HashiCorp Vault AppRole authentication and KV v2 key fetch."""

    def __init__(
        self,
        vault_addr: Optional[str] = None,
        namespace: Optional[str] = None,
        kv_mount: Optional[str] = None,
        paths: Optional[List[str]] = None,
        cacert: Optional[str] = None,
        refresh_seconds: Optional[int] = None,
    ):
        self._vault_addr = (
            vault_addr if vault_addr is not None else os.getenv("VAULT_ADDR", "")
        ).strip().rstrip("/")
        # VAULT_NAMESPACE defaults to "admin"; empty string means no header
        if namespace is not None:
            self._namespace = namespace.strip()
        else:
            raw_ns = os.getenv("VAULT_NAMESPACE")
            self._namespace = "admin" if raw_ns is None else raw_ns.strip()

        self._kv_mount = (
            kv_mount if kv_mount is not None else os.getenv("VAULT_KV_MOUNT", "secret")
        ).strip().strip("/")

        if paths is not None:
            self._paths = list(paths)
        else:
            raw_paths = os.getenv("VAULT_TWIN_PATHS", "").strip()
            if raw_paths:
                self._paths = [p.strip() for p in raw_paths.split(",") if p.strip()]
            else:
                self._paths = list(DEFAULT_PATHS)

        self._cacert = (
            cacert if cacert is not None else os.getenv("VAULT_CACERT", "")
        ).strip()

        if refresh_seconds is not None:
            self._refresh_seconds = refresh_seconds
        else:
            raw_ref = os.getenv("VAULT_REFRESH_SECONDS", "").strip()
            self._refresh_seconds = int(raw_ref) if raw_ref else 300

        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

        self._token: Optional[str] = None
        self._token_expires_at: float = 0.0
        self._last_refresh_wall: Optional[str] = None
        self._last_error: Optional[str] = None
        self._paths_data: Dict[str, Dict[str, Any]] = {}
        self._last_versions: Dict[str, int] = {}

    @property
    def enabled(self) -> bool:
        return bool(self._vault_addr)

    def _get_ssl_context(self) -> Optional[ssl.SSLContext]:
        if not self._vault_addr.startswith("https://"):
            return None
        if self._cacert:
            return ssl.create_default_context(cafile=self._cacert)
        try:
            import certifi
            return ssl.create_default_context(cafile=certifi.where())
        except ImportError:
            return ssl.create_default_context()

    def _login(self) -> None:
        """Authenticate via AppRole and cache token with lease duration."""
        role_id = get_secret("VAULT_ROLE_ID")
        secret_id = get_secret("VAULT_SECRET_ID")
        if not role_id or not secret_id:
            raise ValueError("Missing VAULT_ROLE_ID or VAULT_SECRET_ID credentials")

        login_url = f"{self._vault_addr}/v1/auth/approle/login"
        body = json.dumps({"role_id": role_id, "secret_id": secret_id}).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self._namespace:
            headers["X-Vault-Namespace"] = self._namespace

        req = urllib.request.Request(login_url, data=body, headers=headers, method="POST")
        ctx = self._get_ssl_context()
        with urllib.request.urlopen(req, context=ctx, timeout=10.0) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        auth = data.get("auth", {})
        client_token = auth.get("client_token")
        if not client_token:
            raise ValueError("Vault AppRole login returned no client_token")

        lease_dur = float(auth.get("lease_duration", 3600) or 3600)
        self._token = client_token
        self._token_expires_at = time.time() + lease_dur
        logger.debug("Vault AppRole authentication successful")

    def _ensure_token(self) -> str:
        """Ensure active client token, refreshing 60s before lease expiration."""
        now = time.time()
        if not self._token or now >= (self._token_expires_at - 60.0):
            self._login()
        return self._token  # type: ignore[return-value]

    def _http_get_kv(self, path: str, version: Optional[int] = None) -> Dict[str, Any]:
        """Send authenticated GET request to Vault KV v2."""
        token = self._ensure_token()
        clean_path = path.strip("/")
        url = f"{self._vault_addr}/v1/{self._kv_mount}/data/{clean_path}"
        if version is not None:
            url = f"{url}?version={version}"

        headers = {
            "X-Vault-Token": token,
            "Accept": "application/json",
        }
        if self._namespace:
            headers["X-Vault-Namespace"] = self._namespace

        req = urllib.request.Request(url, headers=headers, method="GET")
        ctx = self._get_ssl_context()
        with urllib.request.urlopen(req, context=ctx, timeout=10.0) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _read_kv_with_retry(self, path: str, version: Optional[int] = None) -> Dict[str, Any]:
        """Fetch KV v2 data, retrying once on 403 Forbidden with fresh token."""
        try:
            return self._http_get_kv(path, version=version)
        except urllib.error.HTTPError as e:
            if e.code == 403:
                logger.debug(f"Vault 403 Forbidden for '{path}'. Re-authenticating...")
                self._login()
                return self._http_get_kv(path, version=version)
            raise

    def _parse_secret_payload(
        self, path: str, data_block: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], Any]:
        """Parse KV v2 data block into metadata dictionary and loaded key object."""
        secret_data = data_block.get("data", {})
        metadata = data_block.get("metadata", {})
        version = metadata.get("version", 1)
        created_time = metadata.get("created_time")

        if "key" in secret_data:
            fmt = "pem-key"
            pem_str = secret_data.get("key", "")
            kid = None
            active_after = None
            fingerprint = None
        else:
            fmt = "kid"
            kid = secret_data.get("kid")
            pem_str = (
                secret_data.get("private_key")
                or secret_data.get("public_key")
                or secret_data.get("key", "")
            )
            active_after = secret_data.get("active_after")
            fingerprint = secret_data.get("fingerprint")

        key_obj, alg = load_and_detect_key(pem_str)

        fingerprint_ok = None
        if fingerprint is not None and isinstance(fingerprint, str):
            calc_fp = None
            if "public_key" in secret_data and isinstance(secret_data["public_key"], str):
                pub_bytes = secret_data["public_key"].encode("utf-8")
                calc_fp = hashlib.sha256(pub_bytes).hexdigest()
            elif key_obj is not None and hasattr(key_obj, "public_key"):
                pub_bytes = key_obj.public_key().public_bytes(
                    serialization.Encoding.PEM,
                    serialization.PublicFormat.SubjectPublicKeyInfo,
                )
                calc_fp = hashlib.sha256(pub_bytes).hexdigest()

            if calc_fp is not None:
                fingerprint_ok = calc_fp.lower() == fingerprint.strip().lower()
            else:
                fingerprint_ok = False

        meta = {
            "path": path,
            "version": version,
            "created_time": created_time,
            "format": fmt,
            "algorithm": alg,
            "kid": kid,
            "active_after": active_after,
            "fingerprint_ok": fingerprint_ok,
        }
        return meta, key_obj

    def refresh(self) -> Tuple[bool, Optional[str]]:
        """Synchronize all configured paths from Vault.

        Keeps last good keys on failure. Returns (changed, error_string).
        """
        if not self.enabled:
            return False, None

        errors: List[str] = []
        now_wall = datetime.datetime.now(datetime.timezone.utc).isoformat()
        any_changed = False

        with self._lock:
            for path in self._paths:
                try:
                    res = self._read_kv_with_retry(path)
                    data_block = res.get("data", {})
                    meta, key_obj = self._parse_secret_payload(path, data_block)
                    version = meta["version"]
                    fmt = meta["format"]

                    raw_secret = data_block.get("data", {})
                    is_private = (
                        "private" in path.lower()
                        or "private_key" in raw_secret
                        or isinstance(
                            key_obj,
                            (x25519.X25519PrivateKey, ed25519.Ed25519PrivateKey, rsa.RSAPrivateKey),
                        )
                    )
                    is_public = (
                        "public" in path.lower()
                        or "public_key" in raw_secret
                        or isinstance(
                            key_obj,
                            (x25519.X25519PublicKey, ed25519.Ed25519PublicKey, rsa.RSAPublicKey),
                        )
                    )

                    # Kid format active_after rule: if private key's active_after is in future, use version-1
                    if fmt == "kid" and is_private:
                        if _is_future_timestamp(meta.get("active_after")) and version > 1:
                            try:
                                prev_res = self._read_kv_with_retry(path, version=version - 1)
                                prev_block = prev_res.get("data", {})
                                prev_meta, prev_key_obj = self._parse_secret_payload(path, prev_block)
                                meta = prev_meta
                                key_obj = prev_key_obj
                                version = meta["version"]
                            except Exception as pe:
                                logger.warning(
                                    f"Failed to fetch previous private key version for '{path}': {pe}"
                                )

                    # Kid format public keys: keep latest and previous
                    prev_entry = None
                    if fmt == "kid" and is_public and version > 1:
                        try:
                            prev_res = self._read_kv_with_retry(path, version=version - 1)
                            prev_block = prev_res.get("data", {})
                            prev_meta, prev_key_obj = self._parse_secret_payload(path, prev_block)
                            prev_entry = {
                                "metadata": prev_meta,
                                "key_obj": prev_key_obj,
                            }
                        except Exception as pe:
                            logger.debug(
                                f"Failed to fetch previous public key version for '{path}': {pe}"
                            )

                    old_v = self._last_versions.get(path)
                    if old_v != version:
                        any_changed = True
                        self._last_versions[path] = version

                    self._paths_data[path] = {
                        "metadata": meta,
                        "key_obj": key_obj,
                        "previous": prev_entry,
                    }
                    logger.info(f"vault keys refreshed: {path} v{version}")
                except Exception as e:
                    err_msg = f"{type(e).__name__}: {e}"
                    errors.append(f"{path}: {err_msg}")
                    logger.warning(f"vault key refresh failed for {path}: {err_msg}")

            self._last_refresh_wall = now_wall
            if errors:
                self._last_error = "; ".join(errors)
            else:
                self._last_error = None

        return any_changed, self._last_error

    def status(self) -> Dict[str, Any]:
        """Return metadata summary for operations status inspection."""
        with self._lock:
            if not self.enabled:
                return {
                    "enabled": False,
                    "status": "disabled",
                    "last_refresh_wall": None,
                    "last_error": None,
                    "paths": {},
                }

            paths_meta: Dict[str, Any] = {}
            for path, data in self._paths_data.items():
                entry = dict(data["metadata"])
                if data.get("previous"):
                    entry["previous_version"] = data["previous"]["metadata"].get("version")
                paths_meta[path] = entry

            return {
                "enabled": True,
                "status": "ok" if self._last_error is None else "degraded",
                "last_refresh_wall": self._last_refresh_wall,
                "last_error": self._last_error,
                "paths": paths_meta,
            }

    def get_key(self, path: str, version: Optional[Union[str, int]] = None) -> Any:
        """Retrieve loaded key object for path ('latest' or 'previous')."""
        with self._lock:
            if not self.enabled:
                return None
            data = self._paths_data.get(path)
            if not data:
                return None
            if version == "previous":
                prev = data.get("previous")
                return prev.get("key_obj") if prev else None
            return data.get("key_obj")

    def start(self) -> None:
        """Initiate first refresh and launch background synchronization thread."""
        if not self.enabled:
            return
        try:
            self.refresh()
        except Exception as e:
            logger.warning(f"Initial Vault key fetch failed: {type(e).__name__}: {e}")

        if self._refresh_seconds > 0 and (self._thread is None or not self._thread.is_alive()):
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._worker_loop, daemon=True, name="vault-keys-sync"
            )
            self._thread.start()
            logger.info(
                f"Vault key synchronization thread started (refresh interval: {self._refresh_seconds}s)"
            )

    def stop(self) -> None:
        """Signal background synchronization thread to terminate."""
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)
            self._thread = None

    def _worker_loop(self) -> None:
        while not self._stop_event.is_set():
            if self._stop_event.wait(timeout=self._refresh_seconds):
                break
            try:
                self.refresh()
            except Exception as e:
                logger.warning(f"Background Vault key refresh error: {type(e).__name__}: {e}")


# Global module singleton instance
_GLOBAL_MANAGER: Optional[VaultKeyManager] = None


def get_manager() -> VaultKeyManager:
    """Return the global VaultKeyManager singleton instance."""
    global _GLOBAL_MANAGER
    if _GLOBAL_MANAGER is None:
        _GLOBAL_MANAGER = VaultKeyManager()
    return _GLOBAL_MANAGER


def set_manager(manager: Optional[VaultKeyManager]) -> None:
    """Replace global manager instance (used for testing)."""
    global _GLOBAL_MANAGER
    _GLOBAL_MANAGER = manager


def get_key(path: str, version: Optional[Union[str, int]] = None) -> Any:
    """Public accessor returning loaded key object or None."""
    return get_manager().get_key(path, version=version)


def status() -> Dict[str, Any]:
    """Public accessor returning metadata status dictionary."""
    return get_manager().status()


def reload_vault_keys() -> str:
    """Refresh keys dynamically; called during runtime secret rotation."""
    mgr = get_manager()
    if not mgr.enabled:
        return "disabled"
    changed, error = mgr.refresh()
    if error:
        return f"error: {error}"
    return "changed" if changed else "unchanged"


def start_vault_sync() -> None:
    """Start Vault key synchronization if enabled."""
    get_manager().start()


def stop_vault_sync() -> None:
    """Stop Vault background key synchronization."""
    get_manager().stop()

