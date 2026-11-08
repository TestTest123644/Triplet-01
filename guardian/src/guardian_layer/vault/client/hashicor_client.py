import os
import threading
import time

import httpx


class VaultError(Exception):
    pass


class HashiCorpClient:
    """
    Minimal HashiCorp Vault client using AppRole auth and the KV v2 engine (httpx).

    - create_token(): logs in with role_id/secret_id and stores a fresh token
    - get_token():    returns a valid token, creating a new one if missing/expired
    - get_secerets(): reads a secret, re-logging in automatically if the token expired
    """

    def __init__(
        self,
        vault_addr: str | None = None,
        role_id: str | None = None,
        secret_id: str | None = None,
        namespace: str | None = None,
        mount="secret",
        timeout=10.0,
        verify=True,
        expiry_skew=30,
    ):
        self.vault_addr = (vault_addr or os.environ["VAULT_ADDR"]).rstrip("/")
        self.role_id = role_id or os.environ["VAULT_ROLE_ID"]
        self.secret_id = secret_id or os.environ["VAULT_SECRET_ID"]
        # HCP Vault uses the "admin" namespace by default
        self.namespace = namespace or os.environ.get("VAULT_NAMESPACE", "admin")
        self.mount = mount
        self.expiry_skew = expiry_skew  # renew this many seconds before real expiry

        self._token = None
        self._expires_at = 0.0
        self._lock = threading.Lock()
        self._http = httpx.Client(timeout=timeout, verify=verify)

    # ------------------------------------------------------------------ lifecycle
    def close(self):
        self._http.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ------------------------------------------------------------------ helpers
    def _headers(self, token=None):
        headers = {}
        if self.namespace:
            headers["X-Vault-Namespace"] = self.namespace
        if token:
            headers["X-Vault-Token"] = token
        return headers

    def _token_is_valid(self):
        return (
            self._token is not None
            and time.time() < self._expires_at - self.expiry_skew
        )

    # ------------------------------------------------------------------ public API
    def create_token(self):
        """Log in via AppRole and store a new client token. Returns the token."""
        url = f"{self.vault_addr}/v1/auth/approle/login"
        payload = {"role_id": self.role_id, "secret_id": self.secret_id}

        try:
            resp = self._http.post(url, json=payload, headers=self._headers())
        except httpx.HTTPError as e:
            raise VaultError(f"AppRole login request failed: {e}") from e

        if resp.status_code != 200:
            raise VaultError(f"AppRole login failed ({resp.status_code}): {resp.text}")

        auth = resp.json()["auth"]
        with self._lock:
            self._token = auth["client_token"]
            self._expires_at = time.time() + auth["lease_duration"]
        return self._token

    def get_token(self):
        """Return a valid token, creating a new one if none exists or it has expired."""
        if not self._token_is_valid():
            self.create_token()
        return self._token

    def get_secrets(self, path, version=None) -> dict:
        """
        Read a KV v2 secret.

        path:    path under the mount, e.g. "comm/brain/guardian/private"
        version: optional secret version
        key:     optional field to return (e.g. "key"); otherwise the whole dict

        If Vault answers 403 (token expired or revoked early), a new token is
        created and the request is retried once.
        """
        url = f"{self.vault_addr}/v1/{self.mount}/data/{path.lstrip('/')}"
        params = {"version": version} if version else None

        resp = self._read(url, params)
        if resp.status_code == 403:
            # Token may have expired or been revoked: get a new one and retry once
            self.create_token()
            resp = self._read(url, params)

        if resp.status_code == 404:
            raise VaultError(f"Secret not found: {path}")
        if resp.status_code != 200:
            raise VaultError(
                f"Failed to read '{path}' ({resp.status_code}): {resp.text}"
            )

        data = resp.json()["data"]["data"]
        return data

    # ------------------------------------------------------------------ internal
    def _read(self, url, params):
        try:
            return self._http.get(
                url, params=params, headers=self._headers(self.get_token())
            )
        except httpx.HTTPError as e:
            raise VaultError(f"Vault request failed: {e}") from e
