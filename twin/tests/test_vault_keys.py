"""Unit tests for HashiCorp Vault key synchronization in Twin Layer."""

from __future__ import annotations

import datetime
import hashlib
import json
import logging
import os
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, Optional

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, rsa, x25519
from starlette.testclient import TestClient

from twin.red_team_api import build_red_team_app
from twin.secrets import reload_secrets
from twin.simulation import CitySimulator
from twin.vault_keys import (
    VaultKeyManager,
    get_key,
    load_and_detect_key,
    set_manager,
    start_vault_sync,
    status,
    stop_vault_sync,
)


def _gen_x25519_pair():
    priv = x25519.X25519PrivateKey.generate()
    pem_priv = priv.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode("utf-8")
    pem_pub = (
        priv.public_key()
        .public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        .decode("utf-8")
    )
    return pem_priv, pem_pub


def _gen_rsa_pair():
    priv = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem_priv = priv.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode("utf-8")
    pem_pub = (
        priv.public_key()
        .public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        .decode("utf-8")
    )
    return pem_priv, pem_pub


class FakeVaultHandler(BaseHTTPRequestHandler):
    """Minimal fake HashiCorp Vault HTTP handler."""

    def log_message(self, format, *args):
        pass  # Silence stderr request logging

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/v1/auth/approle/login":
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length).decode("utf-8")) if length > 0 else {}
            role_id = body.get("role_id")
            secret_id = body.get("secret_id")

            if (
                role_id == self.server.expected_role_id
                and secret_id == self.server.expected_secret_id
            ):
                self.server.login_count += 1
                self.server.current_token = f"hvs.mock_token_{self.server.login_count}"
                resp = {
                    "auth": {
                        "client_token": self.server.current_token,
                        "lease_duration": 3600,
                        "renewable": True,
                    }
                }
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(resp).encode("utf-8"))
            else:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"errors":["invalid credentials"]}')
        else:
            self.send_response(404)
            self.end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        token = self.headers.get("X-Vault-Token")

        if self.server.force_403_once:
            self.server.force_403_once = False
            self.send_response(403)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"errors":["permission denied (test 403)"]}')
            return

        if not token or token != self.server.current_token:
            self.send_response(403)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"errors":["invalid or expired token"]}')
            return

        prefix = "/v1/secret/data/"
        if parsed.path.startswith(prefix):
            secret_path = parsed.path[len(prefix) :].strip("/")
            qs = urllib.parse.parse_qs(parsed.query)

            req_version = None
            if "version" in qs:
                try:
                    req_version = int(qs["version"][0])
                except ValueError:
                    pass

            if req_version is not None:
                versions_map = self.server.secret_versions.get(secret_path, {})
                data_block = versions_map.get(req_version)
            else:
                data_block = self.server.secrets.get(secret_path)

            if data_block is not None:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                resp = {"data": data_block}
                self.wfile.write(json.dumps(resp).encode("utf-8"))
            else:
                self.send_response(404)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"errors":["secret not found"]}')
        else:
            self.send_response(404)
            self.end_headers()


class FakeVaultServer:
    """In-memory test Vault server running on a local daemon thread."""

    def __init__(self, role_id: str = "mock-role", secret_id: str = "mock-secret"):
        self.httpd = HTTPServer(("127.0.0.1", 0), FakeVaultHandler)
        self.port = self.httpd.server_address[1]
        self.url = f"http://127.0.0.1:{self.port}"
        self.httpd.expected_role_id = role_id
        self.httpd.expected_secret_id = secret_id
        self.httpd.current_token = "hvs.mock_initial_token"
        self.httpd.login_count = 0
        self.httpd.force_403_once = False
        self.httpd.secrets = {}
        self.httpd.secret_versions = {}
        self._thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()

    def set_secret(
        self,
        path: str,
        data: Dict[str, Any],
        version: int = 1,
        created_time: str = "2026-11-07T12:00:00Z",
    ):
        data_block = {
            "data": data,
            "metadata": {
                "version": version,
                "created_time": created_time,
            },
        }
        self.httpd.secrets[path] = data_block
        if path not in self.httpd.secret_versions:
            self.httpd.secret_versions[path] = {}
        self.httpd.secret_versions[path][version] = data_block


@pytest.fixture
def fake_vault():
    server = FakeVaultServer(role_id="test-role", secret_id="test-secret")
    server.start()
    yield server
    server.stop()


def test_vault_disabled_when_addr_unset():
    """Vault manager is entirely disabled with no thread or network calls when VAULT_ADDR is empty."""
    mgr = VaultKeyManager(vault_addr="")
    assert mgr.enabled is False
    st = mgr.status()
    assert st["enabled"] is False
    assert st["status"] == "disabled"
    assert st["last_error"] is None
    assert st["paths"] == {}
    assert mgr.get_key("comm/twin/guardian/private") is None

    mgr.start()
    assert mgr._thread is None
    mgr.stop()


def test_login_and_read_pem_key_format_detects_x25519(fake_vault, monkeypatch):
    """Login and read of all four paths in 'pem-key' format detects X25519 algorithm."""
    priv1, pub1 = _gen_x25519_pair()
    priv2, pub2 = _gen_x25519_pair()

    fake_vault.set_secret("comm/twin/guardian/private", {"key": priv1}, version=1)
    fake_vault.set_secret("comm/twin/brain/private", {"key": priv2}, version=1)
    fake_vault.set_secret("comm/guardian/twin/public", {"key": pub1}, version=1)
    fake_vault.set_secret("comm/brain/twin/public", {"key": pub2}, version=1)

    monkeypatch.setenv("VAULT_ROLE_ID", "test-role")
    monkeypatch.setenv("VAULT_SECRET_ID", "test-secret")

    mgr = VaultKeyManager(
        vault_addr=fake_vault.url,
        refresh_seconds=0,
    )
    changed, err = mgr.refresh()
    assert err is None
    assert changed is True

    st = mgr.status()
    assert st["enabled"] is True
    assert st["status"] == "ok"
    assert st["last_error"] is None
    assert len(st["paths"]) == 4

    for path in (
        "comm/twin/guardian/private",
        "comm/twin/brain/private",
        "comm/guardian/twin/public",
        "comm/brain/twin/public",
    ):
        p_info = st["paths"][path]
        assert p_info["algorithm"] == "X25519"
        assert p_info["format"] == "pem-key"
        assert p_info["version"] == 1
        assert p_info["fingerprint_ok"] is None
        key_obj = mgr.get_key(path)
        assert key_obj is not None


def test_kid_format_fingerprint_and_active_after_rule(fake_vault, monkeypatch):
    """Kid format evaluates fingerprint and picks version-1 when active_after is in future."""
    monkeypatch.setenv("VAULT_ROLE_ID", "test-role")
    monkeypatch.setenv("VAULT_SECRET_ID", "test-secret")

    priv_rsa_v1, pub_rsa_v1 = _gen_rsa_pair()
    priv_rsa_v2, pub_rsa_v2 = _gen_rsa_pair()

    fp_pub_v2 = hashlib.sha256(pub_rsa_v2.encode("utf-8")).hexdigest()
    future_time = "2099-01-01T00:00:00Z"
    past_time = "2020-01-01T00:00:00Z"

    # Private key: v1 is past, v2 is future -> active_after rule picks v1
    priv_path = "comm/twin/guardian/private"
    fake_vault.set_secret(
        priv_path,
        {
            "kid": "kid-priv-v1",
            "private_key": priv_rsa_v1,
            "active_after": past_time,
        },
        version=1,
    )
    fake_vault.set_secret(
        priv_path,
        {
            "kid": "kid-priv-v2",
            "private_key": priv_rsa_v2,
            "active_after": future_time,
        },
        version=2,
    )

    # Public key: v2 has valid fingerprint; keeps latest (v2) and previous (v1)
    pub_path = "comm/guardian/twin/public"
    fake_vault.set_secret(
        pub_path,
        {
            "kid": "kid-pub-v1",
            "public_key": pub_rsa_v1,
            "fingerprint": "bad-fingerprint",
        },
        version=1,
    )
    fake_vault.set_secret(
        pub_path,
        {
            "kid": "kid-pub-v2",
            "public_key": pub_rsa_v2,
            "fingerprint": fp_pub_v2,
        },
        version=2,
    )

    mgr = VaultKeyManager(
        vault_addr=fake_vault.url,
        paths=[priv_path, pub_path],
        refresh_seconds=0,
    )
    mgr.refresh()

    st = mgr.status()
    # Private key active version should be 1 (due to v2 future active_after)
    priv_st = st["paths"][priv_path]
    assert priv_st["version"] == 1
    assert priv_st["kid"] == "kid-priv-v1"
    assert priv_st["algorithm"] == "RSA-2048"

    # Public key latest should be v2 with valid fingerprint
    pub_st = st["paths"][pub_path]
    assert pub_st["version"] == 2
    assert pub_st["kid"] == "kid-pub-v2"
    assert pub_st["fingerprint_ok"] is True
    assert pub_st["previous_version"] == 1

    # Key objects: previous vs latest
    latest_pub_key = mgr.get_key(pub_path)
    prev_pub_key = mgr.get_key(pub_path, version="previous")
    assert latest_pub_key is not None
    assert prev_pub_key is not None
    assert latest_pub_key != prev_pub_key


def test_403_on_read_triggers_relogin(fake_vault, monkeypatch):
    """HTTP 403 on read forces re-login and transparently succeeds."""
    priv, pub = _gen_x25519_pair()
    fake_vault.set_secret("comm/twin/guardian/private", {"key": priv}, version=1)

    monkeypatch.setenv("VAULT_ROLE_ID", "test-role")
    monkeypatch.setenv("VAULT_SECRET_ID", "test-secret")

    mgr = VaultKeyManager(
        vault_addr=fake_vault.url,
        paths=["comm/twin/guardian/private"],
        refresh_seconds=0,
    )
    mgr.refresh()
    assert fake_vault.httpd.login_count == 1

    # Simulate token revocation / expiration on next read
    fake_vault.httpd.force_403_once = True
    changed, err = mgr.refresh()
    assert err is None
    assert fake_vault.httpd.login_count == 2
    assert mgr.status()["status"] == "ok"


def test_unreachable_vault_continues_startup_and_reports_error(monkeypatch):
    """Unreachable Vault does not crash startup; status shows last_error."""
    monkeypatch.setenv("VAULT_ROLE_ID", "test-role")
    monkeypatch.setenv("VAULT_SECRET_ID", "test-secret")

    # Connect to an unreachable local port
    mgr = VaultKeyManager(
        vault_addr="http://127.0.0.1:59999",
        refresh_seconds=0,
    )
    # start() must not raise
    mgr.start()
    st = mgr.status()
    assert st["enabled"] is True
    assert st["status"] == "degraded"
    assert st["last_error"] is not None
    assert "PRIVATE KEY" not in st["last_error"]
    assert "token" not in st["last_error"].lower()
    mgr.stop()


def test_refresh_picks_up_new_version(fake_vault, monkeypatch):
    """Refresh picks up updated secret versions dynamically."""
    monkeypatch.setenv("VAULT_ROLE_ID", "test-role")
    monkeypatch.setenv("VAULT_SECRET_ID", "test-secret")

    priv1, _ = _gen_x25519_pair()
    priv2, _ = _gen_x25519_pair()

    path = "comm/twin/guardian/private"
    fake_vault.set_secret(path, {"key": priv1}, version=1)

    mgr = VaultKeyManager(
        vault_addr=fake_vault.url,
        paths=[path],
        refresh_seconds=0,
    )
    changed, _ = mgr.refresh()
    assert changed is True
    assert mgr.status()["paths"][path]["version"] == 1

    # Advance version in Vault
    fake_vault.set_secret(path, {"key": priv2}, version=2)
    changed, _ = mgr.refresh()
    assert changed is True
    assert mgr.status()["paths"][path]["version"] == 2


def test_reload_secrets_includes_vault_keys(fake_vault, monkeypatch):
    """twin.secrets.reload_secrets includes VAULT_KEYS in result dictionary."""
    monkeypatch.setenv("VAULT_ROLE_ID", "test-role")
    monkeypatch.setenv("VAULT_SECRET_ID", "test-secret")

    priv, _ = _gen_x25519_pair()
    fake_vault.set_secret("comm/twin/guardian/private", {"key": priv}, version=1)

    mgr = VaultKeyManager(
        vault_addr=fake_vault.url,
        paths=["comm/twin/guardian/private"],
        refresh_seconds=0,
    )
    set_manager(mgr)
    try:
        res = reload_secrets()
        assert "VAULT_KEYS" in res["results"]
        assert res["results"]["VAULT_KEYS"] in ("changed", "unchanged")
    finally:
        set_manager(None)


def test_api_status_contains_vault_block_without_secrets(fake_vault, monkeypatch):
    """GET /api/status contains vault block and does not leak tokens or private keys."""
    monkeypatch.setenv("VAULT_ROLE_ID", "test-role")
    monkeypatch.setenv("VAULT_SECRET_ID", "test-secret")

    priv, pub = _gen_x25519_pair()
    fake_vault.set_secret("comm/twin/guardian/private", {"key": priv}, version=1)
    fake_vault.set_secret("comm/guardian/twin/public", {"key": pub}, version=1)

    mgr = VaultKeyManager(
        vault_addr=fake_vault.url,
        paths=["comm/twin/guardian/private", "comm/guardian/twin/public"],
        refresh_seconds=0,
    )
    mgr.refresh()
    set_manager(mgr)

    try:
        sim = CitySimulator(seed=101)
        app = build_red_team_app(sim, token="test-rt-token")
        client = TestClient(app)

        resp = client.get("/api/status", headers={"X-Red-Team-Token": "test-rt-token"})
        assert resp.status_code == 200
        data = resp.json()
        assert "vault" in data
        assert data["vault"]["enabled"] is True
        assert len(data["vault"]["paths"]) == 2

        raw_json = json.dumps(data)
        assert "PRIVATE KEY" not in raw_json
        assert "BEGIN" not in raw_json
        assert fake_vault.httpd.current_token not in raw_json
    finally:
        set_manager(None)


def test_no_log_records_contain_token_or_begin(fake_vault, monkeypatch, caplog):
    """Logging during Vault operations never includes key material or client tokens."""
    monkeypatch.setenv("VAULT_ROLE_ID", "test-role")
    monkeypatch.setenv("VAULT_SECRET_ID", "test-secret")

    priv, _ = _gen_x25519_pair()
    fake_vault.set_secret("comm/twin/guardian/private", {"key": priv}, version=1)

    mgr = VaultKeyManager(
        vault_addr=fake_vault.url,
        paths=["comm/twin/guardian/private"],
        refresh_seconds=0,
    )

    with caplog.at_level(logging.DEBUG):
        mgr.refresh()

    for record in caplog.records:
        msg = record.getMessage()
        assert "BEGIN" not in msg
        assert "PRIVATE KEY" not in msg
        assert fake_vault.httpd.current_token not in msg

