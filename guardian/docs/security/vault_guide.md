# HashiCorp Vault Guide: Architecture, Operations and Python (`hvac`) Integration

> Scope: documentation for issue #90. It describes how our Vault deployment works and how Python microservices should talk to it. It does not add any wrapper API or application endpoint.
>
> All values below (hostnames, role names, key names) are examples. Never put real secrets, tokens, unseal keys or private keys in this file or in the repository.

## 1. Architecture and operations overview

### 1.1 Cluster topology

Vault runs as a cluster of nodes that use **integrated storage (Raft)**.

| Concept | How it works |
|---|---|
| **Raft consensus** | The nodes elect one **leader**. Every write goes through the leader and is replicated to the followers. A write is committed once a **quorum** (`n/2 + 1` nodes) has stored it. |
| **Fault tolerance** | 3 nodes tolerate 1 failure, 5 nodes tolerate 2. Use an odd number of nodes. |
| **Active / standby** | The Raft leader is the *active* Vault node. The others are *standby*: they forward client requests to the active node and take over if it fails. |
| **Ports** | `8200` is the API (clients, CLI, `hvac`). `8201` is the cluster port used for node-to-node traffic (request forwarding and Raft replication). |
| **Joining nodes** | New nodes join with `retry_join` in the configuration, or with `vault operator raft join`. |

> **About Serf.** Serf gossip is used by Consul, not by Vault. Vault with integrated storage does not run a gossip layer: nodes find each other through `retry_join` / `raft join`, and failure detection comes from Raft heartbeats and leader election. If you are looking for membership information, use `vault operator raft list-peers`.

Example node configuration (`vault.hcl`):

```hcl
storage "raft" {
  path    = "/vault/data"
  node_id = "vault-1"

  retry_join { leader_api_addr = "https://vault-2:8200" }
  retry_join { leader_api_addr = "https://vault-3:8200" }
}

listener "tcp" {
  address         = "0.0.0.0:8200"
  cluster_address = "0.0.0.0:8201"
  tls_cert_file   = "/vault/tls/vault.crt"
  tls_key_file    = "/vault/tls/vault.key"
}

api_addr     = "https://vault-1:8200"
cluster_addr = "https://vault-1:8201"
```

Useful commands for operators:

```bash
vault status                       # Sealed? Initialized? HA mode, active node
vault operator raft list-peers     # leader and followers
vault operator raft snapshot save backup.snap
```

### 1.2 Security model

**TLS.** The API listener serves HTTPS. Clients must trust the CA that signed the Vault certificate and verify the hostname. In Python this means passing the CA bundle path to `hvac` (see section 2.1). Never disable verification. Node-to-node traffic on port 8201 is protected by certificates that Vault generates itself.

**Encryption at rest.** Everything Vault writes to its storage backend (the Raft data directory) is encrypted with AES-256-GCM using Vault's internal *encryption key*. That key is itself encrypted by the *root key*. Someone who steals the disk or a snapshot only sees ciphertext.

**Seal and unseal.** After a restart Vault is **sealed**: it knows where the data is but cannot decrypt it. During `vault operator init` the root key is split with Shamir's Secret Sharing (for example 5 key shares, 3 needed). Unsealing means providing enough shares:

```bash
vault operator unseal      # run with a different share each time until Sealed = false
```

Rules for key material:

- Unseal key shares and the initial root token are shown once by `vault operator init`. Give each share to a different person and store it in a password manager or HSM, never in Git, `.env` files, CI variables or `docker-compose.yml`.
- Revoke the initial root token after the first configuration and use scoped tokens (`vault token revoke <root-token>`).
- Consider auto-unseal (cloud KMS or another Vault) to avoid manual unseal after every restart.

**Secrets engines used.**

- **KV v2** (mounted at `secret/`): versioned application secrets such as database credentials.
- **Transit** (mounted at `transit/`): cryptography as a service. Keys, for example one RSA-4096 key per department, never leave Vault. Services send ciphertext and receive plaintext (section 3).

**Audit.** Enable at least one audit device so every request is logged:

```bash
vault audit enable file file_path=/vault/logs/audit.log
```

### 1.3 Access control and AppRole workflow

Access is controlled by **policies** (what a token may do) and **auth methods** (how a client proves who it is). Services use the **AppRole** auth method.

1. An operator writes a least-privilege policy:

   ```hcl
   # guardian-app.hcl
   path "secret/data/guardian/*"     { capabilities = ["read"] }
   path "secret/metadata/guardian/*" { capabilities = ["read", "list"] }
   path "transit/decrypt/dept-*"     { capabilities = ["update"] }
   ```

   KV v2 paths contain `data/` (values) and `metadata/` (listing, versions). To let a service write, add `create` and `update` on `secret/data/guardian/*`; for `kv patch` the capability is `patch`.

2. The operator creates the role and attaches the policy:

   ```bash
   vault policy write guardian-app guardian-app.hcl
   vault auth enable approle
   vault write auth/approle/role/guardian-app \
       token_policies="guardian-app" \
       token_ttl=15m token_max_ttl=1h \
       secret_id_ttl=24h
   ```

3. Two credentials are issued:

   - **Role ID**: a stable identifier of the role. It is not secret on its own and can ship with the deployment configuration.
   - **Secret ID**: the actual credential. Generate it at deploy time, inject it into the container as a runtime secret (never bake it into an image or commit it), and give it a limited lifetime (`secret_id_ttl`) or a limited number of uses (`secret_id_num_uses`).

   ```bash
   vault read  -field=role_id   auth/approle/role/guardian-app/role-id
   vault write -f -field=secret_id auth/approle/role/guardian-app/secret-id
   ```

4. At startup the service sends `role_id` + `secret_id` to `auth/approle/login` and receives a **client token** limited to the role's policies and TTL. It uses this token for all further requests, renews it before it expires and re-authenticates when it can no longer be renewed.

```
service ──(role_id + secret_id, HTTPS)──▶ Vault auth/approle/login
        ◀──────── client token (policies: guardian-app, TTL 15m) ────────
service ──(X-Vault-Token)──▶ secret/data/guardian/db   (KV v2)
service ──(X-Vault-Token)──▶ transit/decrypt/dept-xxx  (Transit)
```

## 2. Python (`hvac`) integration guide

Install the client (hvac 2.x):

```bash
pip install "hvac>=2.0"
```

The examples read their configuration from environment variables:

| Variable | Meaning | Example |
|---|---|---|
| `VAULT_ADDR` | Vault API address (HTTPS) | `https://vault-1:8200` |
| `VAULT_CACERT` | Path to the CA certificate that signed Vault's certificate | `/etc/ssl/vault/ca.crt` |
| `VAULT_ROLE_ID` | AppRole Role ID | provided with the deployment |
| `VAULT_SECRET_ID` | AppRole Secret ID | injected at runtime, never committed |

### 2.1 Client initialization over HTTPS

```python
import os

import hvac


def make_client() -> hvac.Client:
    """Create a Vault client that verifies the server certificate."""
    return hvac.Client(
        url=os.environ["VAULT_ADDR"],
        # Path to the CA bundle. If unset, the system trust store is used.
        # Never pass verify=False.
        verify=os.environ.get("VAULT_CACERT", True),
        timeout=10,
    )
```

### 2.2 AppRole authentication

```python
def login(client: hvac.Client) -> int:
    """Authenticate with AppRole. Returns the token lifetime in seconds."""
    response = client.auth.approle.login(
        role_id=os.environ["VAULT_ROLE_ID"],
        secret_id=os.environ["VAULT_SECRET_ID"],
    )
    # hvac stores the returned token on the client automatically.
    if not client.is_authenticated():
        raise RuntimeError("AppRole login succeeded but the token is not valid")
    return response["auth"]["lease_duration"]
```

### 2.3 Reading a secret (KV v2)

```python
from hvac.exceptions import InvalidPath


def read_secret(client: hvac.Client, path: str, mount_point: str = "secret"):
    """Return the key/value data of a KV v2 secret, or None if it does not exist."""
    try:
        response = client.secrets.kv.v2.read_secret_version(
            path=path,
            mount_point=mount_point,
            raise_on_deleted_version=True,
        )
    except InvalidPath:
        return None
    # KV v2 nests the payload: response["data"]["data"] holds your key/values,
    # response["data"]["metadata"] holds the version information.
    return response["data"]["data"]
```

### 2.4 Writing and updating a secret (if the policy allows it)

```python
def write_secret(
    client: hvac.Client, path: str, data: dict, mount_point: str = "secret"
):
    """Create a new version of the secret. Replaces all keys of the previous version."""
    return client.secrets.kv.v2.create_or_update_secret(
        path=path, secret=data, mount_point=mount_point
    )


def patch_secret(
    client: hvac.Client, path: str, data: dict, mount_point: str = "secret"
):
    """Update only the given keys and keep the others (needs the `patch` capability)."""
    return client.secrets.kv.v2.patch(path=path, secret=data, mount_point=mount_point)
```

Without the right capability Vault answers `403` and `hvac` raises `hvac.exceptions.Forbidden`.

### 2.5 Token lifecycle: renewal, re-authentication and cleanup

A client token is only valid for its TTL and never lives beyond `token_max_ttl`. A long-running service therefore has to:

1. renew the token before it expires (here at two thirds of the TTL),
2. log in again with AppRole when renewal fails or the token reached its maximum TTL,
3. revoke the token and close the HTTP session on shutdown.

```python
import logging
import threading

from hvac.exceptions import VaultError

log = logging.getLogger("vault")


class VaultSession:
    """Authenticated Vault client that keeps its token alive in a background thread."""

    def __init__(self):
        self.client = make_client()
        self._base_ttl = login(self.client)
        self._ttl = self._base_ttl
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._renew_loop, daemon=True)
        self._thread.start()

    def _renew_loop(self):
        # Event.wait returns True when close() was called, which ends the loop.
        while not self._stop.wait(max(self._ttl * 2 / 3, 1)):
            try:
                lease = self.client.auth.token.renew_self()["auth"]["lease_duration"]
                if lease < self._base_ttl:
                    # The lease was capped by token_max_ttl: get a fresh token now.
                    self._ttl = login(self.client)
                else:
                    self._ttl = lease
            except Exception as exc:  # token expired, Vault unreachable, ...
                log.warning("token renewal failed (%s), logging in again", exc)
                try:
                    self._ttl = login(self.client)
                except Exception as login_exc:
                    log.error("AppRole login failed: %s", login_exc)
                    self._ttl = 5  # retry soon

    def close(self):
        """Stop renewal, revoke the token and close the HTTP session."""
        self._stop.set()
        self._thread.join(timeout=5)
        try:
            self.client.auth.token.revoke_self()
        except VaultError:
            pass  # already expired or revoked
        finally:
            self.client.adapter.close()
```

Typical usage in a service:

```python
import atexit

session = VaultSession()
atexit.register(session.close)

db_config = read_secret(session.client, "guardian/db")
```

Best practices:

- Keep token and secret values out of logs, exception messages and error trackers.
- Cache secrets in memory for a short time instead of calling Vault on every request, and re-read them when they change.
- Use short TTLs (minutes) and a limited `secret_id_ttl`; handle `Forbidden` and `VaultDown` with bounded retries.
- Call `close()` on shutdown (`atexit`, SIGTERM handler, FastAPI/Flask shutdown hook).

## 3. Decrypting with the department key (Transit)

The RSA-4096 key of each department lives in the Transit engine, so the application never holds the private key. The service sends the ciphertext to Vault and receives the plaintext.

Transit expects ciphertext in the form `vault:v<key version>:<base64>`. If a producer encrypted outside Vault with the department's **public** key (RSA-OAEP with SHA-256), wrap the raw bytes into that format before decrypting:

```python
import base64


def wrap_rsa_ciphertext(raw: bytes, key_version: int = 1) -> str:
    """Turn raw RSA-OAEP(SHA-256) ciphertext into Transit's `vault:vN:...` format."""
    return f"vault:v{key_version}:" + base64.b64encode(raw).decode()


def decrypt_for_department(
    client: hvac.Client, department: str, ciphertext: str
) -> bytes:
    """Decrypt `ciphertext` (`vault:vN:...`) with the department's Transit key."""
    response = client.secrets.transit.decrypt_data(
        name=f"dept-{department}",
        ciphertext=ciphertext,
        mount_point="transit",
    )
    return base64.b64decode(response["data"]["plaintext"])
```

The token only needs `update` on `transit/decrypt/dept-*`; do not grant `read` on the key or `export` rights to services. The OAEP hash must be SHA-256 (the hash Transit uses); ciphertext produced with another hash fails to decrypt.

## 4. Troubleshooting

| Symptom | Likely cause |
|---|---|
| `SSLError` / certificate verify failed | `VAULT_CACERT` is missing or does not match the CA that signed Vault's certificate, or the hostname is not in the certificate SAN |
| `hvac.exceptions.InvalidRequest: invalid role or secret ID` | Wrong `role_id`, expired or used-up `secret_id` |
| `hvac.exceptions.Forbidden` on read or write | The policy does not allow that path; remember the `data/` segment for KV v2 |
| `hvac.exceptions.InvalidPath` | The secret or mount does not exist (check `vault secrets list`) |
| `hvac.exceptions.VaultDown` | Vault is sealed or the node is unreachable (`vault status`) |
| Transit `ciphertext ... invalid` | Missing `vault:vN:` prefix, wrong key name, or wrong OAEP hash |

## 5. Security checklist

- [ ] TLS enabled and clients verify the certificate (`verify` points to the CA, never `False`)
- [ ] Initial root token revoked, unseal key shares held by different people and stored outside the repository
- [ ] Least-privilege policies and short token TTLs
- [ ] Secret ID injected at runtime, absent from images, CI logs and `docker-compose.yml`
- [ ] Audit device enabled
- [ ] Raft snapshots scheduled and restore procedure tested
