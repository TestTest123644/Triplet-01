# Vault access policy and key rotation (issue #111)

## 1. Scope

Access control, secret isolation and automatic 24-hour rotation of the
communication keys between the guardian, twin and brain services.
The initial requirement mentioned weekly rotation; the final requirement
(24 h) is applied to all communication keys.

## 2. Identities and policies

- guardian-app, twin-app, brain-app: one AppRole per service, read-only on its own paths (section 3).
- rotator: used by the rotation job. It can create and update secrets under secret/data/comm/*
  and read metadata, but it cannot read key values.

Services authenticate with AppRole and get short-lived tokens
(token_ttl=15m, token_max_ttl=1h). Policies live in vault/policies/
and are applied by vault/setup_policies.py (idempotent).

## 3. Key ownership and isolation

Each direction of each relationship has its own RSA key pair, owned by the sender.

- secret/comm/OWNER/PEER/private is readable by OWNER only.
- secret/comm/OWNER/PEER/public is readable by PEER only.

Per service:

- guardian reads the private keys guardian/twin and guardian/brain,
  and the public keys twin/guardian and brain/guardian.
- twin reads the private keys twin/guardian and twin/brain,
  and the public keys guardian/twin and brain/twin.
- brain reads the private keys brain/guardian and brain/twin,
  and the public keys guardian/brain and twin/brain.

A service can neither write nor read anything outside this list (default deny).
Public keys are integrity-protected by Vault (TLS in transit, KV versioning,
audit log) and are never accepted from any other channel.

## 4. Rotation (every 24 h)

vault/rotate_keys.py runs from a systemd timer (vault/systemd/, 02:00 daily).
For each of the six key pairs it:

1. generates a new RSA key pair (4096 bits);
2. writes the new public key (new KV version, unique kid);
3. writes the new private key with active_after = now + 1h.

Safe transition (no incompatible versions):

- peers accept the latest and previous public key versions (vault_keys.verification_keys);
- the owner keeps signing with the previous private key until active_after has passed
  (vault_keys.signing_key), so peers already know the new public key when it is first used;
- KV keeps 5 versions per secret; older ones are pruned by Vault.

Failure handling: a failing pair does not block the others. The script logs the error,
calls ALERT_WEBHOOK_URL if set, and exits with code 1 (the unit shows as failed).
The previous key stays valid, so a failed rotation causes no outage.
Manual re-run: systemctl start key-rotation.service

## 5. Audit

A file audit device is enabled by setup_policies.py (/vault/logs/audit.log).
It records every authentication and every secret read or write. Logs must be kept on
a persistent volume, shipped off the host, and rotated with logrotate.
Failed (403) requests must be reviewed regularly.

## 6. Revocation

- Token leaked: vault token revoke TOKEN (or by accessor).
- AppRole secret_id leaked: destroy it with
  vault write auth/approle/role/ROLE/secret-id/destroy secret_id=ID, then issue a new one.
- Service compromised: revoke its tokens, destroy its secret_ids, run rotate_keys.py
  immediately, then destroy the old key versions with vault kv destroy -versions=N.
- Role retired: vault delete auth/approle/role/ROLE and remove its policy.

## 7. Failure recovery

- Rotation failed: fix the cause (Vault sealed, network, expired rotator secret_id)
  and re-run the unit. Nothing else is required.
- A service holds an unknown key version: it re-reads the latest and previous versions;
  if still unknown, run a rotation to resynchronise.
- Vault sealed or restarted: unseal, then run the rotation unit.
- Wrong rotation written: vault kv rollback -version=N on the affected path.

## 8. Open points

- AppRole secret_id_ttl is 24 h: services and the rotator must obtain a fresh secret_id
  at deploy time or through response wrapping (see vault_guide.md).
- Alerting relies on a webhook; the target (Slack, mail) must be chosen by the team.
- Keys are stored in KV v2, so a service holds its private key in memory. The Transit engine
  (keys never leave Vault) is the stronger alternative if the team accepts a Vault round-trip
  for each operation.

## 9. Tests

tests/vault_keys/ runs against a disposable Vault in dev mode:

    docker network create vault-test-net
    docker run --rm -d --name vault-test --network vault-test-net --cap-add=IPC_LOCK \
      -p 127.0.0.1:8300:8200 -e VAULT_DEV_ROOT_TOKEN_ID=dev-root \
      -e VAULT_DEV_LISTEN_ADDRESS=0.0.0.0:8200 hashicorp/vault:1.17
    export VAULT_TEST_ADDR=http://127.0.0.1:8300 VAULT_TEST_TOKEN=dev-root
    uv run pytest tests/vault_keys -v

Without those two variables the tests are skipped.
