#!/usr/bin/env bash
set -euo pipefail


PARTIES=(brain guardian twin)

for A in "${PARTIES[@]}"; do
  for B in "${PARTIES[@]}"; do
    [[ "$A" == "$B" ]] && continue

    # Generate the keypair in memory (nothing written to disk)
    PRIV=$(openssl genpkey -algorithm X25519)
    PUB=$(printf '%s\n' "$PRIV" | openssl pkey -pubout)

    # Pass via stdin as JSON so keys never appear in the process list
    jq -n --arg k "$PRIV" '{key:$k}' \
      | vault kv put -mount=secret "comm/$A/$B/private" -

    jq -n --arg k "$PUB" '{key:$k}' \
      | vault kv put -mount=secret "comm/$A/$B/public" -

    echo "created keypair $A -> $B"
  done
done