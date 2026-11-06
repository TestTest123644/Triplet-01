"""Kafka client authentication helper for external mock clients.

Zero internal twin package imports.
Builds client configuration dictionary for confluent-kafka Consumer,
driven strictly by environment variables.
"""

from __future__ import annotations

import os
from typing import Dict


def kafka_client_config() -> Dict[str, str]:
    """Build shared security configuration dictionary for mock Kafka clients.

    Environment variables:
        KAFKA_SECURITY_PROTOCOL: Allowed: PLAINTEXT (default), SASL_PLAINTEXT, SASL_SSL.
        KAFKA_SASL_MECHANISM: Allowed: PLAIN (default), SCRAM-SHA-256, SCRAM-SHA-512.
        KAFKA_USERNAME: SASL username (required for SASL_* protocols).
        KAFKA_PASSWORD_FILE: Path to file holding password (Docker-secret style, trailing newline stripped).
        KAFKA_PASSWORD: Plaintext password literal (used if KAFKA_PASSWORD_FILE is unset).
                        If both are set, KAFKA_PASSWORD_FILE wins.

    Returns:
        Dict[str, str] of configuration parameters to pass into confluent_kafka clients.
        When protocol is PLAINTEXT, returns {"security.protocol": "PLAINTEXT"} with NO sasl.* keys.
    """
    protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").strip()
    allowed_protocols = ("PLAINTEXT", "SASL_PLAINTEXT", "SASL_SSL")
    if protocol not in allowed_protocols:
        raise ValueError(
            f"Invalid KAFKA_SECURITY_PROTOCOL: '{protocol}'. Allowed: {', '.join(allowed_protocols)}"
        )

    if protocol == "PLAINTEXT":
        return {"security.protocol": "PLAINTEXT"}

    mechanism = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN").strip()
    allowed_mechanisms = ("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512")
    if mechanism not in allowed_mechanisms:
        raise ValueError(
            f"Invalid KAFKA_SASL_MECHANISM: '{mechanism}'. Allowed: {', '.join(allowed_mechanisms)}"
        )

    username = os.getenv("KAFKA_USERNAME", "").strip()
    if not username:
        raise ValueError(
            "Missing required environment variable for SASL authentication: KAFKA_USERNAME"
        )

    password: str = ""
    pw_file = os.getenv("KAFKA_PASSWORD_FILE", "").strip()
    if pw_file:
        if not os.path.exists(pw_file):
            raise ValueError(
                f"Password file specified by KAFKA_PASSWORD_FILE does not exist: '{pw_file}'"
            )
        try:
            with open(pw_file, "r", encoding="utf-8") as f:
                content = f.read()
                password = content.rstrip("\r\n")
        except Exception as e:
            raise ValueError(
                f"Unreadable password file specified by KAFKA_PASSWORD_FILE '{pw_file}': {e}"
            )
        if not password:
            raise ValueError(
                f"Password file specified by KAFKA_PASSWORD_FILE is empty: '{pw_file}'"
            )
    elif "KAFKA_PASSWORD" in os.environ:
        password = os.environ["KAFKA_PASSWORD"]
        if not password:
            raise ValueError(
                "Environment variable KAFKA_PASSWORD is set but empty. Must provide non-empty password."
            )
    else:
        raise ValueError(
            "Missing required environment variable for SASL authentication: KAFKA_PASSWORD or KAFKA_PASSWORD_FILE"
        )

    return {
        "security.protocol": protocol,
        "sasl.mechanism": mechanism,
        "sasl.username": username,
        "sasl.password": password,
    }

