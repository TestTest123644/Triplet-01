"""Kafka client authentication helper for external mock clients.

Zero internal twin package imports.
Builds client configuration dictionary for confluent-kafka Consumer,
driven strictly by environment variables.
"""

from __future__ import annotations

import os
from typing import Dict


def get_broker_address_family() -> str:
    """Parse and validate KAFKA_BROKER_ADDRESS_FAMILY environment variable.

    Returns:
        One of 'any' (default), 'v4', 'v6'.
        Raises ValueError if invalid.
    """
    raw = (os.getenv("KAFKA_BROKER_ADDRESS_FAMILY", "any") or "any").strip().lower() or "any"
    allowed = ("any", "v4", "v6")
    if raw not in allowed:
        raise ValueError(
            f"Invalid KAFKA_BROKER_ADDRESS_FAMILY: '{raw}'. Allowed: {', '.join(allowed)}"
        )
    return raw


def kafka_client_config() -> Dict[str, str]:
    """Build shared security configuration dictionary for mock Kafka clients.

    Environment variables:
        KAFKA_SECURITY_PROTOCOL: Allowed: PLAINTEXT (default), SASL_PLAINTEXT, SASL_SSL.
        KAFKA_SASL_MECHANISM: Allowed: PLAIN (default), SCRAM-SHA-256, SCRAM-SHA-512.
        KAFKA_USERNAME: SASL username (required for SASL_* protocols).
        KAFKA_PASSWORD_FILE: Path to file holding password (Docker-secret style, trailing newline stripped).
        KAFKA_PASSWORD: Plaintext password literal (used if KAFKA_PASSWORD_FILE is unset).
                        If both are set, KAFKA_PASSWORD_FILE wins.
        KAFKA_SSL_CA_LOCATION: Path to CA certificate file (optional, only used for SASL_SSL).

    Returns:
        Dict[str, str] of configuration parameters to pass into confluent_kafka clients.
        When protocol is PLAINTEXT, returns {"security.protocol": "PLAINTEXT"} with NO sasl.* or ssl.* keys.
    """
    protocol = (os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT") or "PLAINTEXT").strip() or "PLAINTEXT"
    allowed_protocols = ("PLAINTEXT", "SASL_PLAINTEXT", "SASL_SSL")
    if protocol not in allowed_protocols:
        raise ValueError(
            f"Invalid KAFKA_SECURITY_PROTOCOL: '{protocol}'. Allowed: {', '.join(allowed_protocols)}"
        )

    if protocol == "PLAINTEXT":
        return {"security.protocol": "PLAINTEXT"}

    mechanism = (os.getenv("KAFKA_SASL_MECHANISM", "PLAIN") or "PLAIN").strip() or "PLAIN"
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

    config: Dict[str, str] = {
        "security.protocol": protocol,
        "sasl.mechanism": mechanism,
        "sasl.username": username,
        "sasl.password": password,
    }

    if protocol == "SASL_SSL":
        ca_location = os.getenv("KAFKA_SSL_CA_LOCATION", "").strip()
        if ca_location:
            if not os.path.exists(ca_location):
                raise ValueError(
                    f"CA file specified by KAFKA_SSL_CA_LOCATION does not exist: '{ca_location}'"
                )
            config["ssl.ca.location"] = ca_location

    return config

