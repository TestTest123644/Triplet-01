"""Shared Kafka client authentication and configuration helper.

Builds client configuration dictionary for confluent-kafka Producer, AdminClient,
and Consumer, driven strictly by environment variables.
"""

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
    """Build shared security configuration dictionary for Kafka clients.

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
        # When protocol is PLAINTEXT, return only security.protocol with no sasl.* or ssl.* keys
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

    from twin.secrets import get_secret

    password = get_secret("KAFKA_PASSWORD")
    if password is None:
        raise ValueError(
            "Missing required environment variable for SASL authentication: KAFKA_PASSWORD or KAFKA_PASSWORD_FILE"
        )
    if not password:
        raise ValueError(
            "Environment variable KAFKA_PASSWORD is set but empty. Must provide non-empty password."
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

