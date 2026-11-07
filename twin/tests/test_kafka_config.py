"""Tests for Kafka client configuration and SASL authentication helper (Phase 7C)."""

import os
import tempfile
import pytest
from unittest.mock import MagicMock, patch

from twin.kafka_config import get_broker_address_family, kafka_client_config
from twin.kafka_streamer import KafkaStreamer
from twin.main import wait_for_kafka_ready
from mocks.reading_source import KafkaSource


def test_kafka_client_config_plaintext(monkeypatch):
    """PLAINTEXT protocol gives no sasl.* keys."""
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    conf = kafka_client_config()
    assert conf.get("security.protocol") == "PLAINTEXT"
    sasl_keys = [k for k in conf if k.startswith("sasl.")]
    assert len(sasl_keys) == 0, f"Expected no sasl keys for PLAINTEXT, found: {sasl_keys}"


def test_kafka_client_config_plaintext_default(monkeypatch):
    """When KAFKA_SECURITY_PROTOCOL is unset, defaults to PLAINTEXT with no sasl keys."""
    monkeypatch.delenv("KAFKA_SECURITY_PROTOCOL", raising=False)
    conf = kafka_client_config()
    assert conf.get("security.protocol") == "PLAINTEXT"
    sasl_keys = [k for k in conf if k.startswith("sasl.")]
    assert len(sasl_keys) == 0


def test_kafka_client_config_sasl_plaintext_with_password_file(monkeypatch):
    """SASL_PLAINTEXT with password file returns the exact 4-key config with stripped newline."""
    with tempfile.NamedTemporaryFile("w", delete=False) as f:
        f.write("super-secret-password-123\r\n")
        pw_file = f.name

    try:
        monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
        monkeypatch.setenv("KAFKA_SASL_MECHANISM", "PLAIN")
        monkeypatch.setenv("KAFKA_USERNAME", "sensor_writer")
        monkeypatch.setenv("KAFKA_PASSWORD_FILE", pw_file)
        monkeypatch.delenv("KAFKA_PASSWORD", raising=False)

        expected = {
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanism": "PLAIN",
            "sasl.username": "sensor_writer",
            "sasl.password": "super-secret-password-123",
        }
        actual = kafka_client_config()
        assert actual == expected
    finally:
        if os.path.exists(pw_file):
            os.remove(pw_file)


def test_kafka_client_config_file_wins_over_literal(monkeypatch):
    """When both KAFKA_PASSWORD_FILE and KAFKA_PASSWORD are set, the file wins."""
    with tempfile.NamedTemporaryFile("w", delete=False) as f:
        f.write("file-password\n")
        pw_file = f.name

    try:
        monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
        monkeypatch.setenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256")
        monkeypatch.setenv("KAFKA_USERNAME", "ssl_writer")
        monkeypatch.setenv("KAFKA_PASSWORD_FILE", pw_file)
        monkeypatch.setenv("KAFKA_PASSWORD", "literal-password")

        conf = kafka_client_config()
        assert conf["sasl.password"] == "file-password"
        assert conf["security.protocol"] == "SASL_SSL"
        assert conf["sasl.mechanism"] == "SCRAM-SHA-256"
    finally:
        if os.path.exists(pw_file):
            os.remove(pw_file)


def test_kafka_client_config_missing_username_raises(monkeypatch):
    """Missing KAFKA_USERNAME under SASL protocol raises ValueError naming the variable."""
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
    monkeypatch.delenv("KAFKA_USERNAME", raising=False)
    monkeypatch.setenv("KAFKA_PASSWORD", "some-pw")

    with pytest.raises(ValueError, match="KAFKA_USERNAME"):
        kafka_client_config()


def test_kafka_client_config_missing_password_raises(monkeypatch):
    """Missing password under SASL protocol raises ValueError naming the variable."""
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
    monkeypatch.setenv("KAFKA_USERNAME", "some_user")
    monkeypatch.delenv("KAFKA_PASSWORD", raising=False)
    monkeypatch.delenv("KAFKA_PASSWORD_FILE", raising=False)

    with pytest.raises(ValueError, match="KAFKA_PASSWORD"):
        kafka_client_config()


def test_kafka_client_config_unreadable_password_file_raises(monkeypatch):
    """Non-existent or unreadable KAFKA_PASSWORD_FILE raises ValueError naming the variable."""
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
    monkeypatch.setenv("KAFKA_USERNAME", "some_user")
    monkeypatch.setenv("KAFKA_PASSWORD_FILE", "/nonexistent/path/to/kafka.secret")

    with pytest.raises(ValueError, match="KAFKA_PASSWORD_FILE"):
        kafka_client_config()


def test_kafka_client_config_invalid_protocol_raises(monkeypatch):
    """Invalid protocol raises ValueError naming KAFKA_SECURITY_PROTOCOL."""
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "INVALID_PROTO")
    with pytest.raises(ValueError, match="KAFKA_SECURITY_PROTOCOL"):
        kafka_client_config()


def test_kafka_client_config_invalid_mechanism_raises(monkeypatch):
    """Invalid mechanism raises ValueError naming KAFKA_SASL_MECHANISM."""
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
    monkeypatch.setenv("KAFKA_USERNAME", "user")
    monkeypatch.setenv("KAFKA_PASSWORD", "pw")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "GSSAPI_KERBEROS")
    with pytest.raises(ValueError, match="KAFKA_SASL_MECHANISM"):
        kafka_client_config()


def test_producer_receives_helper_keys(monkeypatch):
    """confluent_kafka.Producer receives the keys returned by kafka_client_config."""
    expected_sasl = {
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "mock_user",
        "sasl.password": "mock_pass",
    }
    monkeypatch.setattr("twin.kafka_streamer.kafka_client_config", lambda: expected_sasl)

    captured_conf = {}

    def mock_producer_init(conf):
        captured_conf.update(conf)
        mock_prod = MagicMock()
        return mock_prod

    import confluent_kafka
    monkeypatch.setattr(confluent_kafka, "Producer", mock_producer_init)

    streamer = KafkaStreamer(bootstrap_servers="mock:9092", mock_mode=False)
    for k, v in expected_sasl.items():
        assert captured_conf.get(k) == v, f"Expected {k}={v} in Producer config, got {captured_conf.get(k)}"


@pytest.mark.asyncio
async def test_admin_client_receives_helper_keys(monkeypatch):
    """confluent_kafka.admin.AdminClient in wait_for_kafka_ready receives the helper's keys."""
    expected_sasl = {
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": "admin_user",
        "sasl.password": "admin_pass",
    }
    monkeypatch.setattr("twin.kafka_config.kafka_client_config", lambda: expected_sasl)

    captured_conf = {}

    class MockAdminClient:
        def __init__(self, conf):
            captured_conf.update(conf)

        def list_topics(self, timeout=2.0):
            mock_res = MagicMock()
            mock_res.brokers = {"broker-1": MagicMock()}
            return mock_res

    import confluent_kafka.admin
    monkeypatch.setattr(confluent_kafka.admin, "AdminClient", MockAdminClient)

    ready = await wait_for_kafka_ready(bootstrap_servers="mock:9092", max_retries=1, delay=0.01)
    assert ready is True
    for k, v in expected_sasl.items():
        assert captured_conf.get(k) == v, f"Expected {k}={v} in AdminClient config, got {captured_conf.get(k)}"


def test_consumer_receives_helper_keys(monkeypatch):
    """confluent_kafka.Consumer in KafkaSource receives the helper's keys."""
    expected_sasl = {
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "consumer_user",
        "sasl.password": "consumer_pass",
    }
    monkeypatch.setattr("mocks.kafka_config.kafka_client_config", lambda: expected_sasl)

    captured_conf = {}

    def mock_consumer_init(conf):
        captured_conf.update(conf)
        mock_c = MagicMock()
        return mock_c

    import confluent_kafka
    monkeypatch.setattr(confluent_kafka, "Consumer", mock_consumer_init)

    source = KafkaSource(bootstrap_servers="mock:9092", group_id="test-grp", topic="twin.sensors")
    assert source.consumer is not None
    for k, v in expected_sasl.items():
        assert captured_conf.get(k) == v, f"Expected {k}={v} in Consumer config, got {captured_conf.get(k)}"


def test_producer_acks_all_idempotence_default(monkeypatch):
    """Default acks is 'all' and enable.idempotence is True."""
    monkeypatch.delenv("KAFKA_ACKS", raising=False)
    conf = KafkaStreamer.build_producer_config(bootstrap_servers="mock:9092")
    assert conf["acks"] == "all"
    assert conf["enable.idempotence"] is True


def test_producer_acks_override_1(monkeypatch):
    """KAFKA_ACKS=1 disables idempotence."""
    monkeypatch.setenv("KAFKA_ACKS", "1")
    conf = KafkaStreamer.build_producer_config(bootstrap_servers="mock:9092")
    assert conf["acks"] in (1, "1")
    assert conf["enable.idempotence"] is False


def test_producer_acks_override_0(monkeypatch):
    """KAFKA_ACKS=0 disables idempotence."""
    monkeypatch.setenv("KAFKA_ACKS", "0")
    conf = KafkaStreamer.build_producer_config(bootstrap_servers="mock:9092")
    assert conf["acks"] in (0, "0")
    assert conf["enable.idempotence"] is False


def test_producer_acks_invalid_raises(monkeypatch):
    """Invalid KAFKA_ACKS raises ValueError naming KAFKA_ACKS and allowed values."""
    monkeypatch.setenv("KAFKA_ACKS", "invalid")
    with pytest.raises(ValueError, match="KAFKA_ACKS.*all, 1, 0"):
        KafkaStreamer.build_producer_config(bootstrap_servers="mock:9092")


def test_streamer_init_invalid_acks_raises(monkeypatch):
    """Streamer startup fails immediately when KAFKA_ACKS is invalid."""
    monkeypatch.setenv("KAFKA_ACKS", "2")
    with pytest.raises(ValueError, match="KAFKA_ACKS"):
        KafkaStreamer(bootstrap_servers="mock:9092", mock_mode=True)


def test_kafka_client_config_ssl_ca_location_valid(monkeypatch):
    """SASL_SSL with valid KAFKA_SSL_CA_LOCATION includes ssl.ca.location."""
    with tempfile.NamedTemporaryFile("w", delete=False) as f:
        f.write("-----BEGIN CERTIFICATE-----\nMOCK\n-----END CERTIFICATE-----\n")
        ca_file = f.name

    try:
        monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
        monkeypatch.setenv("KAFKA_USERNAME", "ssl_user")
        monkeypatch.setenv("KAFKA_PASSWORD", "ssl_secret")
        monkeypatch.setenv("KAFKA_SSL_CA_LOCATION", ca_file)

        conf = kafka_client_config()
        assert conf["security.protocol"] == "SASL_SSL"
        assert conf["ssl.ca.location"] == ca_file
    finally:
        if os.path.exists(ca_file):
            os.remove(ca_file)


def test_kafka_client_config_ssl_ca_location_missing_raises(monkeypatch):
    """Non-existent CA file specified in KAFKA_SSL_CA_LOCATION raises ValueError."""
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_USERNAME", "ssl_user")
    monkeypatch.setenv("KAFKA_PASSWORD", "ssl_secret")
    monkeypatch.setenv("KAFKA_SSL_CA_LOCATION", "/nonexistent/path/to/ca.pem")

    with pytest.raises(ValueError, match="KAFKA_SSL_CA_LOCATION"):
        kafka_client_config()


def test_kafka_client_config_plaintext_ignores_ca_location(monkeypatch):
    """PLAINTEXT protocol returns only security.protocol, ignoring KAFKA_SSL_CA_LOCATION."""
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    monkeypatch.setenv("KAFKA_SSL_CA_LOCATION", "/nonexistent/ignored/ca.pem")

    conf = kafka_client_config()
    assert conf == {"security.protocol": "PLAINTEXT"}


def test_mock_kafka_config_ssl_ca_location(monkeypatch):
    """twin/mocks/kafka_config handles KAFKA_SSL_CA_LOCATION identically."""
    import mocks.kafka_config as mock_kc

    with tempfile.NamedTemporaryFile("w", delete=False) as f:
        f.write("-----BEGIN CERTIFICATE-----\nMOCK\n-----END CERTIFICATE-----\n")
        ca_file = f.name

    try:
        monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
        monkeypatch.setenv("KAFKA_USERNAME", "mock_user")
        monkeypatch.setenv("KAFKA_PASSWORD", "mock_secret")
        monkeypatch.setenv("KAFKA_SSL_CA_LOCATION", ca_file)

        conf = mock_kc.kafka_client_config()
        assert conf["security.protocol"] == "SASL_SSL"
        assert conf["ssl.ca.location"] == ca_file

        monkeypatch.setenv("KAFKA_SSL_CA_LOCATION", "/nonexistent/mock_ca.pem")
        with pytest.raises(ValueError, match="KAFKA_SSL_CA_LOCATION"):
            mock_kc.kafka_client_config()
    finally:
        if os.path.exists(ca_file):
            os.remove(ca_file)


def test_broker_address_family_default(monkeypatch):
    """Default broker address family is 'any'."""
    monkeypatch.delenv("KAFKA_BROKER_ADDRESS_FAMILY", raising=False)
    assert get_broker_address_family() == "any"


def test_broker_address_family_valid(monkeypatch):
    """Explicit valid families 'v4', 'v6', and 'any' (case-insensitive)."""
    monkeypatch.setenv("KAFKA_BROKER_ADDRESS_FAMILY", "v4")
    assert get_broker_address_family() == "v4"

    monkeypatch.setenv("KAFKA_BROKER_ADDRESS_FAMILY", "V6")
    assert get_broker_address_family() == "v6"

    monkeypatch.setenv("KAFKA_BROKER_ADDRESS_FAMILY", "any")
    assert get_broker_address_family() == "any"


def test_broker_address_family_invalid_raises(monkeypatch):
    """Invalid family raises ValueError naming KAFKA_BROKER_ADDRESS_FAMILY."""
    monkeypatch.setenv("KAFKA_BROKER_ADDRESS_FAMILY", "ipv4")
    with pytest.raises(ValueError, match="KAFKA_BROKER_ADDRESS_FAMILY"):
        get_broker_address_family()


def test_producer_receives_broker_address_family(monkeypatch):
    """Producer configuration includes broker.address.family."""
    monkeypatch.setenv("KAFKA_BROKER_ADDRESS_FAMILY", "v4")
    conf = KafkaStreamer.build_producer_config(bootstrap_servers="mock:9092")
    assert conf["broker.address.family"] == "v4"


def test_streamer_init_invalid_broker_address_family_raises(monkeypatch):
    """KafkaStreamer raises ValueError on startup if KAFKA_BROKER_ADDRESS_FAMILY is invalid."""
    monkeypatch.setenv("KAFKA_BROKER_ADDRESS_FAMILY", "invalid_family")
    with pytest.raises(ValueError, match="KAFKA_BROKER_ADDRESS_FAMILY"):
        KafkaStreamer(bootstrap_servers="mock:9092", mock_mode=False)


def test_consumer_receives_broker_address_family(monkeypatch):
    """KafkaSource Consumer receives broker.address.family."""
    monkeypatch.setenv("KAFKA_BROKER_ADDRESS_FAMILY", "v6")

    captured_conf = {}

    def mock_consumer_init(conf):
        captured_conf.update(conf)
        return MagicMock()

    import confluent_kafka
    monkeypatch.setattr(confluent_kafka, "Consumer", mock_consumer_init)

    source = KafkaSource(bootstrap_servers="mock:9092", group_id="test-grp", topic="twin.sensors")
    assert captured_conf.get("broker.address.family") == "v6"


def test_mock_kafka_config_broker_address_family(monkeypatch):
    """twin/mocks/kafka_config handles KAFKA_BROKER_ADDRESS_FAMILY identically."""
    import mocks.kafka_config as mock_kc

    monkeypatch.setenv("KAFKA_BROKER_ADDRESS_FAMILY", "v4")
    assert mock_kc.get_broker_address_family() == "v4"

    monkeypatch.setenv("KAFKA_BROKER_ADDRESS_FAMILY", "invalid")
    with pytest.raises(ValueError, match="KAFKA_BROKER_ADDRESS_FAMILY"):
        mock_kc.get_broker_address_family()


def test_twin_src_never_touches_broker():
    """Verify twin/src contains zero broker administration or topic creation methods."""
    forbidden = ["create_topics", "NewTopic", "alter_configs", "create_partitions"]
    src_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
    violations = []
    for root, _, files in os.walk(src_dir):
        for f in files:
            if f.endswith(".py"):
                path = os.path.join(root, f)
                with open(path, "r", encoding="utf-8") as fh:
                    for line_num, line in enumerate(fh, 1):
                        for term in forbidden:
                            if term in line:
                                violations.append(f"{path}:{line_num}: {term} found in '{line.strip()}'")
    assert not violations, f"Forbidden broker manipulation detected:\n" + "\n".join(violations)

