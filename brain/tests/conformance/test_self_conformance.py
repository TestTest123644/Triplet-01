"""The conformance kit, run against our own mocks and our own contracts (spec 10.4).

If the kit cannot pass against the mocks the Brain was built on, it is not a fair test of a
partner. The same checks run against the live Twin when `BRAIN_TWIN_MCP` is reachable.
"""

import json
from pathlib import Path

import pytest

from city_brain.edge.profiles import GuardianProfile, TwinProfile
from conformance.test_partner_conformance import (
    REQUIRED_TWIN_TOOLS,
    Finding,
    assert_conformant,
    check_guardian,
    check_twin,
    conformance_report,
)
from mocks.guardian.mock_guardian import MockGuardian, trust_all
from mocks.keys import derive_keypair
from mocks.twin import MockTwin
from mocks.twin.alt import ALT_PROFILE, AltTwin

BRAIN = Path(__file__).resolve().parents[2]
GUARDIAN_PROFILE = BRAIN / "profiles" / "guardian-triplet07.yaml"
PRIV, PUB = derive_keypair("conformance:guardian")


@pytest.fixture
def twin():
    return MockTwin()


@pytest.fixture
def guardian_profile():
    return GuardianProfile.load(GUARDIAN_PROFILE, public_key_b64=PUB)


# ----- the mocks pass their own kit -----


def test_the_mock_twin_is_conformant(twin):
    findings = check_twin(twin, twin.profile)
    assert_conformant(findings)
    assert len(findings) >= 5


def test_the_alternative_partner_is_conformant():
    alt = AltTwin(MockTwin())
    assert_conformant(check_twin(alt, alt.profile))


def test_the_mock_guardian_is_conformant(twin, guardian_profile):
    readings = twin.tick()
    assert_conformant(check_guardian(MockGuardian(PRIV, trust_all()), guardian_profile,
                                     readings, PUB))


def test_the_report_counts_what_passed(twin):
    report = conformance_report(check_twin(twin, twin.profile))
    assert "checks passed" in report and "FAIL" not in report


# ----- the kit notices a partner that does not conform -----


def test_an_empty_registry_fails(twin):
    class Empty(MockTwin):
        def list_devices(self):
            return []

    findings = check_twin(Empty(), twin.profile)
    assert any(not f.ok and "non-empty" in f.rule for f in findings)


def test_a_missing_key_fails(twin):
    class Keyless(MockTwin):
        def get_device_key(self, device_id):
            return None

    findings = check_twin(Keyless(), twin.profile)
    assert any(not f.ok and "public key" in f.rule for f in findings)


def test_an_unsigned_reading_fails(twin):
    class Unsigned(MockTwin):
        def tick(self):
            readings = super().tick()
            for reading in readings:
                reading["sig"] = "bm90YXNpZw=="
            return readings

    findings = check_twin(Unsigned(), twin.profile)
    assert any(not f.ok and "edge guard" in f.rule for f in findings)


def test_a_missing_tool_fails(twin):
    class Toolless(MockTwin):
        tool_names = ("list_devices",)

    findings = check_twin(Toolless(), twin.profile)
    failed = [f for f in findings if not f.ok and "tool" in f.rule]
    assert failed and "set_valve" in failed[0].detail


def test_a_silent_guardian_fails(twin, guardian_profile):
    from mocks.guardian.mock_guardian import silent

    findings = check_guardian(MockGuardian(PRIV, silent()), guardian_profile, twin.tick(), PUB)
    assert any(not f.ok and "verdict is issued" in f.rule for f in findings)


def test_an_unsigned_verdict_fails(twin, guardian_profile):
    from mocks.guardian.mock_guardian import unsigned

    findings = check_guardian(MockGuardian(PRIV, unsigned()), guardian_profile, twin.tick(), PUB)
    assert any(not f.ok for f in findings)


def test_a_verdict_signed_by_someone_else_fails(twin):
    other_priv, _ = derive_keypair("not:the:guardian")
    profile = GuardianProfile.load(GUARDIAN_PROFILE, public_key_b64=PUB)
    findings = check_guardian(MockGuardian(other_priv), profile, twin.tick(), PUB)
    assert any(not f.ok and "signed by the Guardian" in f.rule for f in findings)


def test_a_vocabulary_that_cannot_say_untrusted_fails(tmp_path, twin):
    data = GUARDIAN_PROFILE.read_text(encoding="utf-8").replace("  UNTRUSTED: UNTRUSTED\n", "")
    data = data.replace("  MALICIOUS: UNTRUSTED\n", "").replace("  COMPROMISED: UNTRUSTED\n", "")
    path = tmp_path / "guardian-partial.yaml"
    path.write_text(data, encoding="utf-8")
    profile = GuardianProfile.load(path, public_key_b64=PUB)
    findings = check_guardian(MockGuardian(PRIV), profile, twin.tick(), PUB)
    assert any(not f.ok and "vocabulary" in f.rule for f in findings)


def test_a_finding_reads_as_a_sentence():
    assert str(Finding("something", False, "because")) == "FAIL something: because"
    assert str(Finding("something", True)) == "PASS something"


# ----- the contracts match what the code serves -----


def test_the_openapi_contract_matches_the_live_api(tmp_path):
    from city_brain.contracts import openapi_document

    committed = json.loads((BRAIN / "contracts" / "openapi.json").read_text(encoding="utf-8"))
    assert committed == json.loads(openapi_document()), (
        "contracts/openapi.json is out of date; run `make contracts`")


def test_the_asyncapi_contract_names_every_channel():
    import yaml

    document = yaml.safe_load((BRAIN / "contracts" / "asyncapi.yaml").read_text(encoding="utf-8"))
    channels = set(document["channels"])
    assert channels == {"twin.sensors", "guardian.verdicts", "guardian.events", "brain.events"}


def test_the_interface_card_documents_the_configuration():
    text = (BRAIN / "INTERFACE.md").read_text(encoding="utf-8")
    for needed in (
        "twin.sensors", "guardian.verdicts", "guardian.events", "brain.events",
        "BRAIN__", "BRAIN_OPERATOR_TOKEN", "ed25519", "8200", "8110", "8120",
        "MCP_AUTH_TOKEN", "KAFKA_PASSWORD_FILE", "telemetry-ingest", "guardian.optional",
    ):
        assert needed in text, needed


def test_the_readme_shows_how_to_run_it():
    text = (BRAIN / "README.md").read_text(encoding="utf-8")
    for needed in ("make scenario", "make timeline", "docker compose", "standalone", "triplet"):
        assert needed in text, needed


# ----- against the live partners -----


@pytest.mark.twin_live
def test_the_live_twin_is_conformant():
    import asyncio

    from city_brain.adapters.mcp_twin import McpTwinClient
    from tests.partners import ENDPOINTS

    url = ENDPOINTS["twin_live"].value()

    async def probe():
        client = McpTwinClient(url, timeout_s=5.0)
        try:
            devices, keys, _ = await client.load_registry()
            return devices, keys
        finally:
            await client.close()

    devices, keys = asyncio.run(probe())
    profile = TwinProfile.load(BRAIN / "profiles" / "twin-triplet07.yaml")

    class LiveTwin:
        tool_names = REQUIRED_TWIN_TOOLS

        def list_devices(self):
            return devices

        def get_device_key(self, device_id):
            return keys.get(device_id)

        def tick(self):
            return []  # readings come off Kafka, not the tool surface

    assert_conformant(check_twin(LiveTwin(), profile, ticks=0))
