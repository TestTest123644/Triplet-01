"""What the kit is pointed at. By default our own mocks, so it can be run as it stands.

A partner runs the kit against their own Twin and Guardian by overriding these fixtures in
their own conftest, or by pointing the environment at a live endpoint.
"""

from pathlib import Path

import pytest

from city_brain.edge.profiles import GuardianProfile, TwinProfile
from mocks.guardian.mock_guardian import MockGuardian, trust_all
from mocks.keys import derive_keypair
from mocks.twin import MockTwin

BRAIN = Path(__file__).resolve().parents[1]
GUARDIAN_PROFILE = BRAIN / "profiles" / "guardian-triplet07.yaml"
PRIVATE_KEY, PUBLIC_KEY = derive_keypair("conformance-kit:guardian")


@pytest.fixture
def conformance_twin() -> MockTwin:
    return MockTwin()


@pytest.fixture
def conformance_twin_profile(conformance_twin) -> TwinProfile:
    return conformance_twin.profile


@pytest.fixture
def conformance_guardian() -> MockGuardian:
    return MockGuardian(PRIVATE_KEY, policy=trust_all())


@pytest.fixture
def conformance_guardian_profile() -> GuardianProfile:
    return GuardianProfile.load(GUARDIAN_PROFILE, public_key_b64=PUBLIC_KEY)


@pytest.fixture
def conformance_guardian_key() -> str:
    return PUBLIC_KEY


@pytest.fixture
def conformance_readings(conformance_twin) -> list[dict]:
    return conformance_twin.tick()
