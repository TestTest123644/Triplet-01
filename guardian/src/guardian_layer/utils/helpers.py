from datetime import UTC, datetime
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa


def find_project_root(
    start: Path | None = None, markers: tuple[str, ...] = (".git", "pyproject.toml")
) -> Path | None:
    """The nearest folder, from ``start`` upwards, holding a marker; None if none does.

    ``start`` defaults to this file's folder, so the answer does not depend on the
    working directory or on how deep the caller sits in the package.
    """
    start = (start or Path(__file__).parent).resolve()
    for folder in (start, *start.parents):
        if any((folder / marker).exists() for marker in markers):
            return folder
    return None


def epoch_seconds(timestamp: datetime) -> float:
    """Seconds since the epoch; no offset means UTC (PreProcess and StaticCheck)."""
    if timestamp.tzinfo is None:  # else Python reads it as the machine's local time
        timestamp = timestamp.replace(tzinfo=UTC)
    return timestamp.timestamp()


# Load an RSA private key from a PEM file.
def load_private_key(path: str) -> rsa.RSAPrivateKey | None:

    with open(path, "rb") as file:
        private_key_pem = file.read()

    private_key = serialization.load_pem_private_key(private_key_pem, password=None)

    if not isinstance(private_key, rsa.RSAPrivateKey):
        return None

    return private_key


# Decrypt the encrypted data using RSA
def decrypt_data(data: bytes, private_key: rsa.RSAPrivateKey) -> bytes:

    return private_key.decrypt(
        data,
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None,
        ),
    )
