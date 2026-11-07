from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, padding, rsa

from guardian_layer.utils.helpers import decrypt_data, load_private_key


def test_load_private_key(tmp_path) -> None:
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )

    pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    key_path = tmp_path / "private_key.pem"
    key_path.write_bytes(pem)

    loaded_key = load_private_key(str(key_path))

    assert isinstance(loaded_key, rsa.RSAPrivateKey)
    assert loaded_key.private_numbers() == private_key.private_numbers()


def test_decrypt_data() -> None:
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )

    public_key = private_key.public_key()

    plaintext = b"sensor telemetry"

    encrypted = public_key.encrypt(
        plaintext,
        padding.OAEP(
            mgf=padding.MGF1(
                algorithm=hashes.SHA256(),
            ),
            algorithm=hashes.SHA256(),
            label=None,
        ),
    )

    decrypted = decrypt_data(
        encrypted,
        private_key,
    )

    assert decrypted == plaintext


def test_load_private_key_rejects_non_rsa_key(tmp_path) -> None:
    private_key = ec.generate_private_key(
        ec.SECP256R1(),
    )

    pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    key_path = tmp_path / "ec_private_key.pem"
    key_path.write_bytes(pem)

    loaded_key = load_private_key(
        str(key_path),
    )

    assert loaded_key is None
