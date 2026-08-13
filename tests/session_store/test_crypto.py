import pytest

from keboola_mcp_server.session_store.crypto import (
    KEY_SIZE,
    DecryptionError,
    decrypt,
    encrypt,
    resolve_encryption_key,
)


def _key(byte: int = 1) -> bytes:
    return bytes([byte]) * KEY_SIZE


def test_round_trip() -> None:
    ciphertext = encrypt(b'kbc_at_secret', _key())
    assert decrypt(ciphertext, _key()) == b'kbc_at_secret'


def test_ciphertext_differs_each_call() -> None:
    # Random nonce per call -- same plaintext must not produce identical ciphertext.
    assert encrypt(b'same plaintext', _key()) != encrypt(b'same plaintext', _key())


def test_wrong_key_fails() -> None:
    ciphertext = encrypt(b'kbc_at_secret', _key(1))
    with pytest.raises(DecryptionError):
        decrypt(ciphertext, _key(2))


def test_tampered_ciphertext_fails() -> None:
    ciphertext = bytearray(encrypt(b'kbc_at_secret', _key()))
    ciphertext[-1] ^= 0xFF  # flip a bit in the GCM tag/ciphertext
    with pytest.raises(DecryptionError):
        decrypt(bytes(ciphertext), _key())


def test_wrong_key_version_fails() -> None:
    ciphertext = bytearray(encrypt(b'kbc_at_secret', _key()))
    ciphertext[0] = 99
    with pytest.raises(DecryptionError, match='key version'):
        decrypt(bytes(ciphertext), _key())


@pytest.mark.parametrize('key_len', [16, 31, 33])
def test_rejects_wrong_key_length(key_len: int) -> None:
    with pytest.raises(ValueError, match='32 bytes'):
        encrypt(b'x', bytes(key_len))


def test_resolve_encryption_key_decodes_base64() -> None:
    import base64

    raw = _key(7)
    assert resolve_encryption_key(base64.b64encode(raw).decode()) == raw


def test_resolve_encryption_key_falls_back_when_unset() -> None:
    key = resolve_encryption_key(None)
    assert len(key) == KEY_SIZE
    # Stable within the process (same fallback reused, not regenerated per call).
    assert resolve_encryption_key(None) == key


@pytest.mark.parametrize('bad_value', ['not-base64!!!', 'aGVsbG8='])  # valid base64, wrong length
def test_resolve_encryption_key_rejects_invalid_input(bad_value: str) -> None:
    with pytest.raises(ValueError, match='.+'):
        resolve_encryption_key(bad_value)
