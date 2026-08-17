"""AES-256-GCM encryption for session data at rest (oauth_session_persistence RFC).

GCM is authenticated encryption: tampering with the ciphertext (or decrypting with the wrong
key) raises ``InvalidTag`` rather than silently returning garbage plaintext.

Ciphertext layout: ``<key_version: 1 byte><nonce: 12 bytes><GCM ciphertext+tag>``. The
key-version prefix exists so a future key rotation can be introduced without a data migration
(RFC Open Question #2) -- v1 ships with exactly one supported version.
"""

import base64
import os
import secrets

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

KEY_SIZE = 32  # AES-256
_NONCE_SIZE = 12  # 96-bit GCM nonce, standard choice
_KEY_VERSION = 1

# Process-local fallback key for local dev/tests when KBC_SESSION_ENCRYPTION_KEY is unset.
# Mirrors mcp.py's _FALLBACK_SCOPE_SECRET: fine for a throwaway local Postgres, useless across a
# process restart -- a real deployment must set the env var.
_FALLBACK_KEY = secrets.token_bytes(KEY_SIZE)


class DecryptionError(Exception):
    """Raised when ciphertext fails authentication (wrong key, corruption, or tampering)."""


def encrypt(plaintext: bytes, key: bytes, aad: bytes | None = None) -> bytes:
    """``aad`` (additional authenticated data) is bound into the auth tag but never transmitted --
    the caller must supply the identical value again to `decrypt`. Use it to tie ciphertext to a
    caller identity so a token minted for one caller fails authentication if replayed by another,
    even with the right key (see `scope.py`'s `resolve_scope_binding_aad`)."""
    if len(key) != KEY_SIZE:
        raise ValueError(f'Encryption key must be {KEY_SIZE} bytes, got {len(key)}.')
    nonce = os.urandom(_NONCE_SIZE)
    ciphertext = AESGCM(key).encrypt(nonce, plaintext, aad)
    return bytes([_KEY_VERSION]) + nonce + ciphertext


def decrypt(blob: bytes, key: bytes, aad: bytes | None = None) -> bytes:
    if len(key) != KEY_SIZE:
        raise ValueError(f'Encryption key must be {KEY_SIZE} bytes, got {len(key)}.')
    if not blob or blob[0] != _KEY_VERSION:
        raise DecryptionError(f'Unsupported or missing key version in ciphertext: {blob[:1]!r}.')
    nonce, ciphertext = blob[1 : 1 + _NONCE_SIZE], blob[1 + _NONCE_SIZE :]
    try:
        return AESGCM(key).decrypt(nonce, ciphertext, aad)
    except InvalidTag as e:
        raise DecryptionError('Ciphertext failed authentication (wrong key/aad or tampered data).') from e


def resolve_encryption_key(session_encryption_key: str | None) -> bytes:
    """Decodes the base64-encoded ``KBC_SESSION_ENCRYPTION_KEY``, or falls back to a process-local
    key when unset (local dev/tests only -- see module docstring)."""
    if not session_encryption_key:
        return _FALLBACK_KEY
    try:
        key = base64.b64decode(session_encryption_key, validate=True)
    except Exception as e:
        raise ValueError('KBC_SESSION_ENCRYPTION_KEY is not valid base64.') from e
    if len(key) != KEY_SIZE:
        raise ValueError(f'KBC_SESSION_ENCRYPTION_KEY must decode to {KEY_SIZE} bytes, got {len(key)}.')
    return key
