"""Field-level encryption for at-rest secrets.

Wraps :class:`cryptography.fernet.Fernet` so call-sites can write
``encrypt("plain text")`` and ``decrypt(stored)`` without thinking
about key management. The key is sourced from
``settings.encryption_key`` and must be a Fernet-format URL-safe
base64-encoded 32-byte key.

Use this for:

- TOTP secrets in ``users.totp_secret``
- PII fields in ``audit_logs`` (MSISDN, IMEI) per CLAUDE.md §6.1

Production should swap the static settings key for a KMS-backed
implementation. The public API in this module is the seam we'll
preserve through that migration.
"""

from __future__ import annotations

from functools import lru_cache

from cryptography.fernet import Fernet, InvalidToken

from config.settings import get_settings


class EncryptionError(Exception):
    """Raised when a payload can't be encrypted/decrypted with the
    configured key. Wraps :class:`cryptography.fernet.InvalidToken`."""


@lru_cache(maxsize=1)
def _fernet() -> Fernet:
    settings = get_settings()
    raw = settings.encryption_key.get_secret_value()
    key_bytes = raw.encode("utf-8") if isinstance(raw, str) else raw
    return Fernet(key_bytes)


def encrypt(plain: str) -> str:
    """Encrypt ``plain`` and return a URL-safe ASCII string suitable for
    a ``String`` SQLAlchemy column."""

    if not isinstance(plain, str):
        raise EncryptionError("encrypt() expects a str")
    token = _fernet().encrypt(plain.encode("utf-8"))
    return token.decode("ascii")


def decrypt(stored: str) -> str:
    """Inverse of :func:`encrypt`."""

    if not isinstance(stored, str):
        raise EncryptionError("decrypt() expects a str")
    try:
        plain_bytes = _fernet().decrypt(stored.encode("ascii"))
    except InvalidToken as exc:
        raise EncryptionError("invalid token or wrong key") from exc
    return plain_bytes.decode("utf-8")


def reset_cache() -> None:
    """Drop the cached Fernet instance. Tests use this when toggling
    keys; production never needs it."""

    _fernet.cache_clear()
