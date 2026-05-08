"""Password hashing helpers (bcrypt, direct).

We use ``bcrypt`` directly rather than via ``passlib`` because passlib 1.7.4
is incompatible with bcrypt >= 4.1 (it can't read the version string and
its 72-byte truncation has been removed). Direct use is straightforward
and gives us explicit control over the truncation that bcrypt requires.
"""

from __future__ import annotations

import bcrypt

# bcrypt's hard input limit. Inputs are truncated to this length on both
# the hash and verify paths so the two never disagree.
_MAX_BYTES = 72
_DEFAULT_ROUNDS = 12


def _prepare(plain: str) -> bytes:
    return plain.encode("utf-8")[:_MAX_BYTES]


def hash_password(plain: str, *, rounds: int = _DEFAULT_ROUNDS) -> str:
    return bcrypt.hashpw(_prepare(plain), bcrypt.gensalt(rounds=rounds)).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(_prepare(plain), hashed.encode("utf-8"))
    except (ValueError, TypeError):
        return False


def needs_rehash(hashed: str, *, rounds: int = _DEFAULT_ROUNDS) -> bool:
    """True if the stored hash uses fewer rounds than ``rounds`` and should
    be regenerated on the next successful login. Bcrypt format is
    ``$2b$<rounds>$<salt><hash>``."""

    try:
        parts = hashed.split("$")
        return int(parts[2]) < rounds
    except (IndexError, ValueError):
        return True
