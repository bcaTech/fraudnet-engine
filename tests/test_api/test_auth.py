"""Auth tests: token mint/verify, password hashing, RBAC dependencies.

These tests are pure-unit — they don't spin up a FastAPI app or hit
Postgres. The integration-flavoured login round-trip is exercised in
the API integration suite (which runs against the live stack).
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from api.auth.jwt import AuthError, create_access_token, decode_token
from api.auth.passwords import hash_password, needs_rehash, verify_password
from api.auth.rbac import (
    ROLE_ADMIN,
    ROLE_ANALYST,
    ROLE_EXTERNAL_OPERATOR,
    ROLE_INVESTIGATOR,
    ROLE_LAW_ENFORCEMENT,
    ROLE_VIEWER,
    _has_role,
)

# ---------------------------------------------------------------------------
# Password hashing
# ---------------------------------------------------------------------------


def test_hash_and_verify_password_round_trip() -> None:
    hashed = hash_password("hunter2", rounds=4)  # low rounds for test speed
    assert verify_password("hunter2", hashed) is True
    assert verify_password("wrong-password", hashed) is False


def test_password_truncation_at_72_bytes() -> None:
    """bcrypt's 72-byte cap shouldn't desync hash vs verify."""

    long_pw = "a" * 200
    hashed = hash_password(long_pw, rounds=4)
    # Still verifies — both sides truncate consistently
    assert verify_password(long_pw, hashed) is True
    # Different password that still matches the first 72 bytes also verifies
    # (this is bcrypt's documented behaviour, not a bug)
    assert verify_password(long_pw + "ZZZ", hashed) is True


def test_needs_rehash_detects_old_rounds() -> None:
    weak = hash_password("x", rounds=4)
    assert needs_rehash(weak, rounds=12) is True
    assert needs_rehash(weak, rounds=4) is False


def test_needs_rehash_handles_garbage() -> None:
    assert needs_rehash("not-a-real-hash") is True


# ---------------------------------------------------------------------------
# JWT mint / verify
# ---------------------------------------------------------------------------


def test_create_and_decode_token_round_trip() -> None:
    token, exp = create_access_token(user_id="user-abc", username="ada", role="investigator")
    claims = decode_token(token)
    assert claims.sub == "user-abc"
    assert claims.username == "ada"
    assert claims.role == "investigator"
    assert claims.exp == int(exp.timestamp())


def test_decode_rejects_garbage() -> None:
    with pytest.raises(AuthError):
        decode_token("this.is.not.a.real.jwt")


def test_decode_rejects_expired_token() -> None:
    token, _ = create_access_token(
        user_id="u",
        username="x",
        role="viewer",
        expires_in=timedelta(seconds=-1),  # already expired
    )
    with pytest.raises(AuthError):
        decode_token(token)


def test_decode_rejects_token_missing_required_claims() -> None:
    """A token signed with our key but lacking 'role' should be rejected."""

    from jose import jwt

    from config.settings import get_settings

    s = get_settings()
    payload = {"sub": "u", "iat": 0, "exp": 9_999_999_999}  # no 'role'
    bad = jwt.encode(payload, s.jwt_secret.get_secret_value(), algorithm=s.jwt_algorithm)
    with pytest.raises(AuthError):
        decode_token(bad)


# ---------------------------------------------------------------------------
# RBAC hierarchy
# ---------------------------------------------------------------------------


def test_admin_satisfies_lower_internal_roles() -> None:
    for r in (ROLE_VIEWER, ROLE_ANALYST, ROLE_INVESTIGATOR):
        assert _has_role(ROLE_ADMIN, r) is True


def test_viewer_does_not_satisfy_investigator() -> None:
    assert _has_role(ROLE_VIEWER, ROLE_INVESTIGATOR) is False


def test_internal_role_does_not_satisfy_peer_role() -> None:
    """Admin shouldn't auto-grant law_enforcement permissions."""

    assert _has_role(ROLE_ADMIN, ROLE_LAW_ENFORCEMENT) is False
    assert _has_role(ROLE_ADMIN, ROLE_EXTERNAL_OPERATOR) is False


def test_peer_role_only_satisfies_itself_exactly() -> None:
    assert _has_role(ROLE_LAW_ENFORCEMENT, ROLE_LAW_ENFORCEMENT) is True
    assert _has_role(ROLE_LAW_ENFORCEMENT, ROLE_EXTERNAL_OPERATOR) is False
    assert _has_role(ROLE_LAW_ENFORCEMENT, ROLE_VIEWER) is False
