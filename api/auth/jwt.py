"""JWT minting and verification.

HS256 by default; algorithm + secret + expiry are settings-driven.
Tokens carry ``sub`` (user id), ``username``, ``role``, plus standard
``iat`` / ``exp`` claims. Tenant scoping (``tenant_id``) is reserved for
Phase 4 multi-tenancy and is plumbed through but unused for now.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from jose import JWTError, jwt
from pydantic import BaseModel, Field

from config.settings import get_settings


class TokenClaims(BaseModel):
    """Decoded JWT claims for an authenticated user."""

    sub: str
    username: str
    role: str
    tenant_id: str | None = None
    iat: int
    exp: int
    extra: dict[str, Any] = Field(default_factory=dict)


def create_access_token(
    *,
    user_id: str,
    username: str,
    role: str,
    tenant_id: str | None = None,
    expires_in: timedelta | None = None,
    extra: dict[str, Any] | None = None,
) -> tuple[str, datetime]:
    """Mint a signed access token. Returns ``(token, expires_at)``."""

    settings = get_settings()
    now = datetime.now(UTC)
    expires_at = now + (expires_in or timedelta(minutes=settings.jwt_expire_minutes))
    payload: dict[str, Any] = {
        "sub": user_id,
        "username": username,
        "role": role,
        "tenant_id": tenant_id,
        "iat": int(now.timestamp()),
        "exp": int(expires_at.timestamp()),
    }
    if extra:
        payload.update(extra)
    token = jwt.encode(
        payload,
        settings.jwt_secret.get_secret_value(),
        algorithm=settings.jwt_algorithm,
    )
    return token, expires_at


def decode_token(token: str) -> TokenClaims:
    """Decode and validate a JWT. Raises :class:`AuthError` on failure."""

    settings = get_settings()
    try:
        payload = jwt.decode(
            token,
            settings.jwt_secret.get_secret_value(),
            algorithms=[settings.jwt_algorithm],
        )
    except JWTError as exc:
        raise AuthError(f"invalid token: {exc}") from exc
    if "sub" not in payload or "role" not in payload:
        raise AuthError("token missing required claims")
    extra = {
        k: v for k, v in payload.items() if k not in {"sub", "username", "role", "tenant_id", "iat", "exp"}
    }
    return TokenClaims(
        sub=str(payload["sub"]),
        username=str(payload.get("username", "")),
        role=str(payload["role"]),
        tenant_id=payload.get("tenant_id"),
        iat=int(payload.get("iat", 0)),
        exp=int(payload.get("exp", 0)),
        extra=extra,
    )


class AuthError(Exception):
    """Raised when a token is missing, malformed, expired, or untrusted."""
