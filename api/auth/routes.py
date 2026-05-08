"""Auth routes: login, current-user, user creation.

- ``POST /auth/login`` — exchange username + password for a bearer token.
- ``GET /auth/me`` — return the current principal.
- ``POST /auth/users`` — create a user (admin only).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import select

from api.dependencies import DBSessionDep
from api.schemas import APIResponse, ok
from db.models import User

from .jwt import create_access_token
from .passwords import hash_password, needs_rehash, verify_password
from .rbac import (
    ALL_ROLES,
    ROLE_ADMIN,
    CurrentUser,
    require_role,
)

router = APIRouter(prefix="/auth", tags=["auth"])


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------


class LoginRequest(BaseModel):
    username: str = Field(..., min_length=1, max_length=80)
    password: str = Field(..., min_length=1, max_length=200)
    # Optional six-digit TOTP code; required when the user has totp_enabled.
    totp_code: str | None = Field(None, pattern=r"^\d{6}$")


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_at: datetime
    user: dict[str, Any]


class UserCreateRequest(BaseModel):
    username: str = Field(..., min_length=3, max_length=80)
    email: str = Field(..., pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$", max_length=160)
    password: str = Field(..., min_length=8, max_length=200)
    role: str = Field(...)


def _user_to_dict(u: User) -> dict[str, Any]:
    return {
        "id": u.id,
        "username": u.username,
        "email": u.email,
        "role": u.role,
        "active": u.active,
        "totp_enabled": u.totp_enabled,
        "created_at": u.created_at.isoformat() if u.created_at else None,
        "last_login": u.last_login.isoformat() if u.last_login else None,
    }


# ---------------------------------------------------------------------------
# Login
# ---------------------------------------------------------------------------


@router.post("/login")
async def login(payload: LoginRequest, db: DBSessionDep) -> APIResponse[TokenResponse]:
    user = (await db.execute(select(User).where(User.username == payload.username))).scalar_one_or_none()
    if user is None or not user.active:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid credentials")
    if not verify_password(payload.password, user.password_hash):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid credentials")

    # Second factor: require TOTP if the user enabled it. The 401 body
    # carries totp_required=true so the frontend can pivot to its TOTP
    # prompt screen without re-entering the password.
    if user.totp_enabled:
        if not payload.totp_code:
            raise HTTPException(
                status.HTTP_401_UNAUTHORIZED,
                detail={
                    "error": "totp_required",
                    "totp_required": True,
                    "message": "TOTP code required for this user",
                },
            )
        from .totp import verify as totp_verify

        if not user.totp_secret or not totp_verify(user.totp_secret, payload.totp_code):
            raise HTTPException(
                status.HTTP_401_UNAUTHORIZED,
                detail={
                    "error": "totp_invalid",
                    "totp_required": True,
                    "message": "TOTP code is invalid",
                },
            )

    # Opportunistic rehash if scheme parameters changed.
    if needs_rehash(user.password_hash):
        user.password_hash = hash_password(payload.password)
    user.last_login = datetime.now(UTC)
    await db.commit()

    token, exp = create_access_token(
        user_id=user.id,
        username=user.username,
        role=user.role,
    )
    return ok(TokenResponse(access_token=token, expires_at=exp, user=_user_to_dict(user)))


# ---------------------------------------------------------------------------
# Current user
# ---------------------------------------------------------------------------


@router.get("/me")
async def me(user: CurrentUser, db: DBSessionDep) -> APIResponse[dict[str, Any]]:
    if user.sub == "anon":
        return ok(
            {
                "id": "anon",
                "username": "anon",
                "role": user.role,
                "anonymous": True,
            }
        )
    record = (await db.execute(select(User).where(User.id == user.sub))).scalar_one_or_none()
    if record is None:
        # Token valid but user record gone — return token claims directly.
        return ok(
            {
                "id": user.sub,
                "username": user.username,
                "role": user.role,
                "stale_record": True,
            }
        )
    return ok(_user_to_dict(record))


# ---------------------------------------------------------------------------
# User creation (admin only)
# ---------------------------------------------------------------------------


@router.post(
    "/users",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_role(ROLE_ADMIN))],
)
async def create_user(payload: UserCreateRequest, db: DBSessionDep) -> APIResponse[dict[str, Any]]:
    if payload.role not in ALL_ROLES:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            f"invalid role '{payload.role}'; must be one of {list(ALL_ROLES)}",
        )
    existing = (
        await db.execute(
            select(User).where((User.username == payload.username) | (User.email == payload.email))
        )
    ).scalar_one_or_none()
    if existing is not None:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            "username or email already exists",
        )
    user = User(
        id=f"user-{uuid.uuid4().hex[:12]}",
        username=payload.username,
        email=payload.email,
        password_hash=hash_password(payload.password),
        role=payload.role,
        active=True,
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)
    return ok(_user_to_dict(user))


@router.get("/users", dependencies=[Depends(require_role(ROLE_ADMIN))])
async def list_users(db: DBSessionDep) -> APIResponse[list[dict[str, Any]]]:
    rows = (await db.execute(select(User).order_by(User.created_at.desc()))).scalars().all()
    return ok([_user_to_dict(u) for u in rows])


class UserUpdateRequest(BaseModel):
    role: str | None = None
    active: bool | None = None
    email: str | None = Field(None, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$", max_length=160)
    password: str | None = Field(None, min_length=8, max_length=200)


@router.put(
    "/users/{user_id}",
    dependencies=[Depends(require_role(ROLE_ADMIN))],
)
async def update_user(
    user_id: str, payload: UserUpdateRequest, db: DBSessionDep
) -> APIResponse[dict[str, Any]]:
    """Admin-only: update a user's role / email / password / active flag."""

    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if user is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "user not found")

    if payload.role is not None:
        if payload.role not in ALL_ROLES:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, f"invalid role '{payload.role}'")
        user.role = payload.role
    if payload.active is not None:
        user.active = payload.active
    if payload.email is not None:
        user.email = payload.email
    if payload.password is not None:
        user.password_hash = hash_password(payload.password)

    await db.commit()
    await db.refresh(user)
    return ok(_user_to_dict(user))


@router.delete(
    "/users/{user_id}",
    dependencies=[Depends(require_role(ROLE_ADMIN))],
)
async def disable_user(user_id: str, db: DBSessionDep) -> APIResponse[dict[str, Any]]:
    """Soft-delete: flip the active flag rather than deleting the row.

    Hard delete would orphan FK references on alerts / audit logs;
    deactivating preserves history while the user can no longer log
    in.
    """

    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if user is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "user not found")
    user.active = False
    await db.commit()
    await db.refresh(user)
    return ok(_user_to_dict(user))


# ---------------------------------------------------------------------------
# Step-up auth scaffold
# ---------------------------------------------------------------------------


@router.post(
    "/step-up",
    dependencies=[Depends(require_role(ROLE_ADMIN))],
)
async def step_up(user: CurrentUser) -> APIResponse[dict[str, Any]]:
    """Mint a short-lived elevated token for high-risk operations.

    Real implementation: require a fresh WebAuthn / TOTP factor before
    issuing the elevated claim. For now we issue a 5-minute token with
    a ``step_up=true`` claim that the high-risk routes can check; the
    factor verification is left for the production rollout. Calling
    this with an admin role is the minimum guard.
    """

    from datetime import timedelta

    token, exp = create_access_token(
        user_id=user.sub,
        username=user.username,
        role=user.role,
        expires_in=timedelta(minutes=5),
        extra={"step_up": True},
    )
    return ok(
        {
            "access_token": token,
            "token_type": "bearer",
            "expires_at": exp.isoformat(),
            "step_up": True,
            "note": (
                "Production must require a second factor here. The current "
                "implementation only enforces an admin role on the request."
            ),
        }
    )


# ---------------------------------------------------------------------------
# TOTP — two-factor authentication
# ---------------------------------------------------------------------------


class TOTPSetupResponse(BaseModel):
    secret: str  # base32, shown once so the user can paste into a backup
    provisioning_uri: str
    qr_png_base64: str
    issuer: str = "FraudNet"


class TOTPCodeRequest(BaseModel):
    code: str = Field(..., pattern=r"^\d{6}$")


@router.post("/totp/setup")
async def totp_setup(user: CurrentUser, db: DBSessionDep) -> APIResponse[TOTPSetupResponse]:
    """Mint a fresh TOTP secret for the calling user.

    The secret is stored encrypted under ``users.totp_secret`` but the
    *plaintext* is returned exactly once in the response so the user
    can paste it into their authenticator app or save it as a backup.
    ``totp_enabled`` does NOT flip until :func:`totp_verify` succeeds —
    setup alone shouldn't lock anyone out of login.
    """

    from .totp import generate_secret, provisioning_uri, qr_png_base64, store_secret

    if user.sub == "anon":
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "TOTP setup requires authentication")

    record = (await db.execute(select(User).where(User.id == user.sub))).scalar_one_or_none()
    if record is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "user not found")

    secret = generate_secret()
    record.totp_secret = store_secret(secret)
    record.totp_enabled = False  # require verify before activating
    await db.commit()

    uri = provisioning_uri(secret=secret, account=record.email or record.username)
    return ok(
        TOTPSetupResponse(
            secret=secret,
            provisioning_uri=uri,
            qr_png_base64=qr_png_base64(uri),
        )
    )


@router.post("/totp/verify")
async def totp_verify_endpoint(
    payload: TOTPCodeRequest, user: CurrentUser, db: DBSessionDep
) -> APIResponse[dict[str, Any]]:
    """Verify a code against the stored secret. On success, flips
    ``totp_enabled=True`` so the next login requires the second
    factor."""

    from .totp import verify as totp_verify

    if user.sub == "anon":
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "TOTP verify requires authentication")

    record = (await db.execute(select(User).where(User.id == user.sub))).scalar_one_or_none()
    if record is None or not record.totp_secret:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            "TOTP not initialised — call /auth/totp/setup first",
        )
    if not totp_verify(record.totp_secret, payload.code):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid TOTP code")

    record.totp_enabled = True
    await db.commit()
    await db.refresh(record)
    return ok(_user_to_dict(record))


@router.post("/totp/validate")
async def totp_validate(
    payload: TOTPCodeRequest, user: CurrentUser, db: DBSessionDep
) -> APIResponse[dict[str, Any]]:
    """Validate a code without changing state. Useful for the frontend
    to challenge an existing session before a high-risk action.
    Returns ``{valid: true|false}`` rather than 4xx so the UI can
    re-prompt without hitting the auth-error path."""

    from .totp import verify as totp_verify

    if user.sub == "anon":
        return ok({"valid": False, "reason": "anonymous"})

    record = (await db.execute(select(User).where(User.id == user.sub))).scalar_one_or_none()
    if record is None or not record.totp_enabled or not record.totp_secret:
        return ok({"valid": False, "reason": "totp_not_enabled"})
    return ok({"valid": totp_verify(record.totp_secret, payload.code)})


@router.post("/totp/disable")
async def totp_disable(
    payload: TOTPCodeRequest, user: CurrentUser, db: DBSessionDep
) -> APIResponse[dict[str, Any]]:
    """Disable TOTP. Requires a current valid code so an attacker with
    a session token can't drop the second factor without the device."""

    from .totp import verify as totp_verify

    if user.sub == "anon":
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "TOTP disable requires authentication")

    record = (await db.execute(select(User).where(User.id == user.sub))).scalar_one_or_none()
    if record is None or not record.totp_secret or not record.totp_enabled:
        raise HTTPException(status.HTTP_409_CONFLICT, "TOTP not enabled")
    if not totp_verify(record.totp_secret, payload.code):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid TOTP code")

    record.totp_enabled = False
    record.totp_secret = None
    await db.commit()
    await db.refresh(record)
    return ok(_user_to_dict(record))


# ---------------------------------------------------------------------------
# Frontend-compat: Supabase-shaped session + token refresh
# ---------------------------------------------------------------------------


def _supabase_session(
    *,
    user_payload: dict[str, Any],
    access_token: str,
    expires_at: datetime,
) -> dict[str, Any]:
    """Re-shape our session into the envelope Supabase clients expect.

    Lets the frontend swap from Supabase Auth to our backend without
    refactoring the bits that read ``session.user.id`` / ``access_token`` /
    ``expires_at``.
    """

    expires_in = max(0, int((expires_at - datetime.now(UTC)).total_seconds()))
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "expires_in": expires_in,
        "expires_at": int(expires_at.timestamp()),
        # We don't issue refresh tokens (yet); /auth/refresh works off
        # the current bearer. Surfacing it as the same value keeps the
        # Supabase client happy.
        "refresh_token": access_token,
        "user": {
            "id": user_payload["id"],
            "email": user_payload.get("email"),
            "role": user_payload.get("role"),
            "user_metadata": {
                "username": user_payload.get("username"),
                "totp_enabled": user_payload.get("totp_enabled", False),
            },
            "app_metadata": {
                "role": user_payload.get("role"),
                "active": user_payload.get("active", True),
            },
            "created_at": user_payload.get("created_at"),
            "last_sign_in_at": user_payload.get("last_login"),
        },
    }


@router.get("/session")
async def session(user: CurrentUser, db: DBSessionDep) -> APIResponse[dict[str, Any]]:
    """Return the current session in a Supabase-compatible shape so the
    frontend can use ``data.session`` interchangeably.

    Anonymous callers (``AUTH_REQUIRED=false`` dev mode) receive
    ``{session: null}`` — matches Supabase's "signed-out" response so
    UI code can fall through to login without special-casing dev.
    """

    if user.sub == "anon":
        return ok({"session": None})

    record = (await db.execute(select(User).where(User.id == user.sub))).scalar_one_or_none()
    if record is None:
        return ok({"session": None})

    # Mint a fresh token whose expiry matches what /auth/refresh would
    # produce — clients calling /session typically hold a token but
    # want to know how long it's valid for.
    token, exp = create_access_token(user_id=record.id, username=record.username, role=record.role)
    return ok(
        {
            "session": _supabase_session(
                user_payload=_user_to_dict(record),
                access_token=token,
                expires_at=exp,
            )
        }
    )


@router.post("/refresh")
async def refresh(user: CurrentUser, db: DBSessionDep) -> APIResponse[dict[str, Any]]:
    """Issue a new access token to a caller holding a valid one.

    Sliding-window refresh: the new token's expiry is full-length from
    *now*, regardless of how much time was left on the old one. Because
    we don't track refresh tokens separately, a stolen access token
    can be silently extended — that's an accepted v1 trade-off; rotate
    JWT_SECRET to invalidate every outstanding token if needed.
    """

    if user.sub == "anon":
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "refresh requires an authenticated session")
    record = (await db.execute(select(User).where(User.id == user.sub))).scalar_one_or_none()
    if record is None or not record.active:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "user no longer active")

    token, exp = create_access_token(user_id=record.id, username=record.username, role=record.role)
    return ok(
        _supabase_session(
            user_payload=_user_to_dict(record),
            access_token=token,
            expires_at=exp,
        )
    )
