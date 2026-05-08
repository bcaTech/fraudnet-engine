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
