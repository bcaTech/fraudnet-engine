"""Role-based access control: roles, hierarchy, and FastAPI dependencies.

Internal roles are ranked: ``viewer < analyst < investigator <
senior_investigator < admin``. ``external_operator`` and ``law_enforcement``
are *peer* roles outside this hierarchy — they have explicit, narrow
permissions on the integration / case surfaces and should never be granted
internal RBAC by inheritance.

Use :func:`require_role` as a FastAPI dependency:

    @router.post("/freeze", dependencies=[Depends(require_role("investigator"))])
    async def freeze(...): ...

Or read the current principal directly via the :data:`CurrentUser`
type alias.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Annotated

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from config.settings import get_settings

from .jwt import AuthError, TokenClaims, decode_token

# ---------------------------------------------------------------------------
# Roles
# ---------------------------------------------------------------------------


ROLE_VIEWER = "viewer"
ROLE_ANALYST = "analyst"
ROLE_INVESTIGATOR = "investigator"
ROLE_SENIOR_INVESTIGATOR = "senior_investigator"
ROLE_ADMIN = "admin"
ROLE_EXTERNAL_OPERATOR = "external_operator"
ROLE_LAW_ENFORCEMENT = "law_enforcement"


# Internal hierarchy — each role implicitly grants every role ranked below it.
# Peer roles (external_operator, law_enforcement) are NOT in the hierarchy.
ROLE_RANK: dict[str, int] = {
    ROLE_VIEWER: 0,
    ROLE_ANALYST: 1,
    ROLE_INVESTIGATOR: 2,
    ROLE_SENIOR_INVESTIGATOR: 3,
    ROLE_ADMIN: 4,
}

ALL_ROLES: tuple[str, ...] = (
    ROLE_VIEWER,
    ROLE_ANALYST,
    ROLE_INVESTIGATOR,
    ROLE_SENIOR_INVESTIGATOR,
    ROLE_ADMIN,
    ROLE_EXTERNAL_OPERATOR,
    ROLE_LAW_ENFORCEMENT,
)


def _has_role(actual: str, required: str) -> bool:
    """True iff ``actual`` satisfies ``required``.

    Internal roles use the rank hierarchy (admin satisfies everything).
    Peer roles match exactly — an ``external_operator`` does NOT satisfy
    ``analyst`` and vice-versa.
    """

    if required not in ROLE_RANK:
        return actual == required  # peer role — exact match only
    if actual not in ROLE_RANK:
        return False  # peer role can't satisfy an internal role
    return ROLE_RANK[actual] >= ROLE_RANK[required]


# ---------------------------------------------------------------------------
# Principal extraction
# ---------------------------------------------------------------------------


_bearer = HTTPBearer(auto_error=False, scheme_name="Bearer")

ANON_PRINCIPAL = TokenClaims(
    sub="anon",
    username="anon",
    role=ROLE_ADMIN,  # in dev mode, anonymous == admin so all routes work
    tenant_id=None,
    iat=0,
    exp=0,
)


async def current_user(
    request: Request,
    creds: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)] = None,
) -> TokenClaims:
    """Resolve the current principal from the ``Authorization: Bearer ...``
    header. In dev mode (``AUTH_REQUIRED=false``) a missing token resolves
    to an anonymous admin so existing call-sites keep working."""

    settings = get_settings()
    if creds is None:
        if settings.auth_required:
            raise HTTPException(
                status.HTTP_401_UNAUTHORIZED,
                "authentication required",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return ANON_PRINCIPAL
    try:
        claims = decode_token(creds.credentials)
    except AuthError as exc:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, str(exc)) from exc
    request.state.principal = claims
    return claims


CurrentUser = Annotated[TokenClaims, Depends(current_user)]


# ---------------------------------------------------------------------------
# Role dependencies
# ---------------------------------------------------------------------------


def require_role(*allowed: str):
    """FastAPI dependency that 403s unless the principal satisfies *any*
    of ``allowed``. Internal-role allowed entries match by hierarchy; peer
    roles match exactly.

    Example:
        @router.post(..., dependencies=[Depends(require_role("investigator"))])
    """

    if not allowed:
        raise ValueError("require_role() requires at least one role argument")

    async def _checker(user: CurrentUser) -> TokenClaims:
        # Anonymous principal in dev passes through (admin-equivalent).
        if user.sub == "anon" and not get_settings().auth_required:
            return user
        for role in allowed:
            if _has_role(user.role, role):
                return user
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            f"role '{user.role}' not authorised; need one of {list(allowed)}",
        )

    return _checker


def require_any_role(allowed: Iterable[str]):
    """Like :func:`require_role` but takes an iterable. Sugar for the case
    where the caller is computing the allow-list."""

    return require_role(*allowed)
