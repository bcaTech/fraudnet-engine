"""Audit logging middleware.

Every protected action — every non-GET request to ``/api/*`` or
``/auth/*`` — writes an :class:`db.models.AuditLog` row. The middleware
runs after the route handler so it can record the response status code
and the request duration.

The audit row captures:

- actor: user id + role pulled from ``request.state.principal`` (set by
  :func:`api.auth.rbac.current_user`); falls back to ``anon`` in dev.
- action: derived from the route path (e.g. ``alerts.acknowledge``).
- target kind + id: parsed from the path's last id-like segment.
- request metadata: method, path, status, request id, ip, user-agent,
  duration, anything in ``X-Audit-Extra`` header (JSON-encoded).

Failures inside the middleware itself are swallowed — we never want
audit logging to break a request. Failures land on the worker log so
they're visible to ops.
"""

from __future__ import annotations

import json
import re
import time
import uuid
from typing import Any

from sqlalchemy.exc import SQLAlchemyError
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from config.logging import get_logger
from core.security.encryption import encrypt
from db.models import AuditLog
from db.session import get_async_session

logger = get_logger(__name__)


# Methods that we treat as "protected actions" — anything that mutates
# state. GET / HEAD / OPTIONS are read-only and skipped.
_AUDITED_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})

# Path prefixes we audit. Anything outside these is skipped (e.g.
# health checks, websocket upgrades).
_AUDITED_PREFIXES = ("/api/", "/auth/")

# Fields we redact in the X-Audit-Extra header before persisting.
_REDACTED_FIELDS = frozenset({"password", "api_key", "secret", "token"})

# Field-name patterns whose values get encrypted-at-rest before being
# persisted to audit_logs.extra. Keys are matched case-insensitively
# against the leaf key of the JSON payload.
_ENCRYPTED_FIELDS = frozenset({"msisdn", "imei", "imsi", "phone", "phone_number"})

_UUID_LIKE = re.compile(r"^[a-z0-9-]{6,40}$", re.I)
_MSISDN_RE = re.compile(r"^\+?\d{7,15}$")
_IMEI_RE = re.compile(r"^\d{14,15}$")


def _action_for(path: str, method: str) -> str:
    """Derive an action label like ``alerts.acknowledge`` from the path.

    Strips known prefixes, drops id-like segments, and joins the rest
    with dots. Falls back to ``method:<path>`` if the heuristic gives
    nothing useful.
    """

    parts = [p for p in path.split("/") if p]
    if parts and parts[0] == "api":
        parts = parts[1:]
    keep = [p for p in parts if not _UUID_LIKE.match(p) or "-" not in p]
    if not keep:
        return f"{method.lower()}:{path}"
    return ".".join(keep)


def _target_from(path: str) -> tuple[str | None, str | None]:
    """Best-effort extraction of (kind, id) from the URL.

    For ``/api/alerts/alert-abc/acknowledge`` returns ``("alerts",
    "alert-abc")``. For ``/api/clusters/CLUSTER-0001/expand`` returns
    ``("clusters", "CLUSTER-0001")``. Returns ``(None, None)`` when no
    id-like segment is present.
    """

    parts = [p for p in path.split("/") if p]
    if parts and parts[0] == "api":
        parts = parts[1:]
    kind: str | None = None
    target: str | None = None
    for i, p in enumerate(parts):
        if i + 1 < len(parts) and _UUID_LIKE.match(parts[i + 1]) and "-" in parts[i + 1]:
            kind = p
            target = parts[i + 1]
    return kind, target


def _redact(payload: dict[str, Any]) -> dict[str, Any]:
    """Strip known-sensitive fields from a metadata dict."""

    if not isinstance(payload, dict):
        return {}
    return {k: ("***" if k.lower() in _REDACTED_FIELDS else v) for k, v in payload.items()}


def _encrypt_field(value: Any) -> Any:
    """Wrap ``value`` in Fernet encryption if it's a non-empty string;
    otherwise return as-is so JSON shape stays predictable."""

    if not isinstance(value, str) or not value:
        return value
    try:
        return {"_encrypted": True, "value": encrypt(value)}
    except Exception:  # noqa: BLE001 — never let encryption break audit
        return value


def _looks_like_pii(value: str) -> bool:
    return bool(_MSISDN_RE.match(value) or _IMEI_RE.match(value))


def _encrypt_pii(payload: Any) -> Any:
    """Walk a JSON-shaped payload and encrypt MSISDN / IMEI / IMSI
    fields. Triggers on either the key name (msisdn, imei, etc.) or a
    value that pattern-matches one of those formats."""

    if isinstance(payload, dict):
        return {
            k: _encrypt_field(v)
            if k.lower() in _ENCRYPTED_FIELDS
            else (_encrypt_field(v) if isinstance(v, str) and _looks_like_pii(v) else _encrypt_pii(v))
            for k, v in payload.items()
        }
    if isinstance(payload, list):
        return [_encrypt_pii(v) for v in payload]
    return payload


class AuditMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        path = request.url.path
        method = request.method.upper()
        if method not in _AUDITED_METHODS or not any(path.startswith(p) for p in _AUDITED_PREFIXES):
            return await call_next(request)

        # Per-request id helps correlate the audit row with logs.
        request_id = request.headers.get("X-Request-Id") or uuid.uuid4().hex[:16]
        request.state.request_id = request_id

        started = time.monotonic()
        response = await call_next(request)
        duration_ms = int((time.monotonic() - started) * 1000)

        try:
            await self._persist(request, response, duration_ms, request_id)
        except Exception as exc:  # noqa: BLE001 — audit must not break requests
            logger.warning("audit.persist.failed", error=str(exc))

        # Echo the request id so downstream tools can correlate.
        response.headers["X-Request-Id"] = request_id
        return response

    async def _persist(
        self,
        request: Request,
        response: Response,
        duration_ms: int,
        request_id: str,
    ) -> None:
        path = request.url.path
        method = request.method.upper()
        principal = getattr(request.state, "principal", None)
        actor_id = getattr(principal, "sub", None) if principal else None
        actor_role = getattr(principal, "role", None) if principal else None
        actor_kind = (
            "user" if (actor_id and actor_id != "anon") else ("system" if actor_id == "anon" else "anonymous")
        )
        kind, target = _target_from(path)
        # Encrypt the target id when it looks like raw PII (an MSISDN
        # / IMEI passed in the URL). Most calls use synthetic ids
        # (MOMO-..., AGT-...), so this is rarely triggered, but we
        # cover the case for endpoints like
        # ``/api/external/v1/flags/query?identifier=+233...`` should
        # they ever route through audit.
        if target and _looks_like_pii(target):
            wrapped = _encrypt_field(target)
            target = wrapped if isinstance(wrapped, str) else json.dumps(wrapped)

        extra_raw = request.headers.get("X-Audit-Extra")
        extra: dict[str, Any] | None = None
        if extra_raw:
            try:
                extra = _encrypt_pii(_redact(json.loads(extra_raw)))
            except (json.JSONDecodeError, TypeError):
                extra = {"_invalid_extra": extra_raw[:200]}

        client_ip = request.client.host if request.client else None
        user_agent = request.headers.get("user-agent", "")[:255]

        try:
            async with get_async_session() as db:
                db.add(
                    AuditLog(
                        id=f"audit-{uuid.uuid4().hex[:14]}",
                        actor_id=actor_id,
                        actor_role=actor_role,
                        actor_kind=actor_kind,
                        action=_action_for(path, method),
                        method=method,
                        path=path[:255],
                        status_code=response.status_code,
                        target_kind=kind,
                        target_id=target,
                        request_id=request_id,
                        ip_address=client_ip,
                        user_agent=user_agent,
                        duration_ms=duration_ms,
                        extra=extra,
                    )
                )
                await db.commit()
        except SQLAlchemyError as exc:
            # Most likely cause: audit_logs table doesn't exist yet
            # (first boot before create_all). Skip silently — the next
            # boot will catch up.
            logger.debug("audit.skip", error=str(exc))


__all__ = ["AuditMiddleware"]
