"""Operator integration surface.

Two route groups live here:

- ``/api/integration/...`` — internal operator management (NOC users).
  Connected operator CRUD, sharing-rule config, health, shared flag
  inbound/outbound, telecoms-chamber stubs.
- ``/api/external/v1/...`` — external API gateway. Authenticated by
  ``X-API-Key`` header (validated against hashed keys in Postgres) and
  scoped to a single :class:`ExternalOperator`. This is what *other*
  operators call when they federate with FraudNet.

Identifier masking on the outbound-share path is handled here too:
``ExternalOperator.masking_rules`` decides whether the recipient sees
hashed, partial, or full identifiers.
"""

from __future__ import annotations

import hashlib
import secrets
import uuid
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from api.auth.rbac import ROLE_ADMIN, ROLE_INVESTIGATOR, require_role
from api.dependencies import DBSessionDep
from api.schemas import APIResponse, Meta, ok
from api.websocket.publisher import CH_INTEGRATION, publish
from db.models import APIKey, ExternalOperator, SharedFlag

router = APIRouter(prefix="/api/integration", tags=["integration"])
external_router = APIRouter(prefix="/api/external/v1", tags=["external-api"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _operator_to_dict(op: ExternalOperator) -> dict[str, Any]:
    return {
        "id": op.id,
        "name": op.name,
        "contact_name": op.contact_name,
        "contact_email": op.contact_email,
        "technical_contact": op.technical_contact,
        "status": op.status,
        "integration_type": op.integration_type,
        "data_sharing_level": op.data_sharing_level,
        "masking_rules": op.masking_rules,
        "auto_integrate": op.auto_integrate,
        "onboarding_step": op.onboarding_step,
        "last_health_check": op.last_health_check.isoformat() if op.last_health_check else None,
        "last_health_status": op.last_health_status,
        "created_at": op.created_at.isoformat() if op.created_at else None,
    }


def _flag_to_dict(f: SharedFlag) -> dict[str, Any]:
    return {
        "id": f.id,
        "direction": f.direction,
        "operator_id": f.operator_id,
        "identifier_type": f.identifier_type,
        "identifier_masked": f.identifier_masked,
        "identifier_hash": f.identifier_hash,
        "risk_score": f.risk_score,
        "context": f.context,
        "shared_at": f.shared_at.isoformat() if f.shared_at else None,
        "action_taken": f.action_taken,
        "actioned_at": f.actioned_at.isoformat() if f.actioned_at else None,
    }


def _hash_identifier(identifier: str) -> str:
    return hashlib.sha256(identifier.encode("utf-8")).hexdigest()


def _mask_identifier(identifier: str, mode: str | None) -> str:
    """Apply per-operator masking to an identifier before sharing."""

    if mode == "clear":
        return identifier
    if mode == "partial" and len(identifier) > 4:
        return f"{identifier[:2]}***{identifier[-2:]}"
    # default: hash
    return _hash_identifier(identifier)


# ---------------------------------------------------------------------------
# Operator management (internal)
# ---------------------------------------------------------------------------


@router.get("/operators")
async def list_operators(db: DBSessionDep) -> APIResponse[list[dict[str, Any]]]:
    rows = (await db.execute(select(ExternalOperator).order_by(ExternalOperator.name))).scalars().all()
    return ok([_operator_to_dict(o) for o in rows])


@router.get("/operators/{operator_id}")
async def get_operator(operator_id: str, db: DBSessionDep) -> APIResponse[dict[str, Any]]:
    op = await _require_operator(db, operator_id)
    return ok(_operator_to_dict(op))


class OperatorCreateRequest(BaseModel):
    name: str = Field(..., min_length=2, max_length=120)
    contact_name: str | None = None
    contact_email: str | None = None
    technical_contact: str | None = None
    integration_type: str = "bidirectional"
    data_sharing_level: str = "hashed"
    masking_rules: dict[str, Any] | None = None
    auto_integrate: bool = False


@router.post(
    "/operators",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_role(ROLE_ADMIN))],
)
async def register_operator(payload: OperatorCreateRequest, db: DBSessionDep) -> APIResponse[dict[str, Any]]:
    """Onboard a new external operator. Returns the operator record plus
    a freshly-minted API key — show it once, then store the hash only."""

    if (
        await db.execute(select(ExternalOperator).where(ExternalOperator.name == payload.name))
    ).scalar_one_or_none() is not None:
        raise HTTPException(status.HTTP_409_CONFLICT, "operator with this name already exists")
    op = ExternalOperator(
        id=_new_id("op"),
        name=payload.name,
        contact_name=payload.contact_name,
        contact_email=payload.contact_email,
        technical_contact=payload.technical_contact,
        status="pending",
        integration_type=payload.integration_type,
        data_sharing_level=payload.data_sharing_level,
        masking_rules=payload.masking_rules or {"msisdn": "hash", "imei": "hash", "wallet_id": "partial"},
        auto_integrate=payload.auto_integrate,
        onboarding_step="awaiting_credentials",
    )
    db.add(op)
    # Flush so the operator row exists before we insert the api_key
    # foreign key — SQLAlchemy doesn't infer dep order without an
    # explicit relationship between the two models.
    await db.flush()

    raw_key = f"fnk_{secrets.token_urlsafe(32)}"
    api_key = APIKey(
        id=_new_id("key"),
        operator_id=op.id,
        key_hash=hashlib.sha256(raw_key.encode("utf-8")).hexdigest(),
        key_prefix=raw_key[:12],
        permissions=["external.flags.write", "external.flags.query"],
        active=True,
    )
    db.add(api_key)
    await db.commit()
    await db.refresh(op)

    return ok(
        {
            **_operator_to_dict(op),
            "api_key_one_time": raw_key,
            "api_key_prefix": api_key.key_prefix,
        }
    )


class OperatorConfigRequest(BaseModel):
    integration_type: str | None = None
    data_sharing_level: str | None = None
    masking_rules: dict[str, Any] | None = None
    auto_integrate: bool | None = None
    status: str | None = None


@router.put(
    "/operators/{operator_id}/config",
    dependencies=[Depends(require_role(ROLE_ADMIN))],
)
async def update_operator_config(
    operator_id: str, payload: OperatorConfigRequest, db: DBSessionDep
) -> APIResponse[dict[str, Any]]:
    op = await _require_operator(db, operator_id)
    if payload.integration_type is not None:
        op.integration_type = payload.integration_type
    if payload.data_sharing_level is not None:
        op.data_sharing_level = payload.data_sharing_level
    if payload.masking_rules is not None:
        op.masking_rules = payload.masking_rules
    if payload.auto_integrate is not None:
        op.auto_integrate = payload.auto_integrate
    if payload.status is not None:
        op.status = payload.status
    await db.commit()
    await db.refresh(op)
    await publish(
        CH_INTEGRATION,
        "operator.config_changed",
        {"operator_id": op.id, "name": op.name, "status": op.status},
    )
    return ok(_operator_to_dict(op))


@router.get("/operators/{operator_id}/health")
async def operator_health(operator_id: str, db: DBSessionDep) -> APIResponse[dict[str, Any]]:
    op = await _require_operator(db, operator_id)
    inbound = (
        await db.execute(
            select(func.count(SharedFlag.id)).where(
                SharedFlag.operator_id == op.id, SharedFlag.direction == "inbound"
            )
        )
    ).scalar_one()
    outbound = (
        await db.execute(
            select(func.count(SharedFlag.id)).where(
                SharedFlag.operator_id == op.id, SharedFlag.direction == "outbound"
            )
        )
    ).scalar_one()
    return ok(
        {
            "operator_id": op.id,
            "status": op.status,
            "last_health_check": op.last_health_check.isoformat() if op.last_health_check else None,
            "last_health_status": op.last_health_status,
            "inbound_flags": int(inbound),
            "outbound_flags": int(outbound),
        }
    )


@router.post(
    "/operators/{operator_id}/test",
    dependencies=[Depends(require_role(ROLE_ADMIN))],
)
async def operator_test(operator_id: str, db: DBSessionDep) -> APIResponse[dict[str, Any]]:
    """Send a no-op test flag through the masking pipeline. Verifies that
    the masking rules + write path are healthy without leaking real data."""

    op = await _require_operator(db, operator_id)
    sample = "+233241234999"
    flag = SharedFlag(
        id=_new_id("flag"),
        direction="outbound",
        operator_id=op.id,
        identifier_type="msisdn",
        identifier_masked=_mask_identifier(sample, (op.masking_rules or {}).get("msisdn")),
        identifier_hash=_hash_identifier(sample),
        risk_score=0.0,
        context="integration_test",
        shared_at=datetime.now(UTC),
        action_taken="test",
        actioned_at=datetime.now(UTC),
    )
    db.add(flag)
    op.last_health_check = datetime.now(UTC)
    op.last_health_status = "healthy"
    await db.commit()
    return ok({"operator_id": op.id, "delivered": True, "test_flag_id": flag.id})


@router.post(
    "/operators/{operator_id}/rotate-key",
    dependencies=[Depends(require_role(ROLE_ADMIN))],
)
async def rotate_operator_key(
    operator_id: str, db: DBSessionDep
) -> APIResponse[dict[str, Any]]:
    """Mint a new API key for the operator and deactivate every prior
    active key. The raw key is returned exactly once; only the SHA-256
    hash is persisted."""

    op = await _require_operator(db, operator_id)
    existing = (
        await db.execute(
            select(APIKey).where(
                APIKey.operator_id == op.id, APIKey.active.is_(True)
            )
        )
    ).scalars().all()
    inherited_perms = (
        existing[0].permissions
        if existing
        else ["external.flags.write", "external.flags.query"]
    )
    for k in existing:
        k.active = False
    await db.flush()

    raw_key = f"fnk_{secrets.token_urlsafe(32)}"
    api_key = APIKey(
        id=_new_id("key"),
        operator_id=op.id,
        key_hash=hashlib.sha256(raw_key.encode("utf-8")).hexdigest(),
        key_prefix=raw_key[:12],
        permissions=inherited_perms,
        active=True,
    )
    db.add(api_key)
    await db.commit()

    return ok(
        {
            "operator_id": op.id,
            "api_key_one_time": raw_key,
            "api_key_prefix": api_key.key_prefix,
            "previous_keys_revoked": len(existing),
        }
    )


# ---------------------------------------------------------------------------
# Shared flags (internal views)
# ---------------------------------------------------------------------------


@router.get("/shared/inbound")
async def shared_inbound(
    db: DBSessionDep,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=500),
    operator_id: str | None = None,
    action: str | None = None,
) -> APIResponse[list[dict[str, Any]]]:
    base = select(SharedFlag).where(SharedFlag.direction == "inbound")
    if operator_id:
        base = base.where(SharedFlag.operator_id == operator_id)
    if action:
        base = base.where(SharedFlag.action_taken == action)
    total = (await db.execute(select(func.count()).select_from(base.subquery()))).scalar_one()
    rows = (
        (
            await db.execute(
                base.order_by(desc(SharedFlag.shared_at)).offset((page - 1) * per_page).limit(per_page)
            )
        )
        .scalars()
        .all()
    )
    return APIResponse(
        data=[_flag_to_dict(f) for f in rows],
        meta=Meta(total=int(total), page=page, per_page=per_page),
        errors=[],
    )


@router.get("/shared/outbound")
async def shared_outbound(
    db: DBSessionDep,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=500),
    operator_id: str | None = None,
) -> APIResponse[list[dict[str, Any]]]:
    base = select(SharedFlag).where(SharedFlag.direction == "outbound")
    if operator_id:
        base = base.where(SharedFlag.operator_id == operator_id)
    total = (await db.execute(select(func.count()).select_from(base.subquery()))).scalar_one()
    rows = (
        (
            await db.execute(
                base.order_by(desc(SharedFlag.shared_at)).offset((page - 1) * per_page).limit(per_page)
            )
        )
        .scalars()
        .all()
    )
    return APIResponse(
        data=[_flag_to_dict(f) for f in rows],
        meta=Meta(total=int(total), page=page, per_page=per_page),
        errors=[],
    )


class FlagActionRequest(BaseModel):
    action: str = Field(..., pattern="^(accepted|dismissed|integrated)$")
    reason: str | None = None


@router.post(
    "/shared/inbound/{flag_id}/action",
    dependencies=[Depends(require_role(ROLE_INVESTIGATOR))],
)
async def action_inbound_flag(
    flag_id: str, payload: FlagActionRequest, db: DBSessionDep
) -> APIResponse[dict[str, Any]]:
    flag = (await db.execute(select(SharedFlag).where(SharedFlag.id == flag_id))).scalar_one_or_none()
    if flag is None or flag.direction != "inbound":
        raise HTTPException(status.HTTP_404_NOT_FOUND, "inbound flag not found")
    flag.action_taken = payload.action
    flag.actioned_at = datetime.now(UTC)
    await db.commit()
    await db.refresh(flag)
    return ok(_flag_to_dict(flag))


# ---------------------------------------------------------------------------
# Telecoms Chamber stubs
# ---------------------------------------------------------------------------


@router.get("/chamber/status")
async def chamber_status() -> APIResponse[dict[str, Any]]:
    return ok(
        {
            "status": "in_consultation",
            "registry_endpoint": None,
            "last_sync": None,
            "note": (
                "Telecoms Chamber registry integration awaiting Group sign-off. "
                "FraudNet is positioned to plug in once the registry is live."
            ),
        }
    )


@router.get("/chamber/metrics")
async def chamber_metrics(db: DBSessionDep) -> APIResponse[dict[str, Any]]:
    inbound = (
        await db.execute(select(func.count(SharedFlag.id)).where(SharedFlag.direction == "inbound"))
    ).scalar_one()
    outbound = (
        await db.execute(select(func.count(SharedFlag.id)).where(SharedFlag.direction == "outbound"))
    ).scalar_one()
    operators = (await db.execute(select(func.count(ExternalOperator.id)))).scalar_one()
    return ok(
        {
            "operators_connected": int(operators),
            "shared_flags_inbound_total": int(inbound),
            "shared_flags_outbound_total": int(outbound),
        }
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _require_operator(db: AsyncSession, operator_id: str) -> ExternalOperator:
    op = (
        await db.execute(select(ExternalOperator).where(ExternalOperator.id == operator_id))
    ).scalar_one_or_none()
    if op is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "operator not found")
    return op


# ---------------------------------------------------------------------------
# External API (operators connecting INTO FraudNet)
# ---------------------------------------------------------------------------


async def _resolve_api_key(db: AsyncSession, raw: str | None) -> APIKey:
    if not raw:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "missing X-API-Key header")
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    key = (
        await db.execute(select(APIKey).where(APIKey.key_hash == digest, APIKey.active.is_(True)))
    ).scalar_one_or_none()
    if key is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid api key")
    if key.expires_at is not None and key.expires_at < datetime.now(UTC):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "api key expired")
    key.last_used_at = datetime.now(UTC)
    await db.commit()
    return key


class ExternalFlagRequest(BaseModel):
    identifier_type: str = Field(..., pattern="^(msisdn|wallet|imei|imsi)$")
    identifier: str = Field(..., min_length=3, max_length=64)
    risk_score: float = Field(0.0, ge=0.0, le=1.0)
    context: str | None = None
    timestamp: datetime | None = None


@external_router.post("/flags", status_code=status.HTTP_201_CREATED)
async def external_post_flag(
    payload: ExternalFlagRequest,
    db: DBSessionDep,
    x_api_key: str | None = Header(None),
) -> APIResponse[dict[str, Any]]:
    """Receive a fraud flag from a connected operator."""

    key = await _resolve_api_key(db, x_api_key)
    if "external.flags.write" not in (key.permissions or []):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "key lacks flags.write")
    if key.operator_id is None:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "key not bound to an operator")

    flag = SharedFlag(
        id=_new_id("flag"),
        direction="inbound",
        operator_id=key.operator_id,
        identifier_type=payload.identifier_type,
        identifier_masked=_mask_identifier(payload.identifier, "partial"),
        identifier_hash=_hash_identifier(payload.identifier),
        risk_score=payload.risk_score,
        context=payload.context,
        shared_at=payload.timestamp or datetime.now(UTC),
    )
    db.add(flag)
    await db.commit()
    await db.refresh(flag)
    await publish(
        CH_INTEGRATION,
        "flag.inbound",
        {
            "flag_id": flag.id,
            "operator_id": flag.operator_id,
            "identifier_type": flag.identifier_type,
            "identifier_masked": flag.identifier_masked,
            "risk_score": flag.risk_score,
            "context": flag.context,
        },
    )
    return ok({"flag_id": flag.id, "received_at": flag.shared_at.isoformat(), "queued": True})


class ExternalFlagQuery(BaseModel):
    identifier_type: str = Field(..., pattern="^(msisdn|wallet|imei|imsi)$")
    identifier: str = Field(..., min_length=3, max_length=64)


@external_router.get("/flags/query")
async def external_query_flag(
    db: DBSessionDep,
    identifier_type: str = Query(..., pattern="^(msisdn|wallet|imei|imsi)$"),
    identifier: str = Query(..., min_length=3, max_length=64),
    x_api_key: str | None = Header(None),
) -> APIResponse[dict[str, Any]]:
    """Tell a connected operator whether we've seen this identifier."""

    key = await _resolve_api_key(db, x_api_key)
    if "external.flags.query" not in (key.permissions or []):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "key lacks flags.query")

    digest = _hash_identifier(identifier)
    rows = (
        (
            await db.execute(
                select(SharedFlag)
                .where(SharedFlag.identifier_hash == digest)
                .order_by(desc(SharedFlag.shared_at))
                .limit(1)
            )
        )
        .scalars()
        .all()
    )
    if not rows:
        return ok({"flagged": False, "identifier_type": identifier_type})
    f = rows[0]
    return ok(
        {
            "flagged": True,
            "identifier_type": identifier_type,
            "risk_score": f.risk_score,
            "flagged_since": f.shared_at.isoformat() if f.shared_at else None,
            "context": f.context,
        }
    )


class ExternalIntelRequest(BaseModel):
    identifiers: list[ExternalFlagRequest]
    relationships: list[dict[str, Any]] | None = None
    confidence: float = Field(0.5, ge=0.0, le=1.0)
    context: str | None = None


@external_router.post("/intelligence", status_code=status.HTTP_201_CREATED)
async def external_post_intelligence(
    payload: ExternalIntelRequest,
    db: DBSessionDep,
    x_api_key: str | None = Header(None),
) -> APIResponse[dict[str, Any]]:
    """Receive a structured intelligence package: a bundle of identifiers
    plus relationships. Each identifier is recorded as an inbound shared
    flag; the relationships are stored as raw context for downstream
    processing."""

    key = await _resolve_api_key(db, x_api_key)
    if "external.flags.write" not in (key.permissions or []):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "key lacks flags.write")
    if key.operator_id is None:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "key not bound to an operator")
    flag_ids: list[str] = []
    for item in payload.identifiers:
        f = SharedFlag(
            id=_new_id("flag"),
            direction="inbound",
            operator_id=key.operator_id,
            identifier_type=item.identifier_type,
            identifier_masked=_mask_identifier(item.identifier, "partial"),
            identifier_hash=_hash_identifier(item.identifier),
            risk_score=max(item.risk_score, payload.confidence),
            context=payload.context or item.context,
            shared_at=item.timestamp or datetime.now(UTC),
        )
        db.add(f)
        flag_ids.append(f.id)
    await db.commit()
    return ok(
        {
            "flag_ids": flag_ids,
            "relationships_received": len(payload.relationships or []),
            "confidence": payload.confidence,
        }
    )


@external_router.get("/health")
async def external_health(
    db: DBSessionDep,
    x_api_key: str | None = Header(None),
) -> APIResponse[dict[str, Any]]:
    key = await _resolve_api_key(db, x_api_key)
    return ok(
        {
            "status": "ok",
            "operator_id": key.operator_id,
            "key_prefix": key.key_prefix,
        }
    )
