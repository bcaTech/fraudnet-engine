"""Law-enforcement collaboration surface.

Routes the NOC + LE portal share: case lifecycle, secure message threads,
evidence package generation + download, outcome tracking (arrests,
prosecutions, convictions, recovered funds), agency registry, and an
inbound-intelligence channel for receiving fraud signals from law
enforcement.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from api.auth.rbac import (
    ROLE_ADMIN,
    ROLE_INVESTIGATOR,
    ROLE_LAW_ENFORCEMENT,
    require_role,
)
from api.dependencies import DBSessionDep, Neo4jDep
from api.schemas import APIResponse, Meta, ok
from core.evidence.builder import build_for_cluster
from db.models import (
    EvidencePackage,
    LEAgency,
    LECase,
    LECaseMessage,
    LEOutcome,
)

router = APIRouter(prefix="/api/law-enforcement", tags=["law-enforcement"])


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


# ---------------------------------------------------------------------------
# Agencies
# ---------------------------------------------------------------------------


def _agency_to_dict(a: LEAgency) -> dict[str, Any]:
    return {
        "id": a.id,
        "name": a.name,
        "type": a.type,
        "contact_name": a.contact_name,
        "contact_email": a.contact_email,
        "contact_phone": a.contact_phone,
        "active": a.active,
        "created_at": a.created_at.isoformat() if a.created_at else None,
    }


@router.get("/agencies")
async def list_agencies(db: DBSessionDep) -> APIResponse[list[dict[str, Any]]]:
    rows = (await db.execute(select(LEAgency).order_by(LEAgency.name))).scalars().all()
    return ok([_agency_to_dict(a) for a in rows])


class AgencyCreateRequest(BaseModel):
    name: str = Field(..., min_length=2, max_length=160)
    type: str = Field(..., min_length=2, max_length=40)
    contact_name: str | None = Field(None, max_length=120)
    contact_email: str | None = Field(None, max_length=160)
    contact_phone: str | None = Field(None, max_length=32)


@router.post(
    "/agencies",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_role(ROLE_ADMIN))],
)
async def create_agency(payload: AgencyCreateRequest, db: DBSessionDep) -> APIResponse[dict[str, Any]]:
    if (
        await db.execute(select(LEAgency).where(LEAgency.name == payload.name))
    ).scalar_one_or_none() is not None:
        raise HTTPException(status.HTTP_409_CONFLICT, "agency with this name already exists")
    a = LEAgency(
        id=_new_id("agency"),
        name=payload.name,
        type=payload.type,
        contact_name=payload.contact_name,
        contact_email=payload.contact_email,
        contact_phone=payload.contact_phone,
        active=True,
    )
    db.add(a)
    await db.commit()
    await db.refresh(a)
    return ok(_agency_to_dict(a))


# ---------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------


def _case_to_dict(
    c: LECase, *, include_messages: bool = False, agency: LEAgency | None = None
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": c.id,
        "agency_id": c.agency_id,
        "agency": _agency_to_dict(agency) if agency is not None else None,
        "status": c.status,
        "cluster_ids": list(c.cluster_ids or []),
        "created_by": c.created_by,
        "created_at": c.created_at.isoformat() if c.created_at else None,
        "assigned_officer": c.assigned_officer,
        "officer_contact": c.officer_contact,
        "notes": c.notes,
    }
    if include_messages:
        payload["messages"] = [
            {
                "id": m.id,
                "sender_id": m.sender_id,
                "sender_role": m.sender_role,
                "content": m.content,
                "timestamp": m.timestamp.isoformat() if m.timestamp else None,
                "attachments": m.attachments,
            }
            for m in sorted(c.messages, key=lambda x: x.timestamp or datetime.min)
        ]
    return payload


class CaseCreateRequest(BaseModel):
    agency_id: str
    cluster_ids: list[str] = Field(..., min_length=1)
    assigned_officer: str | None = None
    officer_contact: str | None = None
    notes: str | None = None
    status: str = "under_review"


@router.get("/cases")
async def list_cases(
    db: DBSessionDep,
    page: int = Query(1, ge=1),
    per_page: int = Query(25, ge=1, le=200),
    status_filter: str | None = Query(None, alias="status"),
    agency_id: str | None = None,
) -> APIResponse[list[dict[str, Any]]]:
    base = select(LECase)
    if status_filter:
        base = base.where(LECase.status == status_filter)
    if agency_id:
        base = base.where(LECase.agency_id == agency_id)
    total = (await db.execute(select(func.count()).select_from(base.subquery()))).scalar_one()
    rows = (
        (
            await db.execute(
                base.order_by(LECase.created_at.desc()).offset((page - 1) * per_page).limit(per_page)
            )
        )
        .scalars()
        .all()
    )
    return APIResponse(
        data=[_case_to_dict(c) for c in rows],
        meta=Meta(total=int(total), page=page, per_page=per_page),
        errors=[],
    )


@router.get("/cases/{case_id}")
async def get_case(case_id: str, db: DBSessionDep) -> APIResponse[dict[str, Any]]:
    stmt = (
        select(LECase)
        .where(LECase.id == case_id)
        .options(selectinload(LECase.messages), selectinload(LECase.agency))
    )
    c = (await db.execute(stmt)).scalar_one_or_none()
    if c is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "case not found")
    return ok(_case_to_dict(c, include_messages=True, agency=c.agency))


@router.post(
    "/cases",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_role(ROLE_INVESTIGATOR))],
)
async def create_case(
    payload: CaseCreateRequest, db: DBSessionDep, neo4j: Neo4jDep
) -> APIResponse[dict[str, Any]]:
    agency = (await db.execute(select(LEAgency).where(LEAgency.id == payload.agency_id))).scalar_one_or_none()
    if agency is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "unknown agency_id")

    # Validate cluster_ids against Neo4j so we don't create cases for
    # phantoms.
    rows = await neo4j.execute_read(
        """
        MATCH (c:Cluster) WHERE c.cluster_id IN $ids
        RETURN c.cluster_id AS cluster_id
        """,
        {"ids": payload.cluster_ids},
    )
    found = {r["cluster_id"] for r in rows}
    missing = [cid for cid in payload.cluster_ids if cid not in found]
    if missing:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"unknown cluster_ids: {missing}")

    c = LECase(
        id=_new_id("case"),
        agency_id=payload.agency_id,
        status=payload.status,
        cluster_ids=payload.cluster_ids,
        created_by="system",  # TODO: set from authenticated user
        created_at=datetime.now(UTC),
        assigned_officer=payload.assigned_officer or agency.contact_name,
        officer_contact=payload.officer_contact or agency.contact_email,
        notes=payload.notes,
    )
    db.add(c)
    await db.commit()
    await db.refresh(c)
    return ok(_case_to_dict(c, agency=agency))


class CaseUpdateRequest(BaseModel):
    status: str | None = None
    assigned_officer: str | None = None
    officer_contact: str | None = None
    notes: str | None = None


@router.put(
    "/cases/{case_id}",
    dependencies=[Depends(require_role(ROLE_INVESTIGATOR))],
)
async def update_case(
    case_id: str, payload: CaseUpdateRequest, db: DBSessionDep
) -> APIResponse[dict[str, Any]]:
    c = (await db.execute(select(LECase).where(LECase.id == case_id))).scalar_one_or_none()
    if c is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "case not found")
    if payload.status is not None:
        c.status = payload.status
    if payload.assigned_officer is not None:
        c.assigned_officer = payload.assigned_officer
    if payload.officer_contact is not None:
        c.officer_contact = payload.officer_contact
    if payload.notes is not None:
        c.notes = payload.notes
    await db.commit()
    await db.refresh(c)
    return ok(_case_to_dict(c))


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------


class MessageCreateRequest(BaseModel):
    content: str = Field(..., min_length=1, max_length=5000)
    sender_role: str = Field(..., max_length=40)
    attachments: list[dict[str, Any]] | None = None


@router.get("/cases/{case_id}/messages")
async def list_messages(case_id: str, db: DBSessionDep) -> APIResponse[list[dict[str, Any]]]:
    rows = (
        (
            await db.execute(
                select(LECaseMessage)
                .where(LECaseMessage.case_id == case_id)
                .order_by(LECaseMessage.timestamp.asc())
            )
        )
        .scalars()
        .all()
    )
    return ok(
        [
            {
                "id": m.id,
                "sender_id": m.sender_id,
                "sender_role": m.sender_role,
                "content": m.content,
                "timestamp": m.timestamp.isoformat() if m.timestamp else None,
                "attachments": m.attachments,
            }
            for m in rows
        ]
    )


@router.post(
    "/cases/{case_id}/messages",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_role(ROLE_INVESTIGATOR, ROLE_LAW_ENFORCEMENT))],
)
async def post_message(
    case_id: str, payload: MessageCreateRequest, db: DBSessionDep
) -> APIResponse[dict[str, Any]]:
    if (await db.execute(select(LECase).where(LECase.id == case_id))).scalar_one_or_none() is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "case not found")
    m = LECaseMessage(
        id=_new_id("msg"),
        case_id=case_id,
        sender_id="system",  # TODO: from auth
        sender_role=payload.sender_role,
        content=payload.content,
        timestamp=datetime.now(UTC),
        attachments=payload.attachments,
    )
    db.add(m)
    await db.commit()
    await db.refresh(m)
    return ok(
        {
            "id": m.id,
            "sender_id": m.sender_id,
            "sender_role": m.sender_role,
            "content": m.content,
            "timestamp": m.timestamp.isoformat(),
            "attachments": m.attachments,
        }
    )


# ---------------------------------------------------------------------------
# Evidence packages per case
# ---------------------------------------------------------------------------


@router.get("/cases/{case_id}/evidence")
async def list_case_evidence(case_id: str, db: DBSessionDep) -> APIResponse[list[dict[str, Any]]]:
    case = (await db.execute(select(LECase).where(LECase.id == case_id))).scalar_one_or_none()
    if case is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "case not found")
    rows = (
        (
            await db.execute(
                select(EvidencePackage)
                .where(
                    (EvidencePackage.case_id == case_id)
                    | (EvidencePackage.cluster_id.in_(case.cluster_ids or []))
                )
                .order_by(EvidencePackage.generated_at.desc())
            )
        )
        .scalars()
        .all()
    )
    return ok(
        [
            {
                "id": p.id,
                "cluster_id": p.cluster_id,
                "case_id": p.case_id,
                "version": p.version,
                "generated_at": p.generated_at.isoformat() if p.generated_at else None,
                "page_count": p.page_count,
                "file_size": p.file_size,
                "file_hash": p.file_hash,
                "summary": p.summary,
            }
            for p in rows
        ]
    )


@router.post(
    "/cases/{case_id}/evidence/generate",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_role(ROLE_INVESTIGATOR))],
)
async def generate_case_evidence(case_id: str, db: DBSessionDep) -> APIResponse[list[dict[str, Any]]]:
    """Generate (or regenerate) one evidence package per cluster on the case."""

    case = (await db.execute(select(LECase).where(LECase.id == case_id))).scalar_one_or_none()
    if case is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "case not found")
    if not case.cluster_ids:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "case has no clusters attached")
    built = []
    for cid in case.cluster_ids:
        pkg = await build_for_cluster(cid, case_id=case_id, generated_by="system")
        built.append(
            {
                "id": pkg.id,
                "cluster_id": pkg.cluster_id,
                "version": pkg.version,
                "page_count": pkg.page_count,
                "file_size": pkg.file_size,
            }
        )
    return ok(built)


@router.get("/cases/{case_id}/evidence/{pkg_id}/download")
async def download_case_evidence(case_id: str, pkg_id: str, db: DBSessionDep) -> StreamingResponse:
    pkg = (await db.execute(select(EvidencePackage).where(EvidencePackage.id == pkg_id))).scalar_one_or_none()
    if pkg is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "evidence package not found")
    case = (await db.execute(select(LECase).where(LECase.id == case_id))).scalar_one_or_none()
    if case is None or (pkg.case_id != case_id and pkg.cluster_id not in (case.cluster_ids or [])):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "package not on this case")

    pdf_bytes = await _evidence_bytes(pkg)
    return StreamingResponse(
        iter([pdf_bytes]),
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="evidence-{pkg.id}.pdf"',
            "X-Evidence-Hash": pkg.file_hash,
            "X-Evidence-Version": str(pkg.version),
        },
    )


async def _evidence_bytes(pkg: EvidencePackage) -> bytes:
    if pkg.file_path.startswith("s3://"):
        from minio import Minio

        from config.settings import get_settings

        s = get_settings()
        client = Minio(
            s.minio_endpoint,
            access_key=s.minio_access_key,
            secret_key=s.minio_secret_key.get_secret_value(),
            secure=s.minio_secure,
        )
        without_scheme = pkg.file_path[len("s3://") :]
        bucket, _, key = without_scheme.partition("/")
        response = client.get_object(bucket, key)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()
    # Local fallback — rebuild on demand.
    from core.evidence.builder import _gather  # noqa: PLC0415
    from core.evidence.export import render_pdf  # noqa: PLC0415

    payload = await _gather(pkg.cluster_id)
    pdf_bytes, _ = render_pdf(payload)
    return pdf_bytes


# ---------------------------------------------------------------------------
# Outcomes
# ---------------------------------------------------------------------------


def _outcome_to_dict(o: LEOutcome) -> dict[str, Any]:
    return {
        "id": o.id,
        "case_id": o.case_id,
        "outcome_type": o.outcome_type,
        "detail": o.detail,
        "amount_recovered": o.amount_recovered,
        "occurred_at": o.occurred_at.isoformat() if o.occurred_at else None,
        "reported_by": o.reported_by,
    }


@router.get("/cases/{case_id}/outcomes")
async def list_outcomes(case_id: str, db: DBSessionDep) -> APIResponse[list[dict[str, Any]]]:
    rows = (
        (
            await db.execute(
                select(LEOutcome).where(LEOutcome.case_id == case_id).order_by(LEOutcome.occurred_at.desc())
            )
        )
        .scalars()
        .all()
    )
    return ok([_outcome_to_dict(o) for o in rows])


class OutcomeCreateRequest(BaseModel):
    outcome_type: str = Field(..., max_length=40)
    detail: str | None = None
    amount_recovered: float | None = None
    occurred_at: datetime | None = None


@router.post(
    "/cases/{case_id}/outcomes",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_role(ROLE_INVESTIGATOR, ROLE_LAW_ENFORCEMENT))],
)
async def add_outcome(
    case_id: str, payload: OutcomeCreateRequest, db: DBSessionDep
) -> APIResponse[dict[str, Any]]:
    if (await db.execute(select(LECase).where(LECase.id == case_id))).scalar_one_or_none() is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "case not found")
    o = LEOutcome(
        id=_new_id("outcome"),
        case_id=case_id,
        outcome_type=payload.outcome_type,
        detail=payload.detail,
        amount_recovered=payload.amount_recovered,
        occurred_at=payload.occurred_at or datetime.now(UTC),
        reported_by="system",
    )
    db.add(o)
    await db.commit()
    await db.refresh(o)
    return ok(_outcome_to_dict(o))


# ---------------------------------------------------------------------------
# Inbound intelligence
# ---------------------------------------------------------------------------


class InboundIntelRequest(BaseModel):
    """Intelligence package received from a law-enforcement source."""

    agency_id: str | None = None
    summary: str = Field(..., min_length=2, max_length=500)
    severity: str = Field("high", pattern="^(low|medium|high|critical)$")
    identifier_type: str | None = Field(None, pattern="^(msisdn|wallet|imei|imsi)$")
    identifier: str | None = None
    cluster_id: str | None = None
    metadata: dict[str, Any] | None = None


@router.post(
    "/inbound-intel",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_role(ROLE_INVESTIGATOR, ROLE_LAW_ENFORCEMENT))],
)
async def inbound_intel(payload: InboundIntelRequest, db: DBSessionDep) -> APIResponse[dict[str, Any]]:
    """Receive intelligence from law enforcement and create an alert.

    Real implementations would also enqueue a Seed for the mesh-expansion
    pipeline. We persist the payload as an Alert row tagged
    ``type='le_inbound_intel'`` so the analyst queue picks it up.
    """

    # Lazy import — Alert lives next to other routes' models; keep this
    # cheap if the route module is imported in isolation (e.g. tests).
    from db.models import Alert  # noqa: PLC0415

    alert = Alert(
        id=_new_id("alert"),
        created_at=datetime.now(UTC),
        type="le_inbound_intel",
        severity=payload.severity,
        title=f"LE intel: {payload.summary[:80]}",
        description=payload.summary,
        target_type=payload.identifier_type,
        target_id=payload.identifier,
        cluster_id=payload.cluster_id,
        acknowledged=False,
        rule_id=None,
        extra={"agency_id": payload.agency_id, "metadata": payload.metadata},
    )
    db.add(alert)
    await db.commit()
    await db.refresh(alert)
    return ok({"alert_id": alert.id, "title": alert.title, "severity": alert.severity})
