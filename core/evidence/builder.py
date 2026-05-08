"""Evidence-package assembly + persistence.

The builder is the orchestration layer: it pulls all the data the
evidence pack needs (cluster summary, members, fund traces, timeline,
agents, alerts), passes it to the PDF exporter, hashes the result,
stores it on MinIO, and writes the :class:`EvidencePackage` row.

The function returns the persisted record. Failures at any stage raise
so the takedown workflow can decide whether to surface the error to
the analyst or fall back to a previous package version.
"""

from __future__ import annotations

import hashlib
import io
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select

from config.logging import get_logger
from config.settings import get_settings
from core.evidence.export import render_pdf
from core.evidence.fund_trace import trace_for_evidence
from core.evidence.timeline import build_timeline
from core.graph.client import get_neo4j_client
from db.models import EvidencePackage
from db.session import get_async_session

logger = get_logger(__name__)


async def _gather(cluster_id: str) -> dict[str, Any]:
    """Pull everything the PDF exporter needs in one place."""

    client = get_neo4j_client()
    rows = await client.execute_read(
        """
        MATCH (cl:Cluster {cluster_id: $cluster_id})
        OPTIONAL MATCH (n)-[:BELONGS_TO]->(cl)
        WITH cl, count(DISTINCT n) AS member_count, collect(DISTINCT labels(n)[0]) AS labels
        RETURN cl {
            .cluster_id, .name, .seed_type, .seed_date, .seed_node_id,
            .node_count, .confidence_score, .status, .estimated_fraud_value,
            .density, .isolation_score
        } AS cluster, member_count, labels
        """,
        {"cluster_id": cluster_id},
    )
    if not rows:
        raise ValueError(f"cluster '{cluster_id}' not found")
    cluster = rows[0].get("cluster") or {}
    cluster["member_count"] = int(rows[0].get("member_count") or 0)
    cluster["member_labels"] = [
        lab for lab in (rows[0].get("labels") or []) if lab
    ]

    members = await client.execute_read(
        """
        MATCH (cl:Cluster {cluster_id: $cluster_id})
        MATCH (n)-[r:BELONGS_TO]->(cl)
        RETURN
            coalesce(n.wallet_id, n.imei, n.imsi, n.msisdn, n.agent_id) AS id,
            labels(n)[0] AS kind,
            r.role AS role,
            coalesce(r.confidence, 0.0) AS confidence,
            coalesce(n.risk_score, 0.0) AS risk_score,
            coalesce(n.status, 'active') AS status
        ORDER BY confidence DESC
        LIMIT 200
        """,
        {"cluster_id": cluster_id},
    )

    linked_agents = await client.execute_read(
        """
        MATCH (a:Agent)-[r:LINKED_TO]->(cl:Cluster {cluster_id: $cluster_id})
        RETURN a.agent_id AS agent_id, a.name AS name, a.area_name AS area,
               coalesce(a.risk_score, 0.0) AS risk_score,
               a.classification AS classification,
               coalesce(r.fraud_cashout_count, 0) AS cashout_count
        ORDER BY cashout_count DESC
        """,
        {"cluster_id": cluster_id},
    )

    fund_data = await trace_for_evidence(cluster_id)
    timeline = await build_timeline(cluster_id)

    return {
        "cluster": cluster,
        "members": [
            {
                **{k: v for k, v in m.items()},
                "confidence": float(m.get("confidence") or 0.0),
                "risk_score": float(m.get("risk_score") or 0.0),
            }
            for m in members
        ],
        "linked_agents": [
            {
                **{k: v for k, v in a.items()},
                "risk_score": float(a.get("risk_score") or 0.0),
                "cashout_count": int(a.get("cashout_count") or 0),
            }
            for a in linked_agents
        ],
        "fund_flow": fund_data,
        "timeline": timeline,
    }


def _store_to_minio(file_bytes: bytes, *, key: str) -> str | None:
    """Best-effort MinIO upload. Returns the object key (or None on
    failure). The on-disk fallback path keeps local dev usable when
    MinIO is down."""

    try:
        from minio import Minio

        s = get_settings()
        client = Minio(
            s.minio_endpoint,
            access_key=s.minio_access_key,
            secret_key=s.minio_secret_key.get_secret_value(),
            secure=s.minio_secure,
        )
        bucket = s.minio_bucket
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
        client.put_object(
            bucket,
            key,
            io.BytesIO(file_bytes),
            length=len(file_bytes),
            content_type="application/pdf",
        )
        return f"s3://{bucket}/{key}"
    except Exception as exc:  # noqa: BLE001 — fall back to local storage
        logger.warning("evidence.minio_upload_failed", key=key, error=str(exc))
        return None


async def build_for_cluster(
    cluster_id: str,
    *,
    case_id: str | None = None,
    takedown_id: str | None = None,
    generated_by: str | None = None,
) -> EvidencePackage:
    """Assemble + render + persist an evidence package for ``cluster_id``.

    Returns the persisted :class:`EvidencePackage` row. The PDF lives at
    ``record.file_path`` (MinIO key, or local path on fallback).
    """

    payload = await _gather(cluster_id)
    pdf_bytes, page_count = render_pdf(payload)
    file_hash = hashlib.sha256(pdf_bytes).hexdigest()
    file_key = f"clusters/{cluster_id}/evidence-{file_hash[:12]}.pdf"
    stored_path = _store_to_minio(pdf_bytes, key=file_key) or f"local://{file_key}"

    async with get_async_session() as db:
        # Versioning: bump version per existing package for this cluster.
        existing = (
            await db.execute(
                select(EvidencePackage).where(
                    EvidencePackage.cluster_id == cluster_id
                )
            )
        ).scalars().all()
        version = max((p.version for p in existing), default=0) + 1

        record = EvidencePackage(
            id=f"evid-{uuid.uuid4().hex[:12]}",
            cluster_id=cluster_id,
            case_id=case_id,
            takedown_id=takedown_id,
            generated_at=datetime.now(timezone.utc),
            generated_by=generated_by or "system",
            version=version,
            file_hash=file_hash,
            file_path=stored_path,
            page_count=page_count,
            file_size=len(pdf_bytes),
            summary=(
                f"Evidence pack v{version} for cluster {cluster_id} "
                f"({payload['cluster'].get('name') or cluster_id}). "
                f"{len(payload['members'])} members, "
                f"{len(payload['linked_agents'])} cashout agents, "
                f"GHS {payload['fund_flow']['total_traced_value']:.2f} traced."
            ),
        )
        db.add(record)
        await db.commit()
        await db.refresh(record)

    logger.info(
        "evidence.built",
        cluster_id=cluster_id,
        version=version,
        page_count=page_count,
        size=len(pdf_bytes),
        path=stored_path,
    )
    return record
