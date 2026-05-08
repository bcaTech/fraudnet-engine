"""Campaign detection endpoints.

Backed by :mod:`core.analytics.campaign`. The detection sweep is
expensive (multiple Cypher rollups), so the route reads from a Redis
cache populated by the ``tasks.periodic.refresh_campaigns_cache``
beat job (default cadence: every 15 minutes). Cache miss falls back
to running the detection inline so the first request after a cold
start still works.

Cache + shape helpers live in ``core.analytics.campaign`` so the
Celery refresh task can reuse them without importing ``api.*`` (which
its forked workers can't always resolve).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, HTTPException, status

from api.dependencies import Neo4jDep
from api.schemas import APIResponse, ok
from config.logging import get_logger
from core.analytics.campaign import (
    detect_campaigns,
    read_cache,
    shape_campaigns,
    write_cache,
)

logger = get_logger(__name__)

router = APIRouter(prefix="/api/campaigns", tags=["campaigns"])


async def _ensure_cache() -> dict[str, Any]:
    """Return the cached payload, populating it on a miss."""

    cached = await read_cache()
    if cached is not None:
        cached.setdefault("from_cache", True)
        return cached
    raw = await detect_campaigns()
    payload: dict[str, Any] = {
        "scanned_at": raw.get("scanned_at") or datetime.now(UTC).isoformat(),
        "campaigns": shape_campaigns(raw),
        "raw": raw,
        "from_cache": False,
    }
    await write_cache(payload)
    return payload


@router.get("")
async def list_campaigns(neo4j: Neo4jDep) -> APIResponse[dict[str, Any]]:
    """Return campaign detections, preferring the Redis cache."""

    payload = await _ensure_cache()
    return ok(
        {
            "scanned_at": payload.get("scanned_at"),
            "from_cache": payload.get("from_cache", True),
            "count": len(payload.get("campaigns") or []),
            "campaigns": payload.get("campaigns") or [],
        }
    )


@router.get("/{campaign_id}")
async def get_campaign(campaign_id: str) -> APIResponse[dict[str, Any]]:
    """Detail for a single campaign id. Reads from the cached list — a
    campaign id has no meaning independent of the detection cycle that
    produced it."""

    payload = await _ensure_cache()
    for c in payload.get("campaigns") or []:
        if c.get("id") == campaign_id:
            return ok(
                {
                    **c,
                    "scanned_at": payload.get("scanned_at"),
                    "timeline": _timeline_for(payload.get("campaigns") or [], kind=c["kind"]),
                }
            )
    raise HTTPException(status.HTTP_404_NOT_FOUND, "campaign not found in cache")


def _timeline_for(campaigns: list[dict[str, Any]], *, kind: str) -> list[dict[str, Any]]:
    """Bucket → count series across every detection of the same kind.
    Frontend uses this to render the "is this still active" chart."""

    counts: dict[str, int] = {}
    for c in campaigns:
        if c.get("kind") != kind:
            continue
        bucket = str(c.get("bucket") or "")
        if not bucket:
            continue
        detail = c.get("detail") or {}
        n = detail.get("sim_count") or detail.get("wallet_count") or detail.get("sender_count") or 0
        counts[bucket] = counts.get(bucket, 0) + int(n)
    return [{"bucket": b, "count": counts[b]} for b in sorted(counts.keys())]
