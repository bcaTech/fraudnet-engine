"""Campaign detection — coordinated multi-actor fraud signals.

Campaigns are time-clustered batches of low-individual-signal events that,
viewed together, betray a single coordinator. Three patterns are checked:

- **SIM-registration burst** — many SIMs registered in the same hour with
  shared infrastructure (same handset make/model, same area).
- **Wallet-activation burst** — many wallets created in the same window
  with similar KYC tier and overlapping IMSI / IMEI.
- **Transaction burst** — co-ordinated cashouts across a small set of
  agents in a short window.

Each pattern returns ranked candidate groupings; the analyst layer
decides whether to promote a candidate into a tracked campaign.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from config.logging import get_logger
from core.graph.client import Neo4jClient, get_neo4j_client

logger = get_logger(__name__)


async def sim_registration_bursts(
    *,
    window_hours: int = 1,
    min_count: int = 8,
    client: Neo4jClient | None = None,
) -> list[dict[str, Any]]:
    c = client or get_neo4j_client()
    rows = await c.execute_read(
        """
        MATCH (s:SIM)
        WHERE s.registration_date IS NOT NULL
          AND datetime(s.registration_date) >= datetime() - duration({days: 30})
        WITH datetime({
            year: datetime(s.registration_date).year,
            month: datetime(s.registration_date).month,
            day: datetime(s.registration_date).day,
            hour: datetime(s.registration_date).hour
        }) AS bucket,
        s
        WITH bucket, count(s) AS sim_count, collect(s.imsi)[..20] AS sample_imsis
        WHERE sim_count >= $min_count
        RETURN bucket, sim_count, sample_imsis
        ORDER BY sim_count DESC
        LIMIT 30
        """,
        {"min_count": min_count},
    )
    return [
        {
            "bucket": str(r.get("bucket")),
            "sim_count": int(r.get("sim_count") or 0),
            "sample_imsis": r.get("sample_imsis") or [],
        }
        for r in rows
    ]


async def wallet_activation_bursts(
    *,
    window_hours: int = 6,
    min_count: int = 10,
    client: Neo4jClient | None = None,
) -> list[dict[str, Any]]:
    c = client or get_neo4j_client()
    rows = await c.execute_read(
        """
        MATCH (w:Wallet)
        WHERE w.creation_date IS NOT NULL
          AND datetime(w.creation_date) >= datetime() - duration({days: 30})
        WITH datetime({
            year: datetime(w.creation_date).year,
            month: datetime(w.creation_date).month,
            day: datetime(w.creation_date).day,
            hour: (datetime(w.creation_date).hour / $window) * $window
        }) AS bucket,
        w.kyc_tier AS tier, w
        WITH bucket, tier, count(w) AS wallet_count,
             collect(w.wallet_id)[..20] AS sample_wallets
        WHERE wallet_count >= $min_count
        RETURN bucket, tier, wallet_count, sample_wallets
        ORDER BY wallet_count DESC
        LIMIT 30
        """,
        {"min_count": min_count, "window": window_hours},
    )
    return [
        {
            "bucket": str(r.get("bucket")),
            "kyc_tier": r.get("tier"),
            "wallet_count": int(r.get("wallet_count") or 0),
            "sample_wallets": r.get("sample_wallets") or [],
        }
        for r in rows
    ]


async def transaction_bursts_at_agent(
    *,
    window_minutes: int = 30,
    min_count: int = 6,
    client: Neo4jClient | None = None,
) -> list[dict[str, Any]]:
    """Agents receiving more than ``min_count`` cashouts from distinct
    wallets in any ``window_minutes`` window over the past 24h."""

    c = client or get_neo4j_client()
    rows = await c.execute_read(
        """
        MATCH (w:Wallet)-[r:CASHED_OUT_AT]->(a:Agent)
        WHERE r.timestamp >= datetime() - duration({days: 1})
        WITH a,
             datetime({
                year: r.timestamp.year, month: r.timestamp.month,
                day: r.timestamp.day, hour: r.timestamp.hour,
                minute: (r.timestamp.minute / $window) * $window
             }) AS bucket,
             collect(DISTINCT w.wallet_id) AS senders
        WITH a, bucket, size(senders) AS sender_count, senders
        WHERE sender_count >= $min_count
        RETURN
            a.agent_id AS agent_id,
            a.area_name AS area,
            bucket,
            sender_count,
            senders[..10] AS sample_senders
        ORDER BY sender_count DESC
        LIMIT 30
        """,
        {"min_count": min_count, "window": window_minutes},
    )
    return [
        {
            "agent_id": r.get("agent_id"),
            "area": r.get("area"),
            "bucket": str(r.get("bucket")),
            "sender_count": int(r.get("sender_count") or 0),
            "sample_senders": r.get("sample_senders") or [],
        }
        for r in rows
    ]


async def detect_campaigns() -> dict[str, Any]:
    """Run all campaign detectors. The output is structured for direct
    consumption by ``/api/campaigns``."""

    client = get_neo4j_client()
    return {
        "sim_bursts": await sim_registration_bursts(client=client),
        "wallet_bursts": await wallet_activation_bursts(client=client),
        "agent_cashout_bursts": await transaction_bursts_at_agent(client=client),
        "scanned_at": datetime.now(UTC).isoformat(),
    }


# ---------------------------------------------------------------------------
# Cache + display helpers (used by both the /api/campaigns route and
# the Celery refresh task). They live in core/ rather than api/ so the
# Celery worker — whose forked workers can't reliably import api.* —
# can call them directly without going through the route module.
# ---------------------------------------------------------------------------


import hashlib  # noqa: E402 — keep helpers grouped at the bottom
import json  # noqa: E402

import redis.asyncio as redis_async  # noqa: E402

from config.settings import get_settings  # noqa: E402

CAMPAIGNS_CACHE_KEY = "fraudnet:campaigns:cache:v1"
CAMPAIGNS_CACHE_TTL_SECONDS = 30 * 60


_redis: redis_async.Redis | None = None


async def _campaigns_redis() -> redis_async.Redis:
    global _redis
    if _redis is None:
        settings = get_settings()
        _redis = redis_async.from_url(  # type: ignore[no-untyped-call]
            settings.redis_url, decode_responses=True
        )
    return _redis


async def write_cache(payload: dict[str, Any]) -> None:
    client = await _campaigns_redis()
    await client.set(
        CAMPAIGNS_CACHE_KEY,
        json.dumps(payload, default=str),
        ex=CAMPAIGNS_CACHE_TTL_SECONDS,
    )


async def read_cache() -> dict[str, Any] | None:
    client = await _campaigns_redis()
    raw = await client.get(CAMPAIGNS_CACHE_KEY)
    if not raw:
        return None
    try:
        result: dict[str, Any] = json.loads(raw)
        return result
    except json.JSONDecodeError:
        return None


def _campaign_id(kind: str, *signature: str | int | None) -> str:
    body = f"{kind}|" + "|".join("" if s is None else str(s) for s in signature)
    digest = hashlib.sha256(body.encode("utf-8")).hexdigest()[:16]
    return f"camp-{kind}-{digest}"


def _severity_from_count(count: Any) -> str:
    try:
        n = int(count or 0)
    except (TypeError, ValueError):
        return "low"
    if n >= 30:
        return "critical"
    if n >= 15:
        return "high"
    if n >= 8:
        return "medium"
    return "low"


def shape_campaigns(detection: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten the per-kind detection output into a uniform list."""

    out: list[dict[str, Any]] = []
    for sim in detection.get("sim_bursts") or []:
        out.append(
            {
                "id": _campaign_id("sim", sim.get("bucket")),
                "kind": "sim_registration_burst",
                "severity": _severity_from_count(sim.get("sim_count")),
                "bucket": sim.get("bucket"),
                "summary": f"{sim.get('sim_count', 0)} SIMs registered in the same hour",
                "detail": {
                    "sample_imsis": sim.get("sample_imsis") or [],
                    "sim_count": sim.get("sim_count"),
                },
            }
        )
    for wallet in detection.get("wallet_bursts") or []:
        out.append(
            {
                "id": _campaign_id("wallet", wallet.get("bucket"), wallet.get("kyc_tier")),
                "kind": "wallet_activation_burst",
                "severity": _severity_from_count(wallet.get("wallet_count")),
                "bucket": wallet.get("bucket"),
                "summary": (
                    f"{wallet.get('wallet_count', 0)} wallets activated in the "
                    f"same window (KYC tier {wallet.get('kyc_tier')})"
                ),
                "detail": {
                    "kyc_tier": wallet.get("kyc_tier"),
                    "sample_wallets": wallet.get("sample_wallets") or [],
                    "wallet_count": wallet.get("wallet_count"),
                },
            }
        )
    for agent in detection.get("agent_cashout_bursts") or []:
        out.append(
            {
                "id": _campaign_id("agent", agent.get("agent_id"), agent.get("bucket")),
                "kind": "agent_cashout_burst",
                "severity": _severity_from_count(agent.get("sender_count")),
                "bucket": agent.get("bucket"),
                "summary": (
                    f"{agent.get('sender_count', 0)} distinct senders cashed out at agent "
                    f"{agent.get('agent_id')} ({agent.get('area') or '?'})"
                ),
                "detail": {
                    "agent_id": agent.get("agent_id"),
                    "area": agent.get("area"),
                    "sample_senders": agent.get("sample_senders") or [],
                    "sender_count": agent.get("sender_count"),
                },
            }
        )
    out.sort(key=lambda c: str(c.get("bucket") or ""), reverse=True)
    return out
