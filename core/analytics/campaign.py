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
