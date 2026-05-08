"""Sleeper-wallet detection.

A *sleeper* is a wallet that:

1. Has been dormant for at least ``dormant_days`` days, AND
2. Recently received funds (within ``window_days``) FROM a wallet that
   either belongs to an active cluster or has a high risk score.

These wallets often function as cash-out endpoints for a fraud ring —
clean enough to look unremarkable, then activated for a single cash-out
push. Marking them lets the SafeGuard layer apply Send-with-Care or
Ask-Me-First on their next outbound transfer.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from config.logging import get_logger
from core.graph.client import Neo4jClient, get_neo4j_client

logger = get_logger(__name__)


async def detect_sleepers(
    *,
    dormant_days: int = 30,
    window_days: int = 7,
    risk_floor: float = 0.55,
    persist: bool = True,
    client: Neo4jClient | None = None,
) -> list[dict[str, Any]]:
    """Find dormant wallets that received fraud-linked inbound funds in
    the last ``window_days``. Sets ``is_sleeper=true`` and
    ``sleeper_detected_at`` on each match when ``persist=True``."""

    c = client or get_neo4j_client()
    rows = await c.execute_read(
        """
        MATCH (sender:Wallet)-[r:SENT_TO]->(target:Wallet)
        WHERE r.timestamp >= datetime() - duration({days: $window_days})
          AND (
              sender.cluster_id IS NOT NULL
              OR coalesce(sender.risk_score, 0.0) >= $risk_floor
          )
          AND target.last_activity IS NOT NULL
          AND datetime(target.last_activity) <= datetime() - duration({days: $dormant_days})
        WITH target, count(DISTINCT sender) AS fraud_senders,
             sum(r.amount) AS inbound_value,
             collect(DISTINCT sender.wallet_id)[..5] AS sample_senders
        RETURN
            target.wallet_id AS wallet_id,
            target.msisdn    AS msisdn,
            coalesce(target.risk_score, 0.0) AS risk_score,
            target.cluster_id AS cluster_id,
            fraud_senders,
            inbound_value,
            sample_senders
        ORDER BY inbound_value DESC, fraud_senders DESC
        LIMIT 100
        """,
        {
            "window_days": window_days,
            "dormant_days": dormant_days,
            "risk_floor": risk_floor,
        },
    )
    payload = [
        {
            "wallet_id": r.get("wallet_id"),
            "msisdn": r.get("msisdn"),
            "risk_score": float(r.get("risk_score") or 0.0),
            "cluster_id": r.get("cluster_id"),
            "fraud_senders": int(r.get("fraud_senders") or 0),
            "inbound_value": float(r.get("inbound_value") or 0.0),
            "sample_senders": r.get("sample_senders") or [],
        }
        for r in rows
    ]
    if persist and payload:
        await c.execute_write(
            """
            UNWIND $rows AS row
            MATCH (w:Wallet {wallet_id: row.wallet_id})
            SET w.is_sleeper = true,
                w.sleeper_detected_at = datetime(),
                w.sleeper_inbound_value = row.inbound_value,
                w.sleeper_fraud_senders = row.fraud_senders
            """,
            {"rows": payload},
        )
        logger.info("analytics.sleeper.persisted", count=len(payload))
    return payload


async def run_sleeper_scan() -> dict[str, Any]:
    sleepers = await detect_sleepers()
    return {
        "sleeper_count": len(sleepers),
        "sleepers": sleepers,
        "scanned_at": datetime.now(UTC).isoformat(),
    }
