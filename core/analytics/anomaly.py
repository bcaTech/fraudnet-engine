"""Anomaly detection across device, transaction, temporal, and velocity
dimensions.

This is the lighter-weight, rules-of-thumb anomaly layer that runs every
few minutes — the heavyweight learned-model layer (``core/ml/``) lives
separately. The thresholds here are intentionally conservative; the
goal is to flag "weird enough to look at" cases for the analyst queue,
not to be the source of truth for fraud detection.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from config.logging import get_logger
from core.graph.client import Neo4jClient, get_neo4j_client

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Device anomalies
# ---------------------------------------------------------------------------


async def device_anomalies(
    *,
    msisdn_threshold: int = 5,
    swap_threshold: int = 2,
    client: Neo4jClient | None = None,
) -> dict[str, Any]:
    """Find handsets shared across many MSISDNs and SIMs swapped >N times."""

    c = client or get_neo4j_client()
    multi_sim = await c.execute_read(
        """
        MATCH (s:SIM)-[:INSERTED_IN]->(h:Handset)
        WITH h, count(DISTINCT s.msisdn) AS msisdn_count
        WHERE msisdn_count >= $threshold
        RETURN h.imei AS imei, msisdn_count
        ORDER BY msisdn_count DESC
        LIMIT 100
        """,
        {"threshold": msisdn_threshold},
    )
    swappers = await c.execute_read(
        """
        MATCH (s:SIM)
        WHERE coalesce(s.swap_count, 0) >= $threshold
        RETURN s.imsi AS imsi, s.swap_count AS swap_count
        ORDER BY swap_count DESC
        LIMIT 100
        """,
        {"threshold": swap_threshold},
    )
    return {
        "multi_sim_handsets": [dict(r) for r in multi_sim],
        "swap_chain_sims": [dict(r) for r in swappers],
    }


# ---------------------------------------------------------------------------
# Transaction anomalies
# ---------------------------------------------------------------------------


async def transaction_anomalies(
    *,
    amount_zscore_threshold: float = 3.0,
    structuring_threshold: int = 5,
    client: Neo4jClient | None = None,
) -> dict[str, Any]:
    """Round-amount structuring and outlier amounts.

    *Structuring*: a wallet with ``structuring_threshold`` or more
    same-round-amount sends in 24h.

    *Outlier amount*: transactions whose amount is more than
    ``amount_zscore_threshold`` standard deviations above their wallet's
    mean send size.
    """

    c = client or get_neo4j_client()
    structuring = await c.execute_read(
        """
        MATCH (w:Wallet)-[r:SENT_TO]->()
        WHERE r.timestamp >= datetime() - duration({days: 1})
          AND r.amount % 100 = 0
        WITH w, r.amount AS amount, count(*) AS n
        WHERE n >= $threshold
        RETURN w.wallet_id AS wallet_id, amount, n
        ORDER BY n DESC
        LIMIT 50
        """,
        {"threshold": structuring_threshold},
    )
    outliers = await c.execute_read(
        """
        MATCH (w:Wallet)-[r:SENT_TO]->()
        WHERE r.timestamp >= datetime() - duration({days: 7})
        WITH w, collect(r.amount) AS amounts
        WHERE size(amounts) >= 5
        WITH w, amounts,
             reduce(s=0.0, a IN amounts | s + a) / size(amounts) AS mean
        WITH w, amounts, mean,
             reduce(s=0.0, a IN amounts | s + (a - mean) * (a - mean)) / size(amounts) AS variance
        WITH w, mean, sqrt(variance) AS sd, amounts
        WHERE sd > 0
        UNWIND amounts AS amount
        WITH w, mean, sd, amount, (amount - mean) / sd AS z
        WHERE z >= $threshold
        RETURN w.wallet_id AS wallet_id, max(z) AS max_z, max(amount) AS max_amount
        ORDER BY max_z DESC
        LIMIT 50
        """,
        {"threshold": amount_zscore_threshold},
    )
    return {
        "structuring_candidates": [dict(r) for r in structuring],
        "amount_outliers": [
            {**dict(r), "max_z": float(r["max_z"]), "max_amount": float(r["max_amount"])} for r in outliers
        ],
    }


# ---------------------------------------------------------------------------
# Temporal / velocity anomalies
# ---------------------------------------------------------------------------


async def temporal_anomalies(
    *,
    off_hours_start: int = 0,
    off_hours_end: int = 5,
    burst_threshold: int = 10,
    burst_minutes: int = 5,
    client: Neo4jClient | None = None,
) -> dict[str, Any]:
    """Off-hours activity (00:00–05:00 UTC by default) and minute-bucket
    bursts (>N transactions in a 5-minute window)."""

    c = client or get_neo4j_client()
    off_hours = await c.execute_read(
        """
        MATCH (w:Wallet)-[r:SENT_TO]->()
        WHERE r.timestamp >= datetime() - duration({days: 7})
          AND r.timestamp.hour >= $start
          AND r.timestamp.hour <= $end
        WITH w, count(*) AS off_hour_count
        WHERE off_hour_count >= 5
        RETURN w.wallet_id AS wallet_id, off_hour_count
        ORDER BY off_hour_count DESC
        LIMIT 50
        """,
        {"start": off_hours_start, "end": off_hours_end},
    )
    bursts = await c.execute_read(
        """
        MATCH (w:Wallet)-[r:SENT_TO]->()
        WHERE r.timestamp >= datetime() - duration({days: 1})
        WITH w,
             datetime({
                year: r.timestamp.year, month: r.timestamp.month,
                day: r.timestamp.day, hour: r.timestamp.hour,
                minute: (r.timestamp.minute / $bucket) * $bucket
             }) AS bucket,
             count(*) AS n
        WHERE n >= $threshold
        RETURN w.wallet_id AS wallet_id, bucket, n
        ORDER BY n DESC
        LIMIT 50
        """,
        {"threshold": burst_threshold, "bucket": burst_minutes},
    )
    return {
        "off_hours_activity": [dict(r) for r in off_hours],
        "burst_windows": [
            {**{k: v for k, v in r.items() if k != "bucket"}, "bucket": str(r.get("bucket"))} for r in bursts
        ],
    }


async def velocity_anomalies(
    *,
    send_rate_threshold: int = 8,
    window_minutes: int = 5,
    client: Neo4jClient | None = None,
) -> list[dict[str, Any]]:
    """Wallets exceeding ``send_rate_threshold`` outbound transfers in the
    most recent ``window_minutes`` window."""

    c = client or get_neo4j_client()
    rows = await c.execute_read(
        """
        MATCH (w:Wallet)-[r:SENT_TO]->()
        WHERE r.timestamp >= datetime() - duration({minutes: $window})
        WITH w, count(*) AS n
        WHERE n >= $threshold
        RETURN w.wallet_id AS wallet_id, n
        ORDER BY n DESC
        LIMIT 50
        """,
        {"threshold": send_rate_threshold, "window": window_minutes},
    )
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Top-level entry
# ---------------------------------------------------------------------------


async def run_anomaly_scan() -> dict[str, Any]:
    """One-shot run of every anomaly check. Suitable for the periodic
    Celery task or for ad-hoc invocation from a notebook."""

    client = get_neo4j_client()
    return {
        "device": await device_anomalies(client=client),
        "transactions": await transaction_anomalies(client=client),
        "temporal": await temporal_anomalies(client=client),
        "velocity": await velocity_anomalies(client=client),
        "scanned_at": datetime.now(UTC).isoformat(),
    }
