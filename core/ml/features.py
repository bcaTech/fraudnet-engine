"""Feature extraction for ML scoring.

Pulls per-wallet feature vectors from Neo4j. Designed to be cheap on
the hot path (single Cypher per call, no per-wallet round-trip) and to
share its feature definitions with both training and inference.

The vector is intentionally small and behavioural-only — content + GNN
features live in their own modules. Adding a feature here is two
edits: extend ``FEATURE_NAMES`` and the Cypher in :func:`fetch_batch`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from core.graph.client import Neo4jClient, get_neo4j_client

FEATURE_NAMES: tuple[str, ...] = (
    "risk_score",
    "behavioral_score",
    "predictive_score",
    "kyc_tier",
    "balance",
    "out_degree",  # number of distinct counterparties (sent)
    "in_degree",  # number of distinct counterparties (received)
    "outbound_total_30d",  # sum of SENT_TO amounts in last 30d
    "inbound_total_30d",  # sum of inbound SENT_TO amounts in last 30d
    "cashouts_30d",  # number of CASHED_OUT_AT in last 30d
    "is_sleeper",
    "on_watchlist",
)


@dataclass
class WalletFeatures:
    wallet_id: str
    cluster_id: str | None
    label: int  # 1 = fraud-linked (member of any cluster), 0 = clean
    vector: list[float]


_FETCH_CYPHER = """
MATCH (w:Wallet)
WHERE w.wallet_id IN $wallet_ids
OPTIONAL MATCH (w)-[r_out:SENT_TO]->()
WHERE r_out.timestamp >= datetime() - duration({days: 30})
WITH w,
     count(DISTINCT r_out) AS sent_count,
     sum(coalesce(r_out.amount, 0.0)) AS outbound_total
OPTIONAL MATCH ()-[r_in:SENT_TO]->(w)
WHERE r_in.timestamp >= datetime() - duration({days: 30})
WITH w, sent_count, outbound_total,
     count(DISTINCT r_in) AS recv_count,
     sum(coalesce(r_in.amount, 0.0)) AS inbound_total
OPTIONAL MATCH (w)-[c:CASHED_OUT_AT]->()
WHERE c.timestamp >= datetime() - duration({days: 30})
WITH w, sent_count, recv_count, outbound_total, inbound_total,
     count(c) AS cashouts
RETURN
    w.wallet_id AS wallet_id,
    w.cluster_id AS cluster_id,
    coalesce(w.risk_score, 0.0) AS risk_score,
    coalesce(w.behavioral_score, 0.0) AS behavioral_score,
    coalesce(w.predictive_score, 0.0) AS predictive_score,
    coalesce(w.kyc_tier, 0) AS kyc_tier,
    coalesce(w.balance, 0.0) AS balance,
    sent_count AS out_degree,
    recv_count AS in_degree,
    outbound_total AS outbound_total_30d,
    inbound_total AS inbound_total_30d,
    cashouts AS cashouts_30d,
    coalesce(w.is_sleeper, false) AS is_sleeper,
    coalesce(w.on_watchlist, false) AS on_watchlist
"""


_FETCH_ALL_CYPHER = _FETCH_CYPHER.replace("WHERE w.wallet_id IN $wallet_ids", "") + "\nLIMIT $limit"


async def fetch_batch(wallet_ids: list[str], *, client: Neo4jClient | None = None) -> list[WalletFeatures]:
    if not wallet_ids:
        return []
    c = client or get_neo4j_client()
    rows = await c.execute_read(_FETCH_CYPHER, {"wallet_ids": wallet_ids})
    return [_row_to_features(r) for r in rows]


async def fetch_population(*, limit: int = 1000, client: Neo4jClient | None = None) -> list[WalletFeatures]:
    """Fetch a batch of wallets without naming them, capped at ``limit``.
    Used by training jobs to build a sample."""

    c = client or get_neo4j_client()
    rows = await c.execute_read(_FETCH_ALL_CYPHER, {"limit": limit})
    return [_row_to_features(r) for r in rows]


def _row_to_features(r: dict[str, Any]) -> WalletFeatures:
    cluster_id = r.get("cluster_id")
    return WalletFeatures(
        wallet_id=str(r["wallet_id"]),
        cluster_id=cluster_id,
        label=1 if cluster_id else 0,
        vector=[
            float(r.get("risk_score") or 0.0),
            float(r.get("behavioral_score") or 0.0),
            float(r.get("predictive_score") or 0.0),
            float(r.get("kyc_tier") or 0),
            float(r.get("balance") or 0.0),
            float(r.get("out_degree") or 0),
            float(r.get("in_degree") or 0),
            float(r.get("outbound_total_30d") or 0.0),
            float(r.get("inbound_total_30d") or 0.0),
            float(r.get("cashouts_30d") or 0),
            1.0 if r.get("is_sleeper") else 0.0,
            1.0 if r.get("on_watchlist") else 0.0,
        ],
    )
