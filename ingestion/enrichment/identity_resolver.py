"""Resolve an identifier across the MSISDN ↔ IMSI ↔ IMEI ↔ Wallet mesh.

Used by the Kafka consumers to enrich incoming events. Real implementation
would memoise hot lookups in Redis; for now we hit Neo4j directly. The
resolver is intentionally tolerant — missing or partial input returns a
partial answer rather than raising.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from core.graph.client import Neo4jClient, get_neo4j_client


@dataclass
class IdentitySnapshot:
    msisdn: str | None = None
    wallet_id: str | None = None
    imsi: str | None = None
    imei: str | None = None
    cluster_id: str | None = None
    risk_score: float | None = None
    status: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "msisdn": self.msisdn,
            "wallet_id": self.wallet_id,
            "imsi": self.imsi,
            "imei": self.imei,
            "cluster_id": self.cluster_id,
            "risk_score": self.risk_score,
            "status": self.status,
        }


async def resolve_by_wallet(wallet_id: str, *, client: Neo4jClient | None = None) -> IdentitySnapshot:
    c = client or get_neo4j_client()
    rows = await c.execute_read(
        """
        MATCH (w:Wallet {wallet_id: $wallet_id})
        OPTIONAL MATCH (p:PhoneNumber)-[:OWNS_WALLET]->(w)
        OPTIONAL MATCH (s:SIM)-[:HAS_NUMBER]->(p)
        OPTIONAL MATCH (s)-[:INSERTED_IN]->(h:Handset)
        RETURN
            w.wallet_id     AS wallet_id,
            w.msisdn        AS msisdn,
            w.cluster_id    AS cluster_id,
            w.risk_score    AS risk_score,
            w.status        AS status,
            head(collect(DISTINCT s.imsi))  AS imsi,
            head(collect(DISTINCT h.imei))  AS imei
        """,
        {"wallet_id": wallet_id},
    )
    if not rows:
        return IdentitySnapshot(wallet_id=wallet_id)
    r = rows[0]
    return IdentitySnapshot(
        wallet_id=r.get("wallet_id"),
        msisdn=r.get("msisdn"),
        imsi=r.get("imsi"),
        imei=r.get("imei"),
        cluster_id=r.get("cluster_id"),
        risk_score=float(r["risk_score"]) if r.get("risk_score") is not None else None,
        status=r.get("status"),
    )


async def resolve_by_msisdn(msisdn: str, *, client: Neo4jClient | None = None) -> IdentitySnapshot:
    c = client or get_neo4j_client()
    rows = await c.execute_read(
        """
        MATCH (p:PhoneNumber {msisdn: $msisdn})
        OPTIONAL MATCH (p)-[:OWNS_WALLET]->(w:Wallet)
        OPTIONAL MATCH (s:SIM)-[:HAS_NUMBER]->(p)
        OPTIONAL MATCH (s)-[:INSERTED_IN]->(h:Handset)
        RETURN
            p.msisdn  AS msisdn,
            head(collect(DISTINCT w.wallet_id)) AS wallet_id,
            head(collect(DISTINCT w.cluster_id)) AS cluster_id,
            head(collect(DISTINCT w.risk_score)) AS risk_score,
            head(collect(DISTINCT w.status)) AS status,
            head(collect(DISTINCT s.imsi)) AS imsi,
            head(collect(DISTINCT h.imei)) AS imei
        """,
        {"msisdn": msisdn},
    )
    if not rows:
        return IdentitySnapshot(msisdn=msisdn)
    r = rows[0]
    return IdentitySnapshot(
        msisdn=r.get("msisdn") or msisdn,
        wallet_id=r.get("wallet_id"),
        imsi=r.get("imsi"),
        imei=r.get("imei"),
        cluster_id=r.get("cluster_id"),
        risk_score=float(r["risk_score"]) if r.get("risk_score") is not None else None,
        status=r.get("status"),
    )
