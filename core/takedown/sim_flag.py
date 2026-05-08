"""Flag every SIM linked to a cluster's wallets.

The Cypher path traverses Wallet → PhoneNumber ↔ SIM and tags each
matching SIM with ``flagged=true`` and a takedown source. The Scancom
API integration is a stub — real implementation posts the IMSI list
to the Scancom registry for blocking.
"""

from __future__ import annotations

from typing import Any

from config.logging import get_logger
from core.graph.client import Neo4jClient, get_neo4j_client

logger = get_logger(__name__)


async def flag_cluster_sims(
    cluster_id: str, *, client: Neo4jClient | None = None
) -> dict[str, Any]:
    c = client or get_neo4j_client()
    rows = await c.execute_write(
        """
        MATCH (cl:Cluster {cluster_id: $cluster_id})
        MATCH (w:Wallet)-[:BELONGS_TO]->(cl)
        MATCH (p:PhoneNumber {msisdn: w.msisdn})
        MATCH (s:SIM)-[:HAS_NUMBER]->(p)
        WHERE coalesce(s.flagged, false) = false
        SET s.flagged = true,
            s.flag_reason = 'takedown',
            s.flag_date = datetime()
        RETURN s.imsi AS imsi
        """,
        {"cluster_id": cluster_id},
    )
    flagged = [r["imsi"] for r in rows if r.get("imsi")]
    for imsi in flagged:
        await apply_external_sim_flag(imsi)
    logger.info(
        "takedown.sim_flag.complete", cluster_id=cluster_id, count=len(flagged)
    )
    return {"flagged": len(flagged), "imsis": flagged}


async def apply_external_sim_flag(imsi: str) -> bool:  # noqa: ARG001
    """Stub for the Scancom registry flag API.

    Real implementation: POST ``/registry/sim/flag`` with the IMSI,
    takedown reference, and operator/agency signoff. Idempotent on
    IMSI; returns the registry's confirmation id.
    """

    logger.debug("takedown.sim_flag.external_stub", imsi=imsi)
    return True
