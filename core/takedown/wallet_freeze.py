"""Freeze every wallet in a cluster as part of a takedown.

The Cypher write is real — wallets are tagged ``status='frozen'`` with
``freeze_date`` set. The downstream call to the MoMo BSS API
(``apply_external_freeze``) is a documented stub: in production it
posts to a Scancom / MoMo BSS endpoint per wallet; here it just logs.

Returns the count actually frozen so the executor can record it on
:class:`db.models.Takedown`.
"""

from __future__ import annotations

from typing import Any

from config.logging import get_logger
from core.graph.client import Neo4jClient, get_neo4j_client

logger = get_logger(__name__)


async def freeze_cluster_wallets(
    cluster_id: str, *, client: Neo4jClient | None = None
) -> dict[str, Any]:
    c = client or get_neo4j_client()
    rows = await c.execute_write(
        """
        MATCH (cl:Cluster {cluster_id: $cluster_id})
        MATCH (w:Wallet)-[:BELONGS_TO]->(cl)
        WHERE coalesce(w.status, 'active') <> 'frozen'
        SET w.status = 'frozen',
            w.freeze_date = datetime(),
            w.freeze_source = 'takedown'
        RETURN w.wallet_id AS wallet_id
        """,
        {"cluster_id": cluster_id},
    )
    frozen = [r["wallet_id"] for r in rows if r.get("wallet_id")]
    for wallet_id in frozen:
        await apply_external_freeze(wallet_id)
    logger.info(
        "takedown.wallet_freeze.complete",
        cluster_id=cluster_id,
        count=len(frozen),
    )
    return {"frozen": len(frozen), "wallet_ids": frozen}


async def apply_external_freeze(wallet_id: str) -> bool:  # noqa: ARG001
    """Stub for the MoMo BSS freeze API.

    Real implementation:

    - HTTP POST to ``${MOMO_BSS_URL}/wallets/{wallet_id}/freeze`` with
      HMAC-signed body, idempotency key from ``takedown_id``.
    - Retry policy: 3 attempts, exponential backoff. Failures are
      surfaced through the takedown step's ``detail`` JSON so the
      operator can re-run.
    - On success, record the BSS confirmation id.

    The graph-level freeze above is a faithful stand-in for demos and
    tests; the external integration sits behind this seam.
    """

    logger.debug("takedown.wallet_freeze.external_stub", wallet_id=wallet_id)
    return True
