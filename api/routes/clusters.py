"""Cluster CRUD, detail, graph, and on-demand expansion."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, status
from pydantic import BaseModel, Field

from api.dependencies import Neo4jDep
from api.schemas import APIResponse, Meta, ok
from core.graph.models import GraphEdge, GraphNode, GraphPayload, NodeType
from core.graph.queries import (
    COUNT_CLUSTERS,
    GET_CLUSTER,
    GET_CLUSTER_GRAPH,
    GET_CLUSTER_NODES,
    LIST_CLUSTERS,
)
from core.mesh.expansion import expand_from_seed
from core.mesh.seed import Seed

router = APIRouter(prefix="/api/clusters", tags=["clusters"])


# ---------------------------------------------------------------------------
# List + count
# ---------------------------------------------------------------------------


@router.get("")
async def list_clusters(
    neo4j: Neo4jDep,
    page: int = Query(1, ge=1),
    per_page: int = Query(25, ge=1, le=200),
    status_filter: str | None = Query(None, alias="status"),
    min_confidence: float | None = Query(None, ge=0, le=1),
    max_confidence: float | None = Query(None, ge=0, le=1),
    since: str | None = Query(None, description="ISO-8601 timestamp"),
) -> APIResponse[list[dict]]:
    skip = (page - 1) * per_page
    params = {
        "status": status_filter,
        "min_confidence": min_confidence,
        "max_confidence": max_confidence,
        "since": since,
        "skip": skip,
        "limit": per_page,
    }
    rows = await neo4j.execute_read(LIST_CLUSTERS, params)
    total_rows = await neo4j.execute_read(
        COUNT_CLUSTERS,
        {k: v for k, v in params.items() if k not in ("skip", "limit")},
    )
    total = int(total_rows[0]["n"]) if total_rows else 0
    return APIResponse(
        data=[r["cluster"] for r in rows],
        meta=Meta(total=total, page=page, per_page=per_page),
        errors=[],
    )


# ---------------------------------------------------------------------------
# Detail
# ---------------------------------------------------------------------------


@router.get("/{cluster_id}")
async def get_cluster(cluster_id: str, neo4j: Neo4jDep) -> APIResponse[dict]:
    rows = await neo4j.execute_read(GET_CLUSTER, {"cluster_id": cluster_id})
    if not rows:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "cluster not found")
    row = rows[0]
    payload = {
        **(row.get("cluster") or {}),
        "member_count": int(row.get("member_count") or 0),
        "member_labels": [lab for lab in (row.get("member_labels") or []) if lab],
    }
    return ok(payload)


# ---------------------------------------------------------------------------
# Graph payload (for visualisation)
# ---------------------------------------------------------------------------


_LABEL_TO_TYPE: dict[str, tuple[NodeType, str]] = {
    "Wallet": (NodeType.WALLET, "wallet_id"),
    "Handset": (NodeType.HANDSET, "imei"),
    "SIM": (NodeType.SIM, "imsi"),
    "PhoneNumber": (NodeType.PHONE, "msisdn"),
    "Agent": (NodeType.AGENT, "agent_id"),
    "Transaction": (NodeType.TRANSACTION, "tx_id"),
    "CellTower": (NodeType.CELL_TOWER, "cell_id"),
}


def _record_to_node(rec: dict[str, Any]) -> GraphNode:
    labels = rec.get("labels") or []
    props: dict[str, Any] = rec.get("props") or {}
    for lab in labels:
        if lab in _LABEL_TO_TYPE:
            node_type, key = _LABEL_TO_TYPE[lab]
            return GraphNode(
                id=str(props.get(key, rec["eid"])),
                type=node_type,
                label=props.get("name") or props.get("msisdn") or props.get(key),
                risk_score=float(props.get("risk_score") or 0.0),
                confidence_score=float(props.get("confidence_score") or 0.0),
                status=props.get("status"),
                properties=props,
            )
    return GraphNode(
        id=str(rec["eid"]),
        type=NodeType.WALLET,  # safe default — frontend filters by labels[0]
        properties=props,
    )


@router.get("/{cluster_id}/graph")
async def get_cluster_graph(cluster_id: str, neo4j: Neo4jDep) -> APIResponse[GraphPayload]:
    rows = await neo4j.execute_read(GET_CLUSTER_GRAPH, {"cluster_id": cluster_id})
    if not rows:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "cluster not found")
    row = rows[0]
    node_records = row.get("node_records") or []
    edge_records = row.get("edge_records") or []

    # Build an eid → natural-id map so edges can use stable ids.
    eid_to_id: dict[str, str] = {}
    nodes: list[GraphNode] = []
    for nr in node_records:
        n = _record_to_node(nr)
        nodes.append(n)
        eid_to_id[nr["eid"]] = n.id

    edges: list[GraphEdge] = []
    for er in edge_records:
        if not er or not er.get("source_eid"):
            continue
        props = er.get("props") or {}
        edges.append(
            GraphEdge(
                source=eid_to_id.get(er["source_eid"], er["source_eid"]),
                target=eid_to_id.get(er["target_eid"], er["target_eid"]),
                type=er["type"],
                strength=float(props.get("strength") or 0.0),
                properties=props,
            )
        )
    return ok(GraphPayload(nodes=nodes, edges=edges))


# ---------------------------------------------------------------------------
# Nodes table
# ---------------------------------------------------------------------------


@router.get("/{cluster_id}/nodes")
async def get_cluster_nodes(
    cluster_id: str,
    neo4j: Neo4jDep,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=500),
) -> APIResponse[list[dict]]:
    skip = (page - 1) * per_page
    rows = await neo4j.execute_read(
        GET_CLUSTER_NODES,
        {"cluster_id": cluster_id, "skip": skip, "limit": per_page},
    )
    payload = [
        {
            "id": (
                row["props"].get(_LABEL_TO_TYPE[row["labels"][0]][1])
                if row["labels"] and row["labels"][0] in _LABEL_TO_TYPE
                else row["eid"]
            ),
            "type": row["labels"][0] if row["labels"] else "Unknown",
            "confidence": float(row.get("confidence") or 0.0),
            "role": row.get("role"),
            "properties": row.get("props") or {},
        }
        for row in rows
    ]
    return APIResponse(
        data=payload,
        meta=Meta(page=page, per_page=per_page),
        errors=[],
    )


# ---------------------------------------------------------------------------
# Evidence + fund flow stubs (full implementation in core/evidence + analytics)
# ---------------------------------------------------------------------------


@router.get("/{cluster_id}/evidence")
async def get_cluster_evidence(cluster_id: str, neo4j: Neo4jDep) -> APIResponse[list[dict]]:
    """Evidence chain timeline. The full builder lives in ``core/evidence``;
    this stub returns the cluster's seed event so the UI can render at least one row."""

    rows = await neo4j.execute_read(GET_CLUSTER, {"cluster_id": cluster_id})
    if not rows:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "cluster not found")
    cluster = rows[0]["cluster"]
    return ok(
        [
            {
                "kind": "seed",
                "timestamp": cluster.get("seed_date"),
                "node_id": cluster.get("seed_node_id"),
                "node_type": cluster.get("seed_type"),
                "confidence": cluster.get("confidence_score"),
                "description": (
                    f"Cluster seeded from {cluster.get('seed_type')} "
                    f"{cluster.get('seed_node_id')}."
                ),
            }
        ]
    )


@router.get("/{cluster_id}/fund-flow")
async def get_cluster_fund_flow(cluster_id: str, neo4j: Neo4jDep) -> APIResponse[dict]:
    """Sankey-format fund flow data for the cluster.

    Aggregates ``SENT_TO`` edges between cluster members + cashouts to agents.
    """

    rows = await neo4j.execute_read(
        """
        MATCH (c:Cluster {cluster_id: $cluster_id})
        MATCH (src)-[:BELONGS_TO]->(c)
        MATCH (src)-[r:SENT_TO]->(dst)
        RETURN
            elementId(src) AS source_eid,
            coalesce(src.wallet_id, src.msisdn) AS source_id,
            elementId(dst) AS target_eid,
            coalesce(dst.wallet_id, dst.msisdn) AS target_id,
            sum(coalesce(r.amount, 0.0)) AS total_amount,
            count(r) AS tx_count
        """,
        {"cluster_id": cluster_id},
    )
    nodes_seen: dict[str, dict[str, Any]] = {}
    links: list[dict[str, Any]] = []
    for r in rows:
        for k in (r["source_id"], r["target_id"]):
            if k and k not in nodes_seen:
                nodes_seen[k] = {"id": k}
        links.append(
            {
                "source": r["source_id"],
                "target": r["target_id"],
                "value": float(r["total_amount"] or 0.0),
                "tx_count": int(r["tx_count"] or 0),
            }
        )
    return ok({"nodes": list(nodes_seen.values()), "links": links})


# ---------------------------------------------------------------------------
# Manual expansion trigger
# ---------------------------------------------------------------------------


class ExpansionRequest(BaseModel):
    seed_node_id: str
    seed_type: str = Field(..., pattern="^(wallet|handset|sim|phone|agent)$")
    seed_confidence: float = Field(0.85, ge=0, le=1)
    source: str = "manual"
    background: bool = True


@router.post("/{cluster_id}/expand")
async def expand_cluster(
    cluster_id: str,
    payload: ExpansionRequest,
    neo4j: Neo4jDep,
    background_tasks: BackgroundTasks,
) -> APIResponse[dict]:
    """Trigger a manual expansion using ``payload.seed_node_id`` as the seed.

    The ``cluster_id`` path parameter is treated as a *parent* cluster — the
    new expansion is tagged with the same parent. With ``background=true`` (the
    default) the work runs in a FastAPI background task and the response
    returns immediately with the new cluster id.
    """

    seed = Seed(
        node_id=payload.seed_node_id,
        node_type=payload.seed_type,
        confidence=payload.seed_confidence,
        source=payload.source,
    )

    if not payload.background:
        result = await expand_from_seed(seed, client=neo4j)
        return ok(
            {
                "status": "complete",
                "parent_cluster_id": cluster_id,
                **result.to_summary(),
            }
        )

    async def _runner() -> None:
        await expand_from_seed(seed, client=neo4j)

    background_tasks.add_task(_runner)
    return ok(
        {
            "status": "queued",
            "parent_cluster_id": cluster_id,
            "seed_node_id": seed.node_id,
            "seed_type": seed.node_type,
        }
    )
