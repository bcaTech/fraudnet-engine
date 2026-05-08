"""Node lookup, search, and per-node mutations.

These endpoints are the workhorse for the NOC's node-detail drawer:
search by any identifier, fetch a typed detail row, and execute the
analyst-grade actions (freeze, flag, watchlist).
"""

from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from pydantic import BaseModel, Field

from api.auth.rbac import ROLE_INVESTIGATOR, require_role
from api.dependencies import Neo4jDep
from api.schemas import APIResponse, ok
from api.websocket.publisher import CH_CLUSTER_UPDATES, publish
from core.graph.queries import (
    FLAG_NODE,
    FREEZE_WALLET,
    GET_AGENT,
    GET_HANDSET,
    GET_NODE_CONNECTIONS,
    GET_PHONE,
    GET_SIM,
    GET_WALLET,
    SEARCH_NODES,
    UNFREEZE_WALLET,
)

router = APIRouter(prefix="/api/nodes", tags=["nodes"])


# (label, key) lookup — keep aligned with core.mesh.seed.NODE_TYPE_LOOKUP.
NodeType = Literal["wallet", "handset", "sim", "phone", "agent"]
_TYPE_LOOKUP: dict[NodeType, tuple[str, str]] = {
    "wallet": ("Wallet", "wallet_id"),
    "handset": ("Handset", "imei"),
    "sim": ("SIM", "imsi"),
    "phone": ("PhoneNumber", "msisdn"),
    "agent": ("Agent", "agent_id"),
}


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------


@router.get("/search")
async def search_nodes(
    neo4j: Neo4jDep,
    q: str = Query(..., min_length=2, max_length=80, description="Free-text fragment"),
    limit: int = Query(40, ge=1, le=200),
) -> APIResponse[list[dict[str, Any]]]:
    """Substring search across wallet ids, MSISDNs, IMEIs, IMSIs, agent ids."""

    rows = await neo4j.execute_read(
        SEARCH_NODES,
        {"q": q, "limit": limit, "branch_limit": max(10, limit // 2)},
    )
    payload = [
        {
            "type": r["type"],
            "id": r["id"],
            "label": (r.get("label") or "").strip() or r["id"],
            "subtitle": r.get("subtitle"),
            "risk_score": float(r.get("risk_score") or 0.0),
            "status": r.get("status"),
            "cluster_id": r.get("cluster_id"),
        }
        for r in rows
    ]
    return ok(payload)


# ---------------------------------------------------------------------------
# Detail
# ---------------------------------------------------------------------------


@router.get("/{node_type}/{node_id}")
async def get_node(
    node_type: NodeType,
    node_id: str = Path(..., min_length=1),
    neo4j: Neo4jDep = ...,  # type: ignore[assignment]
) -> APIResponse[dict[str, Any]]:
    """Type-aware node detail."""

    if node_type == "wallet":
        rows = await neo4j.execute_read(GET_WALLET, {"wallet_id": node_id})
        if not rows:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "wallet not found")
        return ok({**(rows[0]["wallet"] or {}), "cluster_id": rows[0].get("cluster_id")})

    if node_type == "handset":
        rows = await neo4j.execute_read(GET_HANDSET, {"imei": node_id})
        if not rows:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "handset not found")
        return ok(rows[0]["handset"] or {})

    if node_type == "sim":
        rows = await neo4j.execute_read(GET_SIM, {"imsi": node_id})
        if not rows:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "sim not found")
        return ok(rows[0]["sim"] or {})

    if node_type == "agent":
        rows = await neo4j.execute_read(GET_AGENT, {"agent_id": node_id})
        if not rows:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
        return ok(
            {
                **(rows[0]["agent"] or {}),
                "linked_clusters": rows[0].get("linked_clusters") or [],
            }
        )

    if node_type == "phone":
        rows = await neo4j.execute_read(GET_PHONE, {"msisdn": node_id})
        if not rows:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "phone not found")
        return ok(
            {
                **(rows[0]["phone"] or {}),
                "wallet_ids": rows[0].get("wallet_ids") or [],
            }
        )

    raise HTTPException(status.HTTP_400_BAD_REQUEST, f"unsupported node type: {node_type}")


@router.get("/{node_type}/{node_id}/connections")
async def get_node_connections(
    node_type: NodeType,
    node_id: str,
    neo4j: Neo4jDep,
    limit: int = Query(50, ge=1, le=500),
) -> APIResponse[list[dict[str, Any]]]:
    """All edges from this node sorted by strength desc."""

    label, key = _TYPE_LOOKUP[node_type]
    rows = await neo4j.execute_read(
        GET_NODE_CONNECTIONS,
        {"label": label, "key": key, "id": node_id, "limit": limit},
    )
    payload = [
        {
            "type": r["rel_type"],
            "strength": float(r.get("strength") or 0.0),
            "target_labels": r.get("target_labels") or [],
            "target_properties": r.get("target_props") or {},
            "rel_properties": r.get("rel_props") or {},
        }
        for r in rows
    ]
    return ok(payload)


# ---------------------------------------------------------------------------
# Mutations: freeze / unfreeze / flag / watchlist
# ---------------------------------------------------------------------------


class FlagRequest(BaseModel):
    reason: str = Field(..., min_length=1, max_length=200)


@router.post(
    "/wallet/{wallet_id}/freeze",
    dependencies=[Depends(require_role(ROLE_INVESTIGATOR))],
)
async def freeze_wallet(wallet_id: str, neo4j: Neo4jDep) -> APIResponse[dict[str, Any]]:
    rows = await neo4j.execute_write(FREEZE_WALLET, {"wallet_id": wallet_id})
    if not rows:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "wallet not found")
    wallet = rows[0]["wallet"] or {}
    await publish(
        CH_CLUSTER_UPDATES,
        "wallet.frozen",
        {"wallet_id": wallet_id, "cluster_id": wallet.get("cluster_id"), "wallet": wallet},
    )
    return ok(wallet)


@router.post(
    "/wallet/{wallet_id}/unfreeze",
    dependencies=[Depends(require_role(ROLE_INVESTIGATOR))],
)
async def unfreeze_wallet(wallet_id: str, neo4j: Neo4jDep) -> APIResponse[dict[str, Any]]:
    rows = await neo4j.execute_write(UNFREEZE_WALLET, {"wallet_id": wallet_id})
    if not rows:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "wallet not found")
    wallet = rows[0]["wallet"] or {}
    await publish(
        CH_CLUSTER_UPDATES,
        "wallet.unfrozen",
        {"wallet_id": wallet_id, "cluster_id": wallet.get("cluster_id"), "wallet": wallet},
    )
    return ok(wallet)


@router.post(
    "/{node_type}/{node_id}/flag",
    dependencies=[Depends(require_role(ROLE_INVESTIGATOR))],
)
async def flag_node(
    node_type: NodeType,
    node_id: str,
    payload: FlagRequest,
    neo4j: Neo4jDep,
) -> APIResponse[dict[str, Any]]:
    label, key = _TYPE_LOOKUP[node_type]
    rows = await neo4j.execute_write(
        FLAG_NODE,
        {"label": label, "key": key, "id": node_id, "reason": payload.reason},
    )
    if not rows:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"{node_type} not found")
    return ok(
        {
            "labels": rows[0].get("labels") or [],
            "properties": rows[0].get("props") or {},
        }
    )


@router.post(
    "/{node_type}/{node_id}/watchlist",
    dependencies=[Depends(require_role(ROLE_INVESTIGATOR))],
)
async def add_to_watchlist(
    node_type: NodeType,
    node_id: str,
    neo4j: Neo4jDep,
) -> APIResponse[dict[str, Any]]:
    """Mark a node as on the analyst watch-list. Recorded as a graph property
    so existing read paths can filter on ``n.on_watchlist``."""

    label, key = _TYPE_LOOKUP[node_type]
    rows = await neo4j.execute_write(
        """
        MATCH (n)
        WHERE any(l IN labels(n) WHERE l = $label) AND n[$key] = $id
        SET n.on_watchlist = true,
            n.watchlist_added = datetime()
        RETURN labels(n) AS labels, properties(n) AS props
        """,
        {"label": label, "key": key, "id": node_id},
    )
    if not rows:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"{node_type} not found")
    return ok(
        {
            "labels": rows[0].get("labels") or [],
            "properties": rows[0].get("props") or {},
        }
    )
