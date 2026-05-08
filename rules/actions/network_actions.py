"""Cross-network and external-operator notification actions."""

from __future__ import annotations

from core.graph.client import get_neo4j_client

from .registry import ActionContext, ActionRegistry, ActionResult


async def _block_cross_network(ctx: ActionContext) -> ActionResult:
    client = get_neo4j_client()
    rows = await client.execute_write(
        """
        MATCH (w:Wallet {wallet_id: $wallet_id})
        SET w.cross_network_blocked = true,
            w.cross_network_blocked_at = datetime()
        RETURN w.wallet_id AS wallet_id
        """,
        {"wallet_id": ctx.target},
    )
    if not rows:
        return ActionResult(ok=False, detail={"target": ctx.target}, error="wallet not found")
    return ActionResult(ok=True, detail={"target": ctx.target, "cross_network_blocked": True})


async def _unblock_cross_network(ctx: ActionContext) -> ActionResult:
    client = get_neo4j_client()
    rows = await client.execute_write(
        """
        MATCH (w:Wallet {wallet_id: $wallet_id})
        SET w.cross_network_blocked = false
        REMOVE w.cross_network_blocked_at
        RETURN w.wallet_id AS wallet_id
        """,
        {"wallet_id": ctx.target},
    )
    if not rows:
        return ActionResult(ok=False, detail={"target": ctx.target}, error="wallet not found")
    return ActionResult(ok=True, detail={"target": ctx.target, "cross_network_blocked": False})


async def _notify_external_operator(ctx: ActionContext) -> ActionResult:
    """Stub: real implementation enqueues an outbound shared-flag for the
    operator integration pipeline. For now we just record the intent on
    the graph node so the audit trail shows the rule fired."""

    client = get_neo4j_client()
    operator = ctx.params.get("operator", "all_connected")
    rows = await client.execute_write(
        """
        MATCH (w:Wallet {wallet_id: $wallet_id})
        SET w.external_notify_pending = true,
            w.external_notify_target = $operator,
            w.external_notify_at = datetime()
        RETURN w.wallet_id AS wallet_id
        """,
        {"wallet_id": ctx.target, "operator": operator},
    )
    if not rows:
        return ActionResult(ok=False, detail={"target": ctx.target}, error="wallet not found")
    return ActionResult(
        ok=True, detail={"target": ctx.target, "operator": operator, "queued": True}
    )


def register_all(reg: ActionRegistry) -> None:
    reg.register(
        "block_cross_network",
        _block_cross_network,
        description="Block this wallet from cross-network transfers.",
    )
    reg.register("unblock_cross_network", _unblock_cross_network, description="Re-enable cross-network.")
    reg.register(
        "notify_external_operator",
        _notify_external_operator,
        description="Queue an outbound flag share to a connected operator.",
        params_schema={"operator": {"type": "string", "default": "all_connected"}},
    )
