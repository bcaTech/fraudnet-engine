"""Wallet-targeted action handlers."""

from __future__ import annotations

from typing import Any

from core.graph.client import get_neo4j_client

from .registry import ActionContext, ActionRegistry, ActionResult


async def _set_wallet(wallet_id: str, props: dict[str, Any]) -> dict[str, Any] | None:
    """Apply ``props`` to a wallet via a single Cypher write. Returns the
    wallet's post-write properties, or None if no wallet matched."""

    client = get_neo4j_client()
    set_clause = ", ".join(f"w.{k} = ${k}" for k in props)
    cypher = f"""
        MATCH (w:Wallet {{wallet_id: $wallet_id}})
        SET {set_clause}
        RETURN w.wallet_id AS wallet_id, w.status AS status, properties(w) AS props
    """  # noqa: S608 — set_clause is built from a known dict of identifiers
    rows = await client.execute_write(cypher, {"wallet_id": wallet_id, **props})
    if not rows:
        return None
    return {"wallet_id": rows[0]["wallet_id"], "status": rows[0]["status"]}


async def _freeze(ctx: ActionContext) -> ActionResult:
    res = await _set_wallet(
        ctx.target,
        {"status": "frozen", "freeze_date": "_NEO4J_NOW_"},
    )
    # The `_NEO4J_NOW_` placeholder above doesn't actually work since we're
    # passing as a parameter — switch to a dedicated freeze cypher.
    client = get_neo4j_client()
    rows = await client.execute_write(
        """
        MATCH (w:Wallet {wallet_id: $wallet_id})
        SET w.status = 'frozen', w.freeze_date = datetime()
        RETURN w.wallet_id AS wallet_id, w.status AS status
        """,
        {"wallet_id": ctx.target},
    )
    if not rows:
        return ActionResult(ok=False, detail={"target": ctx.target}, error="wallet not found")
    return ActionResult(ok=True, detail={"target": ctx.target, "status": "frozen"})


async def _unfreeze(ctx: ActionContext) -> ActionResult:
    client = get_neo4j_client()
    rows = await client.execute_write(
        """
        MATCH (w:Wallet {wallet_id: $wallet_id})
        SET w.status = CASE
                WHEN coalesce(w.cluster_id, '') = '' THEN 'active'
                ELSE 'flagged'
            END,
            w.freeze_date = null
        RETURN w.wallet_id AS wallet_id, w.status AS status
        """,
        {"wallet_id": ctx.target},
    )
    if not rows:
        return ActionResult(ok=False, detail={"target": ctx.target}, error="wallet not found")
    return ActionResult(ok=True, detail={"target": ctx.target, "status": rows[0]["status"]})


async def _reduce_limit(ctx: ActionContext) -> ActionResult:
    new_limit = float(ctx.params.get("limit", 100.0))
    res = await _set_wallet(ctx.target, {"transaction_limit": new_limit})
    if res is None:
        return ActionResult(ok=False, detail={"target": ctx.target}, error="wallet not found")
    return ActionResult(ok=True, detail={"target": ctx.target, "transaction_limit": new_limit})


async def _restore_limit(ctx: ActionContext) -> ActionResult:
    client = get_neo4j_client()
    rows = await client.execute_write(
        """
        MATCH (w:Wallet {wallet_id: $wallet_id})
        REMOVE w.transaction_limit
        RETURN w.wallet_id AS wallet_id
        """,
        {"wallet_id": ctx.target},
    )
    if not rows:
        return ActionResult(ok=False, detail={"target": ctx.target}, error="wallet not found")
    return ActionResult(ok=True, detail={"target": ctx.target, "transaction_limit": "cleared"})


async def _restrict_cashout(ctx: ActionContext) -> ActionResult:
    res = await _set_wallet(ctx.target, {"cashout_restricted": True})
    if res is None:
        return ActionResult(ok=False, detail={"target": ctx.target}, error="wallet not found")
    return ActionResult(ok=True, detail={"target": ctx.target, "cashout_restricted": True})


async def _unrestrict_cashout(ctx: ActionContext) -> ActionResult:
    res = await _set_wallet(ctx.target, {"cashout_restricted": False})
    if res is None:
        return ActionResult(ok=False, detail={"target": ctx.target}, error="wallet not found")
    return ActionResult(ok=True, detail={"target": ctx.target, "cashout_restricted": False})


async def _force_kyc(ctx: ActionContext) -> ActionResult:
    res = await _set_wallet(ctx.target, {"kyc_pending_reverification": True})
    if res is None:
        return ActionResult(ok=False, detail={"target": ctx.target}, error="wallet not found")
    return ActionResult(ok=True, detail={"target": ctx.target, "kyc_pending_reverification": True})


async def _customer_warning(ctx: ActionContext) -> ActionResult:
    client = get_neo4j_client()
    rows = await client.execute_write(
        """
        MATCH (w:Wallet {wallet_id: $wallet_id})
        SET w.warnings_count = coalesce(w.warnings_count, 0) + 1,
            w.last_warning_at = datetime()
        RETURN w.warnings_count AS warnings_count
        """,
        {"wallet_id": ctx.target},
    )
    if not rows:
        return ActionResult(ok=False, detail={"target": ctx.target}, error="wallet not found")
    return ActionResult(
        ok=True, detail={"target": ctx.target, "warnings_count": int(rows[0]["warnings_count"])}
    )


def register_all(reg: ActionRegistry) -> None:
    reg.register("freeze_wallet", _freeze, description="Freeze a wallet (sets status='frozen').")
    reg.register("unfreeze_wallet", _unfreeze, description="Unfreeze a wallet.")
    reg.register(
        "reduce_transaction_limit",
        _reduce_limit,
        description="Cap transactions at params.limit.",
        params_schema={"limit": {"type": "number", "default": 100}},
    )
    reg.register("restore_transaction_limit", _restore_limit, description="Clear the limit.")
    reg.register("restrict_cashout", _restrict_cashout, description="Block cashout for the wallet.")
    reg.register("unrestrict_cashout", _unrestrict_cashout, description="Re-enable cashout.")
    reg.register(
        "force_kyc_reverification",
        _force_kyc,
        description="Mark wallet pending KYC re-verification.",
    )
    reg.register("issue_customer_warning", _customer_warning, description="Increment warning count.")
