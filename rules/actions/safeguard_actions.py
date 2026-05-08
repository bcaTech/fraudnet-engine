"""SafeGuard feature actions (Send-with-Care, Ask-Me-First).

These flip wallet-level flags that the MoMo BSS layer would observe and
inject the appropriate UI prompts before the customer can complete a
transaction.
"""

from __future__ import annotations

from core.graph.client import get_neo4j_client

from .registry import ActionContext, ActionRegistry, ActionResult


async def _set_flag(wallet_id: str, flag: str, value: bool) -> bool:
    client = get_neo4j_client()
    rows = await client.execute_write(
        f"""
        MATCH (w:Wallet {{wallet_id: $wallet_id}})
        SET w.{flag} = $value, w.{flag}_at = CASE WHEN $value THEN datetime() ELSE null END
        RETURN w.wallet_id AS wallet_id
        """,  # noqa: S608 — flag is from a hard-coded set below
        {"wallet_id": wallet_id, "value": value},
    )
    return bool(rows)


async def _apply_send_with_care(ctx: ActionContext) -> ActionResult:
    if not await _set_flag(ctx.target, "send_with_care", True):
        return ActionResult(ok=False, detail={"target": ctx.target}, error="wallet not found")
    return ActionResult(ok=True, detail={"target": ctx.target, "send_with_care": True})


async def _remove_send_with_care(ctx: ActionContext) -> ActionResult:
    if not await _set_flag(ctx.target, "send_with_care", False):
        return ActionResult(ok=False, detail={"target": ctx.target}, error="wallet not found")
    return ActionResult(ok=True, detail={"target": ctx.target, "send_with_care": False})


async def _apply_ask_me_first(ctx: ActionContext) -> ActionResult:
    if not await _set_flag(ctx.target, "ask_me_first", True):
        return ActionResult(ok=False, detail={"target": ctx.target}, error="wallet not found")
    return ActionResult(ok=True, detail={"target": ctx.target, "ask_me_first": True})


async def _remove_ask_me_first(ctx: ActionContext) -> ActionResult:
    if not await _set_flag(ctx.target, "ask_me_first", False):
        return ActionResult(ok=False, detail={"target": ctx.target}, error="wallet not found")
    return ActionResult(ok=True, detail={"target": ctx.target, "ask_me_first": False})


def register_all(reg: ActionRegistry) -> None:
    reg.register(
        "apply_send_with_care",
        _apply_send_with_care,
        description="Inject the SafeGuard Send-with-Care prompt for this wallet.",
    )
    reg.register(
        "remove_send_with_care",
        _remove_send_with_care,
        description="Disable Send-with-Care.",
    )
    reg.register(
        "apply_ask_me_first",
        _apply_ask_me_first,
        description="Require Ask-Me-First confirmation on transfers.",
    )
    reg.register(
        "remove_ask_me_first",
        _remove_ask_me_first,
        description="Disable Ask-Me-First.",
    )
