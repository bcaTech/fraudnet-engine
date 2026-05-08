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
    """Enqueue outbound shared-flag rows for the connected operator(s).

    ``params.operator`` is either an operator id, the literal
    ``"all_connected"`` (default), or omitted. The action writes one
    :class:`db.models.SharedFlag` per matching connected operator with
    ``direction='outbound'`` and applies the operator's per-field
    masking rule. The integration delivery loop
    (``tasks.periodic.process_outbound_integration``) flushes those
    rows to the operator HTTP endpoints.
    """

    import hashlib
    import uuid

    from sqlalchemy import select

    from db.models import ExternalOperator, SharedFlag
    from db.session import get_async_session

    operator_param = ctx.params.get("operator", "all_connected")
    risk_score = float(ctx.params.get("risk_score", 0.85))

    client = get_neo4j_client()
    rows = await client.execute_write(
        """
        MATCH (w:Wallet {wallet_id: $wallet_id})
        SET w.external_notify_pending = true,
            w.external_notify_target = $operator,
            w.external_notify_at = datetime()
        RETURN w.wallet_id AS wallet_id, w.msisdn AS msisdn
        """,
        {"wallet_id": ctx.target, "operator": operator_param},
    )
    if not rows:
        return ActionResult(ok=False, detail={"target": ctx.target}, error="wallet not found")
    msisdn = rows[0].get("msisdn") or ctx.target

    flags_queued = 0
    flag_ids: list[str] = []
    async with get_async_session() as db:
        if operator_param == "all_connected":
            ops = (
                (await db.execute(select(ExternalOperator).where(ExternalOperator.status == "connected")))
                .scalars()
                .all()
            )
        else:
            single = (
                await db.execute(select(ExternalOperator).where(ExternalOperator.id == operator_param))
            ).scalar_one_or_none()
            ops = [single] if single is not None else []
        for op in ops:
            mode = (op.masking_rules or {}).get("msisdn", "hash")
            if mode == "clear":
                masked = msisdn
            elif mode == "partial" and len(msisdn) > 4:
                masked = f"{msisdn[:2]}***{msisdn[-2:]}"
            else:
                masked = hashlib.sha256(msisdn.encode()).hexdigest()[:32]
            flag = SharedFlag(
                id=f"flag-{uuid.uuid4().hex[:12]}",
                direction="outbound",
                operator_id=op.id,
                identifier_type="msisdn",
                identifier_masked=masked,
                identifier_hash=hashlib.sha256(msisdn.encode()).hexdigest(),
                risk_score=risk_score,
                context=(
                    f"Rule {ctx.trigger.get('rule_id') or '?'} flagged "
                    f"{ctx.target} for cross-network notification."
                ),
            )
            db.add(flag)
            flag_ids.append(flag.id)
            flags_queued += 1
        if flags_queued:
            await db.commit()

    return ActionResult(
        ok=True,
        detail={
            "target": ctx.target,
            "operator": operator_param,
            "flags_queued": flags_queued,
            "flag_ids": flag_ids,
        },
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
