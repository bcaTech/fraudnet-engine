"""Agent-targeted action handlers."""

from __future__ import annotations

from typing import Any

from core.graph.client import get_neo4j_client

from .registry import ActionContext, ActionRegistry, ActionResult


async def _suspend(ctx: ActionContext) -> ActionResult:
    client = get_neo4j_client()
    rows = await client.execute_write(
        """
        MATCH (a:Agent {agent_id: $agent_id})
        SET a.suspended = true,
            a.suspension_date = datetime(),
            a.suspension_reason = $reason
        RETURN a.agent_id AS agent_id, a.suspended AS suspended
        """,
        {"agent_id": ctx.target, "reason": ctx.params.get("reason", "rule_triggered")},
    )
    if not rows:
        return ActionResult(ok=False, detail={"target": ctx.target}, error="agent not found")
    return ActionResult(ok=True, detail={"target": ctx.target, "suspended": True})


async def _unsuspend(ctx: ActionContext) -> ActionResult:
    client = get_neo4j_client()
    rows = await client.execute_write(
        """
        MATCH (a:Agent {agent_id: $agent_id})
        SET a.suspended = false
        REMOVE a.suspension_date, a.suspension_reason
        RETURN a.agent_id AS agent_id
        """,
        {"agent_id": ctx.target},
    )
    if not rows:
        return ActionResult(ok=False, detail={"target": ctx.target}, error="agent not found")
    return ActionResult(ok=True, detail={"target": ctx.target, "suspended": False})


async def _downgrade_float(ctx: ActionContext) -> ActionResult:
    new_float = float(ctx.params.get("float", 1000.0))
    client = get_neo4j_client()
    rows = await client.execute_write(
        """
        MATCH (a:Agent {agent_id: $agent_id})
        SET a.float_avg_previous = a.float_avg,
            a.float_avg = $new_float,
            a.float_downgraded_at = datetime()
        RETURN a.agent_id AS agent_id
        """,
        {"agent_id": ctx.target, "new_float": new_float},
    )
    if not rows:
        return ActionResult(ok=False, detail={"target": ctx.target}, error="agent not found")
    return ActionResult(ok=True, detail={"target": ctx.target, "float_avg": new_float})


async def _restore_float(ctx: ActionContext) -> ActionResult:
    client = get_neo4j_client()
    rows = await client.execute_write(
        """
        MATCH (a:Agent {agent_id: $agent_id})
        WHERE a.float_avg_previous IS NOT NULL
        SET a.float_avg = a.float_avg_previous
        REMOVE a.float_avg_previous, a.float_downgraded_at
        RETURN a.agent_id AS agent_id, a.float_avg AS float_avg
        """,
        {"agent_id": ctx.target},
    )
    if not rows:
        return ActionResult(ok=False, detail={"target": ctx.target}, error="no prior float")
    return ActionResult(
        ok=True, detail={"target": ctx.target, "float_avg": float(rows[0]["float_avg"])}
    )


async def _agent_warning(ctx: ActionContext) -> ActionResult:
    client = get_neo4j_client()
    rows = await client.execute_write(
        """
        MATCH (a:Agent {agent_id: $agent_id})
        SET a.warnings_count = coalesce(a.warnings_count, 0) + 1,
            a.last_warning_at = datetime()
        RETURN a.warnings_count AS warnings_count
        """,
        {"agent_id": ctx.target},
    )
    if not rows:
        return ActionResult(ok=False, detail={"target": ctx.target}, error="agent not found")
    return ActionResult(
        ok=True,
        detail={"target": ctx.target, "warnings_count": int(rows[0]["warnings_count"])},
    )


def register_all(reg: ActionRegistry) -> None:
    reg.register("suspend_agent", _suspend, description="Suspend a MoMo agent.")
    reg.register("unsuspend_agent", _unsuspend, description="Reactivate a suspended agent.")
    reg.register(
        "downgrade_agent_float",
        _downgrade_float,
        description="Cap an agent's float to params.float.",
        params_schema={"float": {"type": "number", "default": 1000}},
    )
    reg.register("restore_agent_float", _restore_float, description="Restore prior float level.")
    reg.register("issue_agent_warning", _agent_warning, description="Increment agent warning count.")
