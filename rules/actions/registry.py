"""Action registry and dispatcher for the rules engine.

Every action type implements :class:`ActionHandler`. The registry is
populated at import time by the sibling action modules. To add a new
action type, drop a subclass of :class:`ActionHandler` into one of those
modules and register it via :func:`register`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from config.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ActionContext:
    """Everything an action handler needs to do its work.

    ``target`` carries the entity the action operates on (wallet_id,
    agent_id, etc.). ``params`` are the action-specific knobs from the
    rule definition. ``trigger`` carries the rule + node context so the
    handler can write meaningful audit/metadata.
    """

    target: str
    target_type: str  # 'wallet' | 'agent' | 'sim' | 'handset' | 'cluster'
    params: dict[str, Any]
    trigger: dict[str, Any]


@dataclass
class ActionResult:
    ok: bool
    detail: dict[str, Any]
    error: str | None = None


HandlerFn = Callable[[ActionContext], Awaitable[ActionResult]]


class ActionRegistry:
    """Type → handler dispatch with a tiny inventory API for the
    ``GET /api/rules/actions/registry`` endpoint."""

    def __init__(self) -> None:
        self._handlers: dict[str, HandlerFn] = {}
        self._descriptions: dict[str, str] = {}
        self._param_schemas: dict[str, dict[str, Any]] = {}

    def register(
        self,
        action_type: str,
        handler: HandlerFn,
        *,
        description: str = "",
        params_schema: dict[str, Any] | None = None,
    ) -> None:
        if action_type in self._handlers:
            logger.warning("rules.action.duplicate", type=action_type)
        self._handlers[action_type] = handler
        self._descriptions[action_type] = description
        self._param_schemas[action_type] = params_schema or {}

    def types(self) -> list[str]:
        return sorted(self._handlers.keys())

    def describe(self) -> list[dict[str, Any]]:
        return [
            {
                "type": t,
                "description": self._descriptions.get(t, ""),
                "params_schema": self._param_schemas.get(t, {}),
            }
            for t in self.types()
        ]

    async def execute(self, action_type: str, ctx: ActionContext) -> ActionResult:
        handler = self._handlers.get(action_type)
        if handler is None:
            return ActionResult(
                ok=False,
                detail={"action_type": action_type},
                error=f"unknown action type: {action_type}",
            )
        try:
            return await handler(ctx)
        except Exception as exc:  # noqa: BLE001 — never crash the engine on a bad handler
            logger.error("rules.action.error", type=action_type, error=str(exc))
            return ActionResult(ok=False, detail={"action_type": action_type}, error=str(exc))


# Module-level singleton.
_registry: ActionRegistry | None = None


def get_registry() -> ActionRegistry:
    global _registry
    if _registry is None:
        _registry = ActionRegistry()
        _populate(_registry)
    return _registry


def _populate(reg: ActionRegistry) -> None:
    """Import handler modules so they self-register with ``reg``."""

    # Imported here (not at module top) so the registry exists before the
    # handler modules call back into it. Each module calls register_all().
    from . import (
        agent_actions,
        escalation_actions,
        network_actions,
        safeguard_actions,
        wallet_actions,
        webhook_actions,
    )

    for mod in (
        wallet_actions,
        agent_actions,
        safeguard_actions,
        escalation_actions,
        network_actions,
        webhook_actions,
    ):
        mod.register_all(reg)
