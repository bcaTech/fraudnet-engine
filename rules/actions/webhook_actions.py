"""Custom webhook action.

Lets analysts drive arbitrary external integrations from a rule. Real
implementation does an HTTP POST to ``params.url`` with an HMAC-signed
JSON body. For now this is a recorded-intent stub — production hardening
(timeouts, retries, signature scheme, allow-list) is deferred until the
external-integration pipeline lands.
"""

from __future__ import annotations

from .registry import ActionContext, ActionRegistry, ActionResult


async def _custom_webhook(ctx: ActionContext) -> ActionResult:
    url = ctx.params.get("url")
    if not isinstance(url, str) or not url.startswith(("http://", "https://")):
        return ActionResult(
            ok=False,
            detail={"target": ctx.target},
            error="webhook 'url' param missing or invalid",
        )
    # Stub: would post here.
    return ActionResult(
        ok=True,
        detail={
            "target": ctx.target,
            "url": url,
            "delivered": False,
            "note": "stub — webhook delivery not yet wired",
        },
    )


def register_all(reg: ActionRegistry) -> None:
    reg.register(
        "custom_webhook",
        _custom_webhook,
        description="POST to a custom webhook URL.",
        params_schema={"url": {"type": "string", "required": True}},
    )
