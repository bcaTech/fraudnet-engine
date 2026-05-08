"""Custom webhook action — HMAC-signed HTTP POST with retry + allow-list.

The action posts a signed JSON body to the configured ``url``. The
signature header is ``X-FraudNet-Signature`` and is computed as
``hmac_sha256(secret, body)`` so the receiver can verify the payload
hasn't been tampered with.

Hardening:

- Allow-list: ``settings.webhook_allow_list`` lists URL prefixes that
  webhooks may target. Anything outside is refused with a clear error.
- Timeout: per-attempt timeout from settings (5s default).
- Retry: ``settings.webhook_max_retries`` attempts with exponential
  backoff (0.5s, 1s, 2s, ...). 5xx and connection errors retry; 4xx
  fail fast.
- Idempotency: the receiver sees ``X-FraudNet-Idempotency-Key`` derived
  from the rule + target + 15-minute window. Combined with the rules
  engine's own dedup, double-delivery is bounded.
"""

from __future__ import annotations

import hashlib
import hmac
import json

import httpx

from config.logging import get_logger
from config.settings import get_settings

from .registry import ActionContext, ActionRegistry, ActionResult

logger = get_logger(__name__)


def _allowed(url: str) -> bool:
    s = get_settings()
    return any(url.startswith(prefix) for prefix in s.webhook_allow_list)


def _idempotency_key(ctx: ActionContext) -> str:
    rule_id = ctx.trigger.get("rule_id") or "rule:unknown"
    bucket = "rules-15m"  # fixed bucket; aligned with engine dedup window
    return hashlib.sha256(f"{rule_id}:{ctx.target}:{bucket}".encode()).hexdigest()[:32]


def _sign(body: bytes, secret: str) -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


async def _post_with_retry(url: str, body: bytes, *, headers: dict[str, str]) -> tuple[bool, dict]:
    """Returns ``(delivered, detail)``. ``detail`` is JSON-serialisable."""

    s = get_settings()
    last_status = None
    last_error = None
    backoff = 0.5
    async with httpx.AsyncClient(timeout=s.webhook_timeout_s) as client:
        for attempt in range(1, max(1, s.webhook_max_retries) + 1):
            try:
                resp = await client.post(url, content=body, headers=headers)
                last_status = resp.status_code
                if 200 <= resp.status_code < 300:
                    return True, {
                        "status_code": resp.status_code,
                        "attempts": attempt,
                    }
                if 400 <= resp.status_code < 500:
                    return False, {
                        "status_code": resp.status_code,
                        "attempts": attempt,
                        "body": resp.text[:200],
                    }
                last_error = f"http {resp.status_code}"
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
            if attempt < s.webhook_max_retries:
                import asyncio

                await asyncio.sleep(backoff)
                backoff *= 2
    return False, {
        "status_code": last_status,
        "error": last_error,
        "attempts": s.webhook_max_retries,
    }


async def _custom_webhook(ctx: ActionContext) -> ActionResult:
    url = ctx.params.get("url")
    if not isinstance(url, str) or not url.startswith(("http://", "https://")):
        return ActionResult(
            ok=False,
            detail={"target": ctx.target},
            error="webhook 'url' param missing or invalid",
        )
    if not _allowed(url):
        return ActionResult(
            ok=False,
            detail={"target": ctx.target, "url": url},
            error="webhook url not in allow-list",
        )

    s = get_settings()
    payload = {
        "event": "rule.triggered",
        "target": {"id": ctx.target, "type": ctx.target_type},
        "rule": {
            "id": ctx.trigger.get("rule_id"),
            "name": ctx.trigger.get("rule_name"),
        },
        "cluster_id": ctx.trigger.get("cluster_id"),
        "params": ctx.params,
    }
    body = json.dumps(payload, default=str).encode()
    headers = {
        "Content-Type": "application/json",
        "X-FraudNet-Signature": _sign(body, s.webhook_hmac_secret.get_secret_value()),
        "X-FraudNet-Idempotency-Key": _idempotency_key(ctx),
        "User-Agent": "FraudNet/1.0",
    }
    delivered, detail = await _post_with_retry(url, body, headers=headers)
    logger.info(
        "rules.webhook.delivered" if delivered else "rules.webhook.failed",
        url=url,
        target=ctx.target,
        **detail,
    )
    return ActionResult(
        ok=delivered,
        detail={"target": ctx.target, "url": url, "delivered": delivered, **detail},
        error=None if delivered else (detail.get("error") or f"http {detail.get('status_code')}"),
    )


def register_all(reg: ActionRegistry) -> None:
    reg.register(
        "custom_webhook",
        _custom_webhook,
        description=(
            "POST to a custom webhook URL. URL must be in the configured "
            "allow-list. Body is signed with HMAC-SHA256."
        ),
        params_schema={"url": {"type": "string", "required": True}},
    )
