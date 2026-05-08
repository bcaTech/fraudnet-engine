"""Producer-side helper for broadcasting WS events.

Routes, Celery tasks, and Kafka consumers call :func:`publish` to send an
event to a Redis pub/sub channel. The :class:`~api.websocket.bridge.RedisBridge`
running inside the API process picks the message up and fans it out to
connected WebSocket clients.

Channels
--------
- ``fraudnet.ws.alerts`` — alert lifecycle events
- ``fraudnet.ws.cluster_updates`` — cluster state changes
- ``fraudnet.ws.metrics`` — periodic dashboard snapshots

Events use the envelope ``{event, data, timestamp}`` so a single feed can
multiplex multiple event types.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import redis.asyncio as redis_async

from config.logging import get_logger
from config.settings import get_settings

logger = get_logger(__name__)


# Channel constants — keep in sync with bridge.SUBSCRIBED_CHANNELS and feeds.py.
CH_ALERTS: str = "fraudnet.ws.alerts"
CH_CLUSTER_UPDATES: str = "fraudnet.ws.cluster_updates"
CH_METRICS: str = "fraudnet.ws.metrics"


_client: redis_async.Redis | None = None


async def _get_client() -> redis_async.Redis:
    global _client
    if _client is None:
        settings = get_settings()
        _client = redis_async.from_url(settings.redis_url, decode_responses=True)
    return _client


async def close_client() -> None:
    """Tear the publisher's Redis client down at shutdown."""

    global _client
    if _client is not None:
        try:
            await _client.aclose()
        except Exception:  # noqa: BLE001
            pass
        _client = None


def _envelope(event: str, data: Any) -> dict[str, Any]:
    return {
        "event": event,
        "data": data,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


async def publish(channel: str, event: str, data: Any) -> None:
    """Publish a JSON-encoded event to ``channel``. Failures are logged but
    never raised — the broadcast is best-effort."""

    payload = _envelope(event, data)
    try:
        client = await _get_client()
        await client.publish(channel, json.dumps(payload, default=str))
    except Exception as exc:  # noqa: BLE001 — broadcast is best-effort
        logger.warning(
            "ws.publish.failed", channel=channel, event=event, error=str(exc)
        )
