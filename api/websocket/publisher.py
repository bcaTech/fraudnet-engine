"""Producer-side helper for broadcasting WS events.

Routes, Celery tasks, and Kafka consumers call :func:`publish` to send
an event. The :class:`~api.websocket.bridge.RedisBridge` running inside
the API process picks the message up and fans it out to connected
WebSocket clients.

Two transports
--------------

The three *core* channels — alerts, cluster_updates, metrics — use
**Redis Streams**. Streams are durable (we cap each at
:data:`STREAM_MAXLEN` entries via ``XADD ... MAXLEN ~``), so a client
that disconnects can resume by passing the last stream id it saw as
``?since=<id>`` on its next connect. Each broadcast carries its stream
id in the envelope under ``_stream_id`` so clients have something to
save.

The per-takedown channels (``fraudnet.ws.takedown:<id>``) and the
``rules`` / ``integration`` feeds stay on **pub/sub**. They're either
ephemeral or low-volume; backfill isn't load-bearing for them.

Events use the envelope ``{event, data, timestamp, _stream_id?}`` so
a single feed can multiplex multiple event types.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import redis.asyncio as redis_async

from config.logging import get_logger
from config.settings import get_settings

logger = get_logger(__name__)


# Channel / stream constants — keep in sync with bridge + feeds.py.
CH_ALERTS: str = "fraudnet.ws.alerts"
CH_CLUSTER_UPDATES: str = "fraudnet.ws.cluster_updates"
CH_METRICS: str = "fraudnet.ws.metrics"
CH_RULES: str = "fraudnet.ws.rules"
CH_INTEGRATION: str = "fraudnet.ws.integration"

# Per-takedown channels share this prefix; the bridge psubscribes to
# ``fraudnet.ws.takedown:*`` so any new takedown id is routed without
# a bridge restart.
CH_TAKEDOWN_PREFIX: str = "fraudnet.ws.takedown:"

# Channels that ride Redis Streams (with replay). Everything else uses
# pub/sub.
STREAM_CHANNELS: frozenset[str] = frozenset({CH_ALERTS, CH_CLUSTER_UPDATES, CH_METRICS})

# Each stream is capped to this many entries. ~ is the approximate-
# trim flag; Redis only trims when it's cheap, which is what we want.
STREAM_MAXLEN: int = 1000


def takedown_channel(takedown_id: str) -> str:
    return f"{CH_TAKEDOWN_PREFIX}{takedown_id}"


_client: redis_async.Redis | None = None


async def _get_client() -> redis_async.Redis:
    global _client
    if _client is None:
        settings = get_settings()
        _client = redis_async.from_url(settings.redis_url, decode_responses=True)  # type: ignore[no-untyped-call]
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
        "timestamp": datetime.now(UTC).isoformat(),
    }


async def publish(channel: str, event: str, data: Any) -> None:
    """Publish a JSON-encoded event to ``channel``. Stream-backed
    channels go through XADD; everything else uses PUBLISH. Failures
    are logged but never raised — the broadcast is best-effort."""

    payload = _envelope(event, data)
    body = json.dumps(payload, default=str)
    try:
        client = await _get_client()
        if channel in STREAM_CHANNELS:
            await client.xadd(
                channel,
                {"payload": body},
                maxlen=STREAM_MAXLEN,
                approximate=True,
            )
        else:
            await client.publish(channel, body)
    except Exception as exc:  # noqa: BLE001 — broadcast is best-effort
        logger.warning("ws.publish.failed", channel=channel, event_name=event, error=str(exc))


async def fetch_history(channel: str, *, since: str = "-", count: int = 200) -> list[dict[str, Any]]:
    """Replay entries from a stream-backed channel.

    ``since`` is a stream id (e.g. ``"1714000000-0"``); ``"-"`` means
    "from the beginning of the stream". Returns the entries newest-
    last, with ``_stream_id`` injected on each so clients can save the
    cursor.
    """

    if channel not in STREAM_CHANNELS:
        return []
    try:
        client = await _get_client()
        # XRANGE since +, capped to count.
        rows = await client.xrange(channel, min=since, max="+", count=count)
    except Exception as exc:  # noqa: BLE001
        logger.warning("ws.history.failed", channel=channel, since=since, error=str(exc))
        return []
    out: list[dict[str, Any]] = []
    for stream_id, fields in rows:
        raw = fields.get("payload") if isinstance(fields, dict) else None
        if not raw:
            continue
        try:
            envelope = json.loads(raw)
        except json.JSONDecodeError:
            continue
        envelope["_stream_id"] = stream_id
        out.append(envelope)
    return out
