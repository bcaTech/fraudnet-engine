"""Redis → WebSocket broadcast bridge.

Two cooperating loops fan Redis events out to local WebSocket
connections via the shared :class:`~api.websocket.manager.ConnectionManager`:

- **Streams loop** — XREAD BLOCK on the three core stream channels
  (alerts, cluster_updates, metrics). Each entry's stream id is
  attached to the broadcast envelope as ``_stream_id`` so clients can
  resume on reconnect via ``?since=<stream_id>``.
- **Pub/sub loop** — direct subscribe on the lower-volume channels
  (rules, integration) plus pattern subscribe for the dynamic
  per-takedown channels. No backfill on these — they're either
  ephemeral or low-value to replay.

Decoupling producers (route handlers, Celery tasks, Kafka consumers)
from consumers (WS clients) means a publish from any API replica
reaches every connected client regardless of which replica it landed
on, since both replicas read the same Redis.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Iterable
from typing import Any

import redis.asyncio as redis_async

from config.logging import get_logger
from config.settings import get_settings

from .manager import ConnectionManager
from .publisher import (
    CH_ALERTS,
    CH_CLUSTER_UPDATES,
    CH_INTEGRATION,
    CH_METRICS,
    CH_RULES,
    CH_TAKEDOWN_PREFIX,
)

logger = get_logger(__name__)


# Pub/sub channels (no replay use case).
PUBSUB_CHANNELS: tuple[str, ...] = (CH_RULES, CH_INTEGRATION)
# Pattern subs — dynamic per-takedown channels.
PUBSUB_PATTERNS: tuple[str, ...] = (f"{CH_TAKEDOWN_PREFIX}*",)
# Stream channels — re-exported here for tests + symmetry with PUBSUB_CHANNELS.
STREAM_NAMES: tuple[str, ...] = (CH_ALERTS, CH_CLUSTER_UPDATES, CH_METRICS)


class RedisBridge:
    """Owns the Redis subscriber and stream-tailer tasks. Created once
    at app startup; both tasks are cancelled in ``stop()``."""

    def __init__(
        self,
        manager: ConnectionManager,
        *,
        stream_channels: Iterable[str] | None = None,
        pubsub_channels: Iterable[str] | None = None,
        pubsub_patterns: Iterable[str] | None = None,
    ) -> None:
        self._manager = manager
        self._streams: tuple[str, ...] = (
            tuple(stream_channels) if stream_channels is not None else STREAM_NAMES
        )
        self._pubsub_channels: tuple[str, ...] = (
            tuple(pubsub_channels) if pubsub_channels is not None else PUBSUB_CHANNELS
        )
        self._pubsub_patterns: tuple[str, ...] = (
            tuple(pubsub_patterns) if pubsub_patterns is not None else PUBSUB_PATTERNS
        )
        self._client: redis_async.Redis | None = None
        self._pubsub: redis_async.client.PubSub | None = None
        self._pubsub_task: asyncio.Task[None] | None = None
        self._stream_task: asyncio.Task[None] | None = None
        self._stopped = asyncio.Event()

    async def start(self) -> None:
        settings = get_settings()
        self._client = redis_async.from_url(settings.redis_url, decode_responses=True)  # type: ignore[no-untyped-call]
        if self._pubsub_channels or self._pubsub_patterns:
            self._pubsub = self._client.pubsub()
            if self._pubsub_channels:
                await self._pubsub.subscribe(*self._pubsub_channels)
            if self._pubsub_patterns:
                await self._pubsub.psubscribe(*self._pubsub_patterns)
            self._pubsub_task = asyncio.create_task(self._pubsub_loop(), name="ws.redis_bridge.pubsub")
        if self._streams:
            self._stream_task = asyncio.create_task(self._stream_loop(), name="ws.redis_bridge.streams")
        logger.info(
            "ws.bridge.started",
            streams=list(self._streams),
            pubsub_channels=list(self._pubsub_channels),
            pubsub_patterns=list(self._pubsub_patterns),
        )

    async def stop(self) -> None:
        self._stopped.set()
        for task in (self._stream_task, self._pubsub_task):
            if task is not None:
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):  # noqa: BLE001
                    pass
        if self._pubsub is not None:
            try:
                await self._pubsub.unsubscribe()
                await self._pubsub.punsubscribe()
                await self._pubsub.aclose()  # type: ignore[no-untyped-call]
            except Exception:  # noqa: BLE001
                pass
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception:  # noqa: BLE001
                pass
        logger.info("ws.bridge.stopped")

    # ------------------------------------------------------------------
    # Pub/sub loop — channels with no replay use case
    # ------------------------------------------------------------------

    async def _pubsub_loop(self) -> None:
        assert self._pubsub is not None
        backoff = 0.5
        while not self._stopped.is_set():
            try:
                message = await self._pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message is None:
                    backoff = 0.5
                    continue
                # "message" for direct subs, "pmessage" for pattern subs.
                if message.get("type") not in ("message", "pmessage"):
                    continue
                channel = message.get("channel")
                raw = message.get("data")
                if not channel or raw is None:
                    continue
                try:
                    payload: dict[str, Any] = json.loads(raw) if isinstance(raw, str) else raw
                except json.JSONDecodeError:
                    logger.warning("ws.bridge.bad_payload", channel=channel)
                    continue
                await self._manager.broadcast(channel, payload)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.error("ws.bridge.pubsub_error", error=str(exc))
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 10.0)

    # ------------------------------------------------------------------
    # Streams loop — durable, replay-capable channels
    # ------------------------------------------------------------------

    async def _stream_loop(self) -> None:
        assert self._client is not None
        # Track our cursor per stream — start at "$" (only entries
        # arriving after we connect). XREAD updates these as we go.
        cursors: dict[str, str] = {ch: "$" for ch in self._streams}
        backoff = 0.5
        while not self._stopped.is_set():
            try:
                # XREAD BLOCK across all three streams. 1000ms is short
                # enough for prompt shutdown; the loop reissues immediately.
                # redis-py's xread signature wants dict[bytes|str|memoryview,
                # bytes|str|...] — our str→str dict is fine at runtime.
                response = await self._client.xread(
                    cursors,  # type: ignore[arg-type]
                    count=100,
                    block=1000,
                )
                if not response:
                    backoff = 0.5
                    continue
                for stream_name, entries in response:
                    for entry_id, fields in entries:
                        cursors[stream_name] = entry_id
                        raw = fields.get("payload") if isinstance(fields, dict) else None
                        if not raw:
                            continue
                        try:
                            payload = json.loads(raw)
                        except json.JSONDecodeError:
                            logger.warning(
                                "ws.bridge.bad_stream_payload",
                                stream=stream_name,
                                entry_id=entry_id,
                            )
                            continue
                        payload["_stream_id"] = entry_id
                        await self._manager.broadcast(stream_name, payload)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.error("ws.bridge.stream_error", error=str(exc))
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 10.0)
