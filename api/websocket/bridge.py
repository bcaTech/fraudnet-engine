"""Redis pub/sub → WebSocket broadcast bridge.

The bridge is created once at app startup and holds a single Redis pub/sub
subscription on the channels listed in :data:`SUBSCRIBED_CHANNELS`. As
messages arrive, each is parsed as JSON and pushed to every WebSocket
connection associated with the matching channel via the shared
:class:`~api.websocket.manager.ConnectionManager`.

This decouples *event producers* (route handlers, Celery tasks, Kafka
consumers) from *event consumers* (the WS clients). Producers don't need
to know about WS connection state; they just publish to Redis. The bridge
also lets us scale beyond a single API replica — every replica subscribes
to the same channels, so a publish from any process reaches every connected
client regardless of which replica it landed on.
"""

from __future__ import annotations

import asyncio
import json
from typing import Iterable

import redis.asyncio as redis_async

from config.logging import get_logger
from config.settings import get_settings

from .manager import ConnectionManager
from .publisher import CH_ALERTS, CH_CLUSTER_UPDATES, CH_METRICS

logger = get_logger(__name__)


SUBSCRIBED_CHANNELS: tuple[str, ...] = (CH_ALERTS, CH_CLUSTER_UPDATES, CH_METRICS)


class RedisBridge:
    """Owns a Redis pub/sub subscription and fans incoming messages out
    to local WebSocket connections."""

    def __init__(self, manager: ConnectionManager, channels: Iterable[str] | None = None) -> None:
        self._manager = manager
        self._channels = tuple(channels) if channels is not None else SUBSCRIBED_CHANNELS
        self._client: redis_async.Redis | None = None
        self._pubsub: redis_async.client.PubSub | None = None
        self._task: asyncio.Task[None] | None = None
        self._stopped = asyncio.Event()

    async def start(self) -> None:
        settings = get_settings()
        self._client = redis_async.from_url(settings.redis_url, decode_responses=True)
        self._pubsub = self._client.pubsub()
        await self._pubsub.subscribe(*self._channels)
        self._task = asyncio.create_task(self._loop(), name="ws.redis_bridge")
        logger.info("ws.bridge.started", channels=list(self._channels))

    async def stop(self) -> None:
        self._stopped.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        if self._pubsub is not None:
            try:
                await self._pubsub.unsubscribe()
                await self._pubsub.aclose()
            except Exception:  # noqa: BLE001
                pass
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception:  # noqa: BLE001
                pass
        logger.info("ws.bridge.stopped")

    async def _loop(self) -> None:
        assert self._pubsub is not None
        backoff = 0.5
        while not self._stopped.is_set():
            try:
                message = await self._pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=1.0
                )
                if message is None:
                    backoff = 0.5
                    continue
                channel = message.get("channel")
                raw = message.get("data")
                if not channel or raw is None:
                    continue
                try:
                    payload = json.loads(raw) if isinstance(raw, str) else raw
                except json.JSONDecodeError:
                    logger.warning("ws.bridge.bad_payload", channel=channel)
                    continue
                await self._manager.broadcast(channel, payload)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — survive any single message
                logger.error("ws.bridge.loop_error", error=str(exc))
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 10.0)
