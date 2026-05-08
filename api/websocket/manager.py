"""WebSocket connection registry.

A single :class:`ConnectionManager` instance is created at app startup and
held on ``app.state.ws_manager``. Each WebSocket feed calls
:meth:`connect` on accept and :meth:`disconnect` on close; the
:class:`~api.websocket.bridge.RedisBridge` calls :meth:`broadcast` when a
message arrives on the corresponding Redis channel.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any

from fastapi import WebSocket
from starlette.websockets import WebSocketState

from config.logging import get_logger

logger = get_logger(__name__)


class ConnectionManager:
    """In-memory channel → connection set with concurrent-safe fanout."""

    def __init__(self) -> None:
        self._connections: dict[str, set[WebSocket]] = defaultdict(set)
        self._lock = asyncio.Lock()

    async def connect(self, channel: str, ws: WebSocket) -> None:
        await ws.accept()
        async with self._lock:
            self._connections[channel].add(ws)
        logger.info(
            "ws.connected",
            channel=channel,
            total=sum(len(s) for s in self._connections.values()),
        )

    async def disconnect(self, channel: str, ws: WebSocket) -> None:
        async with self._lock:
            self._connections[channel].discard(ws)
        if ws.client_state != WebSocketState.DISCONNECTED:
            try:
                await ws.close()
            except Exception:  # noqa: BLE001 — best-effort close
                pass

    async def broadcast(self, channel: str, message: dict[str, Any]) -> None:
        """Send ``message`` (JSON-serialisable) to every active connection on
        ``channel``. Connections that fail are evicted."""

        async with self._lock:
            targets = list(self._connections.get(channel, ()))
        if not targets:
            return
        dead: list[WebSocket] = []
        for ws in targets:
            try:
                await ws.send_json(message)
            except Exception as exc:  # noqa: BLE001 — drop on any send failure
                logger.warning("ws.send.failed", channel=channel, error=str(exc))
                dead.append(ws)
        if dead:
            async with self._lock:
                for ws in dead:
                    self._connections[channel].discard(ws)

    def channel_sizes(self) -> dict[str, int]:
        return {ch: len(s) for ch, s in self._connections.items()}


# Module-level singleton — created lazily so import order doesn't matter.
_manager: ConnectionManager | None = None


def get_manager() -> ConnectionManager:
    global _manager
    if _manager is None:
        _manager = ConnectionManager()
    return _manager
