"""WebSocket feed routes.

Three feeds, each backed by a Redis pub/sub channel:

- ``/ws/alerts`` — alert lifecycle (new, acknowledged, dismissed)
- ``/ws/cluster-updates`` — cluster status / confidence / member changes
- ``/ws/metrics`` — dashboard metric snapshot every 5s

Each route just delegates to the shared :class:`ConnectionManager`. The
:class:`~api.websocket.bridge.RedisBridge` does the actual broadcasting
when messages arrive on the corresponding channel; the connection only
needs to stay open and absorb whatever the bridge sends.

The ``await ws.receive_text()`` loop blocks until the client disconnects,
which is what FastAPI uses to detect closed connections. Any text the
client sends is ignored — these are server-push feeds, not duplex.
"""

from __future__ import annotations

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from config.logging import get_logger

from .manager import get_manager
from .publisher import (
    CH_ALERTS,
    CH_CLUSTER_UPDATES,
    CH_INTEGRATION,
    CH_METRICS,
    CH_RULES,
    takedown_channel,
)

logger = get_logger(__name__)


router = APIRouter(tags=["websocket"])


async def _serve(channel: str, ws: WebSocket) -> None:
    manager = get_manager()
    await manager.connect(channel, ws)
    try:
        # Send a tiny hello so clients can confirm the channel is live.
        await ws.send_json({"event": "hello", "data": {"channel": channel}})
        while True:
            # Block until disconnect; any inbound text is ignored.
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception as exc:  # noqa: BLE001 — never raise out of a WS handler
        logger.warning("ws.handler.error", channel=channel, error=str(exc))
    finally:
        await manager.disconnect(channel, ws)


@router.websocket("/ws/alerts")
async def ws_alerts(websocket: WebSocket) -> None:
    await _serve(CH_ALERTS, websocket)


@router.websocket("/ws/cluster-updates")
async def ws_cluster_updates(websocket: WebSocket) -> None:
    await _serve(CH_CLUSTER_UPDATES, websocket)


@router.websocket("/ws/metrics")
async def ws_metrics(websocket: WebSocket) -> None:
    await _serve(CH_METRICS, websocket)


@router.websocket("/ws/rules")
async def ws_rules(websocket: WebSocket) -> None:
    """Rule trigger events + shadow-mode log entries."""

    await _serve(CH_RULES, websocket)


@router.websocket("/ws/integration")
async def ws_integration(websocket: WebSocket) -> None:
    """Operator-integration events: inbound/outbound flag receipts,
    operator status changes, health-check results."""

    await _serve(CH_INTEGRATION, websocket)


@router.websocket("/ws/takedown/{takedown_id}")
async def ws_takedown(websocket: WebSocket, takedown_id: str) -> None:
    """Live progress for a single takedown. Each connection scopes itself
    to the per-takedown channel via the prefix; the bridge psubscribes
    so dynamic channels work out of the box."""

    await _serve(takedown_channel(takedown_id), websocket)
