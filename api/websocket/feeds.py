"""WebSocket feed routes.

Six feeds, mixing two transports:

- Redis Streams (durable, replayable): ``/ws/alerts``, ``/ws/cluster-
  updates``, ``/ws/metrics``. Pass ``?since=<stream_id>`` on connect to
  replay every entry posted since that id; the route XRANGE-replays
  the gap then hands over to the bridge for live tail.
- Redis pub/sub (ephemeral): ``/ws/rules``, ``/ws/integration``,
  ``/ws/takedown/{id}``. No backfill — these aren't load-bearing for
  reconnection.

Each route delegates to the shared :class:`ConnectionManager`; the
:class:`~api.websocket.bridge.RedisBridge` keeps broadcasting once the
connection is registered. The ``await ws.receive_text()`` loop blocks
until the client disconnects (FastAPI's standard pattern). Any
inbound text is ignored — these are server-push feeds.
"""

from __future__ import annotations

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect

from config.logging import get_logger

from .manager import get_manager
from .publisher import (
    CH_ALERTS,
    CH_CLUSTER_UPDATES,
    CH_INTEGRATION,
    CH_METRICS,
    CH_RULES,
    STREAM_CHANNELS,
    fetch_history,
    takedown_channel,
)

logger = get_logger(__name__)


router = APIRouter(tags=["websocket"])


async def _serve(channel: str, ws: WebSocket, *, since: str | None = None) -> None:
    manager = get_manager()
    await manager.connect(channel, ws)
    try:
        await ws.send_json({"event": "hello", "data": {"channel": channel}})

        # Stream-backed channels support replay: drain history first,
        # then defer to the bridge for live tail. There's a small race
        # where new entries may arrive between XRANGE and the bridge's
        # next XREAD pass and be replayed twice; clients dedup by
        # ``_stream_id``.
        if since and channel in STREAM_CHANNELS:
            history = await fetch_history(channel, since=since)
            for entry in history:
                await ws.send_json(entry)

        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception as exc:  # noqa: BLE001 — never raise out of a WS handler
        logger.warning("ws.handler.error", channel=channel, error=str(exc))
    finally:
        await manager.disconnect(channel, ws)


# ---------------------------------------------------------------------------
# Stream-backed feeds (replay supported via ?since=)
# ---------------------------------------------------------------------------


@router.websocket("/ws/alerts")
async def ws_alerts(
    websocket: WebSocket,
    since: str | None = Query(None, description="Stream id to resume from"),
) -> None:
    await _serve(CH_ALERTS, websocket, since=since)


@router.websocket("/ws/cluster-updates")
async def ws_cluster_updates(
    websocket: WebSocket,
    since: str | None = Query(None, description="Stream id to resume from"),
) -> None:
    await _serve(CH_CLUSTER_UPDATES, websocket, since=since)


@router.websocket("/ws/metrics")
async def ws_metrics(
    websocket: WebSocket,
    since: str | None = Query(None, description="Stream id to resume from"),
) -> None:
    await _serve(CH_METRICS, websocket, since=since)


# ---------------------------------------------------------------------------
# Pub/sub-backed feeds (no replay)
# ---------------------------------------------------------------------------


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
