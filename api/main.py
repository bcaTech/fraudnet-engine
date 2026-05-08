"""FastAPI application factory.

Bootstraps the ASGI app with:

- structured logging
- a connected Neo4j driver (initialised + schema-applied during startup)
- CORS for the Lovable / dev-server frontends
- the dashboard and cluster routers (additional routers will be added as
  their modules land — see ``CLAUDE.md`` for the full surface).

Run locally with ``uvicorn api.main:app --reload``.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse

from api.auth import routes as auth_routes
from api.routes import agents as agents_routes
from api.routes import alerts as alerts_routes
from api.routes import analytics as analytics_routes
from api.routes import clusters as clusters_routes
from api.routes import dashboard as dashboard_routes
from api.routes import integration as integration_routes
from api.routes import law_enforcement as law_enforcement_routes
from api.routes import nodes as nodes_routes
from api.routes import rules as rules_routes
from api.routes import takedowns as takedowns_routes
from api.schemas import APIResponse, ok
from api.websocket import feeds as ws_feeds
from api.websocket.bridge import RedisBridge
from api.websocket.manager import get_manager as get_ws_manager
from api.websocket.metrics_loop import MetricsPublisher
from api.websocket.publisher import close_client as close_ws_publisher
from config.logging import configure_logging, get_logger
from config.settings import get_settings
from core.graph.client import Neo4jClient, get_neo4j_client
from core.graph.queries import COUNT_BY_LABEL
from core.graph.schema import initialize_schema

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Connect Neo4j, apply the graph schema, start the WS bridge + metrics
    publisher, then yield."""

    configure_logging()
    settings = get_settings()
    logger.info("api.startup", environment=settings.environment)

    client: Neo4jClient = get_neo4j_client()
    await client.connect()

    try:
        await initialize_schema(client)
    except Exception as exc:  # noqa: BLE001 — log + re-raise so the container fails fast
        logger.error("api.schema.init.failed", error=str(exc))
        raise

    app.state.neo4j = client

    # ---- WebSocket plumbing ---------------------------------------------
    ws_manager = get_ws_manager()
    bridge = RedisBridge(ws_manager)
    metrics_pub = MetricsPublisher()
    await bridge.start()
    await metrics_pub.start()
    app.state.ws_manager = ws_manager
    app.state.ws_bridge = bridge
    app.state.ws_metrics = metrics_pub

    try:
        yield
    finally:
        await metrics_pub.stop()
        await bridge.stop()
        await close_ws_publisher()
        await client.close()
        logger.info("api.shutdown")


def create_app() -> FastAPI:
    """Construct and return the ASGI app. Imported by Uvicorn as ``api.main:app``."""

    settings = get_settings()
    app = FastAPI(
        title="FraudNet Intelligence Engine",
        version="0.1.0",
        description=(
            "AI-native fraud network intelligence platform for mobile money. "
            "See CLAUDE.md for the full specification."
        ),
        default_response_class=ORJSONResponse,
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ---- routers ---------------------------------------------------------
    app.include_router(auth_routes.router)
    app.include_router(dashboard_routes.router)
    app.include_router(clusters_routes.router)
    app.include_router(nodes_routes.router)
    app.include_router(agents_routes.router)
    app.include_router(alerts_routes.router)
    app.include_router(takedowns_routes.router)
    app.include_router(rules_routes.router)
    app.include_router(analytics_routes.router)
    app.include_router(law_enforcement_routes.router)
    app.include_router(integration_routes.router)
    app.include_router(integration_routes.external_router)
    app.include_router(ws_feeds.router)

    # ---- root + health --------------------------------------------------
    @app.get("/", include_in_schema=False)
    async def root() -> APIResponse[dict]:
        return ok(
            {
                "service": "fraudnet-engine",
                "version": app.version,
                "docs": "/docs",
                "health": "/health",
            }
        )

    @app.get("/health")
    async def health() -> APIResponse[dict]:
        client = get_neo4j_client()
        neo4j_ok = await client.health()
        node_counts: dict[str, int] = {}
        if neo4j_ok:
            try:
                rows = await client.execute_read(COUNT_BY_LABEL)
                node_counts = {row["label"]: int(row["n"]) for row in rows}
            except Exception:  # noqa: BLE001 — health endpoint is best-effort
                node_counts = {}
        return ok(
            {
                "status": "ok" if neo4j_ok else "degraded",
                "neo4j": "ok" if neo4j_ok else "down",
                "node_counts": node_counts,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )

    return app


app = create_app()
