"""Mesh-engine async tasks.

Wraps :mod:`core.mesh` and :mod:`core.analytics` entry points so they
can be enqueued as Celery jobs. The community detection + centrality
batches run from the periodic schedule (``rescore_active_clusters``);
expand_seed remains a stub until the workers are wired with the real
Neo4j connection lifecycle.
"""

from __future__ import annotations

from config.logging import configure_logging, get_logger

from .celery_app import app

configure_logging()
logger = get_logger(__name__)


def _run_async(coro):
    """Shared sync→async bridge. Delegates to the canonical helper in
    :mod:`tasks.periodic` so per-loop client teardown is centralised."""

    from .periodic import _run_async as _shared_run_async

    return _shared_run_async(coro)


@app.task(name="tasks.mesh_tasks.expand_seed")
def expand_seed(node_id: str, node_type: str, confidence: float = 0.85) -> dict:
    logger.info(
        "celery.mesh.expand_seed",
        status="stub",
        node_id=node_id,
        node_type=node_type,
        confidence=confidence,
    )
    return {"status": "stub", "node_id": node_id}


@app.task(name="tasks.mesh_tasks.rescore_cluster")
def rescore_cluster(cluster_id: str) -> dict:
    """Run community detection + centrality for a single cluster and
    persist the results back to the graph."""

    async def _go():
        from core.analytics.centrality import compute_for_cluster
        from core.analytics.community import detect_louvain
        from core.graph.client import get_neo4j_client

        client = get_neo4j_client()
        try:
            if client._driver is None:  # type: ignore[attr-defined]
                await client.connect()
        except AttributeError:
            await client.connect()
        comm = await detect_louvain(cluster_id)
        cent = await compute_for_cluster(cluster_id)
        return {"community": comm, "centrality": cent}

    try:
        result = _run_async(_go())
        logger.info("celery.mesh.rescore_cluster.complete", cluster_id=cluster_id)
        return result
    except Exception as exc:  # noqa: BLE001
        logger.error("celery.mesh.rescore_cluster.error", cluster_id=cluster_id, error=str(exc))
        raise


@app.task(name="tasks.mesh_tasks.rescore_active_clusters")
def rescore_active_clusters_task(limit: int = 30) -> dict:
    """Batch: community detection + centrality across every active cluster.

    Wired into the beat schedule via ``tasks.periodic.rescore_active_clusters``,
    which delegates here.
    """

    async def _go():
        from core.analytics.centrality import compute_for_active_clusters
        from core.analytics.community import detect_for_active_clusters
        from core.graph.client import get_neo4j_client

        client = get_neo4j_client()
        try:
            if client._driver is None:  # type: ignore[attr-defined]
                await client.connect()
        except AttributeError:
            await client.connect()
        comm = await detect_for_active_clusters(limit=limit)
        cent = await compute_for_active_clusters(limit=limit)
        return {"community": comm, "centrality": cent}

    try:
        result = _run_async(_go())
        logger.info("celery.mesh.rescore_active.complete", limit=limit)
        return result
    except Exception as exc:  # noqa: BLE001
        logger.error("celery.mesh.rescore_active.error", error=str(exc))
        raise
