"""Mesh-engine async tasks.

Wraps :mod:`core.mesh` entry points so they can be enqueued as Celery jobs.
The real ``expand_seed`` task wraps :func:`core.mesh.expansion.expand_from_seed`
in ``asyncio.run`` once the worker is wired with a long-lived Neo4j driver.
For now these are stubs that log and return a status dict.
"""

from __future__ import annotations

from config.logging import configure_logging, get_logger

from .celery_app import app

configure_logging()
logger = get_logger(__name__)


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
    logger.info("celery.mesh.rescore_cluster", status="stub", cluster_id=cluster_id)
    return {"status": "stub", "cluster_id": cluster_id}
