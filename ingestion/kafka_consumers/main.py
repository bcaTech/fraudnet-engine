"""Kafka consumer entry point.

Boots one ``asyncio.Task`` per consumer and supervises them. Run via the
``consumer`` Docker service::

    python -m ingestion.kafka_consumers.main

Each consumer keeps its own offsets in its own consumer group so they
can be scaled / restarted independently. If any task crashes, the
supervisor logs and exits non-zero so the container restart policy
takes over — we don't try to silently restart in-process because that
masks real bugs.
"""

from __future__ import annotations

import asyncio
import signal
from contextlib import suppress

from config.constants import KAFKA_TOPICS
from config.logging import configure_logging, get_logger
from core.graph.client import get_neo4j_client

from .safeguard_consumer import SafeguardConsumer
from .scancom_consumer import ScancomConsumer
from .transaction_consumer import TransactionConsumer

logger = get_logger(__name__)


async def run_all() -> None:
    configure_logging()
    logger.info("kafka.consumers.boot")

    # Connect the Neo4j driver up front. Each consumer reuses the
    # process-wide singleton, so only one connect() is needed.
    client = get_neo4j_client()
    await client.connect()

    consumers = [
        TransactionConsumer(),
        SafeguardConsumer(),
        ScancomConsumer(KAFKA_TOPICS["sim_swaps"], "sim_swaps"),
        ScancomConsumer(KAFKA_TOPICS["device_events"], "device_events"),
    ]

    tasks = [asyncio.create_task(c.run(), name=type(c).__name__) for c in consumers]

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):  # Windows doesn't support add_signal_handler
            loop.add_signal_handler(sig, stop.set)

    # Surface the first failure rather than swallowing it.
    done, pending = await asyncio.wait(
        [*tasks, asyncio.create_task(stop.wait())],
        return_when=asyncio.FIRST_COMPLETED,
    )
    logger.warning("kafka.consumers.shutdown_initiated")
    for c in consumers:
        await c.stop()
    for t in tasks:
        if not t.done():
            t.cancel()
    for t in tasks:
        with suppress(asyncio.CancelledError, Exception):
            await t
    await client.close()

    # Re-raise the first task exception, if any, so the container exits
    # non-zero and the orchestrator can replace it.
    for t in done:
        if t in tasks and t.exception() is not None:
            raise t.exception()  # type: ignore[misc]


def main() -> None:
    try:
        asyncio.run(run_all())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
