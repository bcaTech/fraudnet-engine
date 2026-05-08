"""Neo4j schema initialisation.

Creates uniqueness constraints and supporting indexes that match the schema
documented in CLAUDE.md. Idempotent — safe to run on every API boot.
"""

from __future__ import annotations

from config.logging import get_logger

from .client import Neo4jClient

logger = get_logger(__name__)


# Uniqueness constraints. Each entry is a Cypher CREATE CONSTRAINT ... IF NOT EXISTS.
CONSTRAINT_STATEMENTS: list[str] = [
    "CREATE CONSTRAINT wallet_id IF NOT EXISTS FOR (w:Wallet) REQUIRE w.wallet_id IS UNIQUE",
    "CREATE CONSTRAINT msisdn IF NOT EXISTS FOR (p:PhoneNumber) REQUIRE p.msisdn IS UNIQUE",
    "CREATE CONSTRAINT imei IF NOT EXISTS FOR (h:Handset) REQUIRE h.imei IS UNIQUE",
    "CREATE CONSTRAINT imsi IF NOT EXISTS FOR (s:SIM) REQUIRE s.imsi IS UNIQUE",
    "CREATE CONSTRAINT agent_id IF NOT EXISTS FOR (a:Agent) REQUIRE a.agent_id IS UNIQUE",
    "CREATE CONSTRAINT tx_id IF NOT EXISTS FOR (t:Transaction) REQUIRE t.tx_id IS UNIQUE",
    "CREATE CONSTRAINT cluster_id IF NOT EXISTS FOR (c:Cluster) REQUIRE c.cluster_id IS UNIQUE",
    "CREATE CONSTRAINT cell_id IF NOT EXISTS FOR (t:CellTower) REQUIRE t.cell_id IS UNIQUE",
]

# Supporting indexes for hot read paths.
INDEX_STATEMENTS: list[str] = [
    "CREATE INDEX wallet_risk IF NOT EXISTS FOR (w:Wallet) ON (w.risk_score)",
    "CREATE INDEX wallet_cluster IF NOT EXISTS FOR (w:Wallet) ON (w.cluster_id)",
    "CREATE INDEX wallet_status IF NOT EXISTS FOR (w:Wallet) ON (w.status)",
    "CREATE INDEX wallet_confidence IF NOT EXISTS FOR (w:Wallet) ON (w.confidence_score)",
    "CREATE INDEX wallet_sleeper IF NOT EXISTS FOR (w:Wallet) ON (w.is_sleeper)",
    "CREATE INDEX handset_flagged IF NOT EXISTS FOR (h:Handset) ON (h.flagged)",
    "CREATE INDEX agent_risk IF NOT EXISTS FOR (a:Agent) ON (a.risk_score)",
    "CREATE INDEX agent_classification IF NOT EXISTS FOR (a:Agent) ON (a.classification)",
    "CREATE INDEX cluster_status IF NOT EXISTS FOR (c:Cluster) ON (c.status)",
    "CREATE INDEX cluster_confidence IF NOT EXISTS FOR (c:Cluster) ON (c.confidence_score)",
    "CREATE INDEX tx_timestamp IF NOT EXISTS FOR (t:Transaction) ON (t.timestamp)",
    "CREATE INDEX sim_flagged IF NOT EXISTS FOR (s:SIM) ON (s.flagged)",
]


async def initialize_schema(client: Neo4jClient) -> None:
    """Apply all constraints and indexes. Idempotent."""

    logger.info("neo4j.schema.initialize.start")
    for statement in CONSTRAINT_STATEMENTS + INDEX_STATEMENTS:
        await client.execute_write(statement)
        logger.debug("neo4j.schema.applied", statement=statement)
    logger.info(
        "neo4j.schema.initialize.complete",
        constraints=len(CONSTRAINT_STATEMENTS),
        indexes=len(INDEX_STATEMENTS),
    )


async def drop_schema(client: Neo4jClient) -> None:
    """Drop all FraudNet constraints and indexes. Used by tests only."""

    rows = await client.execute_read("SHOW CONSTRAINTS YIELD name")
    for row in rows:
        await client.execute_write(f"DROP CONSTRAINT {row['name']} IF EXISTS")
    rows = await client.execute_read("SHOW INDEXES YIELD name, type WHERE type <> 'LOOKUP'")
    for row in rows:
        await client.execute_write(f"DROP INDEX {row['name']} IF EXISTS")
