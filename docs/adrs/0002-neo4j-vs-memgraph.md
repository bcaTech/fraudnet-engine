# ADR 0002 — Neo4j (not Memgraph) for the v1 graph store

- **Status:** Accepted.
- **Date:** 2026-05-08.
- **Deciders:** Backend engineering, data infrastructure.
- **Related:** `docs/FraudNet_2.0_backend_spec.md` §4.2 specifies
  Memgraph as the steady-state target.

## Context

The graph is the integrating substrate (CLAUDE.md mission statement).
The FraudNet 2.0 plan picks **Memgraph** because it's an in-memory
graph designed for streaming workloads with continuous mutation and
sub-millisecond query latency. Neo4j Community is on-disk, JVM-based,
and slower under heavy mutation but with broader tooling, a richer
Cypher dialect (notably APOC + GDS), and zero licensing risk for an
operator-internal v1.

## Decision

Use **Neo4j 5 Community Edition** for v1.

Driver: official `neo4j` Python async driver. We accept the JVM
overhead and the on-disk write penalty in exchange for:

- The plugins ecosystem (APOC + Graph Data Science) — `gds.louvain`,
  `gds.pagerank`, etc. — saves us writing community detection from
  scratch in Python (we use NetworkX for now since the Cypher GDS
  surface is limited to Memgraph in production, but Neo4j gives us a
  fallback).
- Familiar tooling for the analyst team — Neo4j Browser is well-known
  and the desktop app makes ad-hoc investigation trivial.
- Mature backup/replication story for a single-region deployment.

The application code is written against **Cypher**, which is shared
between Neo4j and Memgraph. The driver is encapsulated in
`core.graph.client.Neo4jClient` and the query library
(`core/graph/queries.py`) holds every Cypher string — both surfaces
are vendor-neutral enough that swapping the driver implementation
should be a single-commit migration.

## Consequences

**Positive**

- Rich tooling, predictable cost, well-understood operability.
- Cypher portability keeps the migration cost low.
- The async driver fits FastAPI cleanly; no event-loop conflicts at
  the API layer (Celery requires a small ceremony — see the
  per-task client teardown in `tasks/periodic.py`).

**Negative**

- Streaming mutation throughput is lower than Memgraph would deliver.
  Acceptable for v1 (single MoMo operator, ~5k transactions/day in
  the demo). Re-evaluate when we hit ~50k/day sustained.
- We don't get Memgraph's MAGE algorithm library; we substitute
  NetworkX in `core/analytics/`, which is fine on small subgraphs
  but won't scale to a multi-million-node graph without partitioning.

## Migration trigger

Promote to Memgraph when **any** of the following becomes true:

- Sustained graph-write latency > 100ms p95.
- Sustained Neo4j heap GC pauses > 1s.
- We need streaming graph algorithms (Memgraph's MAGE) that NetworkX
  doesn't provide at the data scale.
