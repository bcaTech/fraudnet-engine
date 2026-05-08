# ADR 0003 — Redis (not Aerospike) for the v1 in-memory layer

- **Status:** Accepted.
- **Date:** 2026-05-08.
- **Deciders:** Backend engineering.
- **Related:** `docs/FraudNet_2.0_backend_spec.md` §4.2 specifies
  Aerospike for the inline-tier feature store.

## Context

The FraudNet 2.0 spec picks **Aerospike** because it delivers
sub-millisecond p99 reads at telco scale and replicates cross-DC for
the eventual pan-African federation. Redis is the more familiar
choice for a v1 — Celery uses it as broker + result backend, the
WebSocket bridge uses pub/sub, and the rules engine uses it for
15-minute dedup keys.

## Decision

Use **Redis 7** as the only in-memory store for v1. It plays four
roles:

1. **Celery broker + result backend** (`celery[redis]`).
2. **WebSocket pub/sub bridge** — every replica subscribes to the
   shared channels, so any producer reaches every connected client.
3. **Rules-engine dedup keys** — `SETNX` with a 15-minute TTL.
4. **Hot-cache placeholder** — currently unused for caching but
   wired so the dashboard layer can drop in TTL'd reads later.

We're not running an inline tier yet. There's no VoLTE in-call
tagging; the worst latency budget today is the analyst dashboard's
200–500ms tolerance. Redis comfortably meets that.

The feature store described in the FraudNet 2.0 spec (per-MSISDN
windowed counters with sub-millisecond reads) doesn't exist yet
either. When it does, that's the right time to introduce Aerospike;
today, premature.

## Consequences

**Positive**

- One technology to operate (Celery, WS, dedup all use the same
  Redis cluster).
- Familiar tooling, good Python async client (`redis>=5.0`).
- Free up engineering time we'd otherwise spend writing an Aerospike
  client.

**Negative**

- When the inline tier (Tier 1 actions) lands, Redis won't meet the
  sub-millisecond p99 the latency budget demands. Aerospike will
  need to be introduced specifically for that feature store, with
  Redis kept for everything else. That's a workable end state.
- Redis's persistence story (RDB + AOF) is more fragile than
  Aerospike's. Today we treat Redis as soft state; if it loses data,
  the next decay run + the next rules trigger fill in the gaps.

## Migration trigger

Introduce Aerospike when:

- The inline tier lands and we need < 1ms p99 reads per VoLTE call.
- Or: cross-DC feature replication becomes a hard requirement for
  the pan-African federation.

Until then, Redis stays.
