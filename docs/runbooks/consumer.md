# Runbook — Kafka consumers

One Docker service (`consumer`) supervises four async tasks:
`TransactionConsumer`, `SafeguardConsumer`, two `ScancomConsumer`s
(SIM swaps + device events).

## Health

- `docker compose ps consumer` shows plain `Up` — no probe (Kafka
  consumer-group lag is the proper signal).
- Look for `kafka.consumer.started` log lines on boot, one per topic.

## Symptoms ↔ likely cause ↔ first action

| Symptom | Likely cause | First action |
|---|---|---|
| Container restarts in a loop | Kafka not ready at boot | `docker compose logs kafka`; the consumer's depends_on waits for kafka healthy but a slow Kafka can outlast it |
| One topic shows no `consumer.started` | Topic doesn't exist (auto-create disabled?) | Confirm topic via `docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list` |
| `kafka.handler.error` logs | Bad payload OR the handler raised | Message routed to `<topic>.dlq.v1`. Inspect the DLQ; replay manually after fix |
| Consumer-group lag growing | Throughput exceeded or downstream Neo4j slow | Scale up: increase consumer concurrency or sharder partitions |
| Same event processed twice | At-least-once delivery + non-idempotent handler | All current handlers MERGE on `tx_id` / id-keyed Cypher; investigate any new handler |

## DLQ replay

Each topic has a `<topic>.dlq.v1` companion. Replaying:

```bash
# Inspect DLQ contents
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic fraudnet.transactions.dlq.v1 --from-beginning --max-messages 10
```

A proper replay tool lives at `tools/replay/` (planned). For now, fix
the underlying issue, then re-publish good messages from the DLQ
manually.

## Common one-liners

```bash
# Publish a synthetic transaction
docker compose exec -T api python -c "
import asyncio, json
from aiokafka import AIOKafkaProducer
async def m():
    p = AIOKafkaProducer(bootstrap_servers='kafka:9092')
    await p.start()
    try:
        await p.send_and_wait('fraudnet.transactions', json.dumps({
            'tx_id': 'TX-MANUAL-001', 'type': 'p2p', 'amount': 100,
            'timestamp': '2026-05-08T01:00:00Z',
            'src_wallet_id': 'MOMO-000010', 'dst_wallet_id': 'MOMO-000020',
        }).encode())
    finally:
        await p.stop()
asyncio.run(m())
"

# Tail consumer output
docker compose logs --no-color -f consumer
```

## Escalation

- Consumer-group lag > 10 min and not draining: page Backend
  on-call. Live transactions aren't being indexed.
- Mass DLQ growth (>1000 messages in an hour): investigate before
  replaying. A schema change upstream is the most likely cause.

## SLOs

- End-to-end event latency (Kafka publish → graph write) < 2s p95.
- DLQ rate < 0.1% of total events.
