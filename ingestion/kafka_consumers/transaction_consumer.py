"""Transaction-event consumer.

Subscribes to ``fraudnet.transactions``. For each event we:

1. Validate the minimum payload (``tx_id``, ``type``, ``amount``,
   ``timestamp``, source / destination ids).
2. Upsert the wallet / agent endpoints in Neo4j (idempotent MERGE).
3. Write the corresponding edge (``SENT_TO`` / ``CASHED_OUT_AT`` /
   ``CASHED_IN_AT``).
4. Resolve the source wallet's identity snapshot.
5. Run live, realtime-mode rules against the enriched context. Any rule
   match dispatches actions and persists a ``RuleTrigger`` row.
6. Publish a ``transaction.processed`` event to the WS alerts channel
   when the transaction is flagged so the dashboard can react in
   near-real-time.

Idempotency: the edge MERGE keys on ``tx_id``. Replays don't duplicate
state. Rule dedup is handled by the engine (15-minute Redis windows).
"""

from __future__ import annotations

from typing import Any

from config.constants import KAFKA_TOPICS
from config.logging import get_logger
from core.graph.client import get_neo4j_client
from ingestion.enrichment.identity_resolver import resolve_by_wallet
from rules.engine import evaluate_event

from ._base import KafkaConsumerBase

logger = get_logger(__name__)


_REQUIRED = ("tx_id", "type", "amount", "timestamp")


class TransactionConsumer(KafkaConsumerBase):
    topic = KAFKA_TOPICS["transactions"]
    group_id = "fraudnet.engine.transactions"

    async def handle(self, event: dict[str, Any]) -> None:
        for k in _REQUIRED:
            if k not in event:
                raise ValueError(f"transaction missing '{k}': {event!r}")

        tx_type = str(event["type"]).lower()
        client = get_neo4j_client()

        if tx_type in ("p2p", "transfer", "send"):
            await self._wallet_to_wallet(event, client)
            source_wallet = event.get("src_wallet_id") or event.get("from_wallet_id")
        elif tx_type == "cashout":
            await self._wallet_to_agent(event, client, "CASHED_OUT_AT")
            source_wallet = event.get("wallet_id") or event.get("src_wallet_id")
        elif tx_type == "cashin":
            await self._wallet_to_agent(event, client, "CASHED_IN_AT")
            source_wallet = event.get("wallet_id") or event.get("src_wallet_id")
        else:
            # Unknown type — record the transaction node but don't draw an edge.
            await self._upsert_tx(event, client)
            source_wallet = event.get("src_wallet_id") or event.get("wallet_id")

        if not source_wallet:
            return

        snapshot = await resolve_by_wallet(str(source_wallet), client=client)
        ctx = {
            "_target_type": "wallet",
            "_target_id": snapshot.wallet_id or source_wallet,
            "_cluster_id": snapshot.cluster_id,
            "node": {
                **snapshot.as_dict(),
                "tx_amount": float(event["amount"]),
                "tx_type": tx_type,
                "flagged": bool(event.get("flagged")),
            },
            "transaction": {k: event.get(k) for k in event},
        }
        result = await evaluate_event(ctx)
        if result.get("matches", 0) > 0:
            logger.info(
                "kafka.txn.rule_matched",
                tx_id=event["tx_id"],
                wallet=ctx["_target_id"],
                matches=result["matches"],
            )

        if event.get("flagged"):
            await self._broadcast_flag(event, snapshot.cluster_id)

    # -- writes -----------------------------------------------------------

    @staticmethod
    async def _upsert_tx(event: dict[str, Any], client) -> None:
        await client.execute_write(
            """
            MERGE (t:Transaction {tx_id: $tx_id})
            SET t.type = $type,
                t.amount = $amount,
                t.timestamp = datetime($timestamp),
                t.status = coalesce($status, t.status, 'completed'),
                t.flagged = coalesce($flagged, t.flagged, false),
                t.flag_reason = coalesce($flag_reason, t.flag_reason)
            """,
            {
                "tx_id": event["tx_id"],
                "type": event["type"],
                "amount": float(event["amount"]),
                "timestamp": str(event["timestamp"]),
                "status": event.get("status"),
                "flagged": event.get("flagged"),
                "flag_reason": event.get("flag_reason"),
            },
        )

    async def _wallet_to_wallet(self, event: dict[str, Any], client) -> None:
        src = event.get("src_wallet_id") or event.get("from_wallet_id")
        dst = event.get("dst_wallet_id") or event.get("to_wallet_id")
        if not src or not dst:
            raise ValueError("p2p transaction requires src and dst wallet ids")
        await self._upsert_tx(event, client)
        await client.execute_write(
            """
            MERGE (a:Wallet {wallet_id: $src})
            MERGE (b:Wallet {wallet_id: $dst})
            MERGE (a)-[r:SENT_TO {tx_id: $tx_id}]->(b)
            SET r.amount = $amount,
                r.timestamp = datetime($timestamp),
                r.type = $type,
                r.strength = coalesce($strength, r.strength, 0.30)
            """,
            {
                "src": src,
                "dst": dst,
                "tx_id": event["tx_id"],
                "amount": float(event["amount"]),
                "timestamp": str(event["timestamp"]),
                "type": str(event["type"]),
                "strength": event.get("strength"),
            },
        )

    async def _wallet_to_agent(self, event: dict[str, Any], client, rel: str) -> None:
        wallet = event.get("wallet_id") or event.get("src_wallet_id")
        agent = event.get("agent_id")
        if not wallet or not agent:
            raise ValueError(f"{rel.lower()} requires wallet_id and agent_id")
        await self._upsert_tx(event, client)
        await client.execute_write(
            f"""
            MERGE (w:Wallet {{wallet_id: $wallet_id}})
            MERGE (a:Agent  {{agent_id:  $agent_id}})
            MERGE (w)-[r:{rel} {{tx_id: $tx_id}}]->(a)
            SET r.amount = $amount,
                r.timestamp = datetime($timestamp),
                r.strength = coalesce($strength, r.strength, 0.30)
            """,  # noqa: S608 — rel is from a hard-coded set above
            {
                "wallet_id": wallet,
                "agent_id": agent,
                "tx_id": event["tx_id"],
                "amount": float(event["amount"]),
                "timestamp": str(event["timestamp"]),
                "strength": event.get("strength"),
            },
        )

    @staticmethod
    async def _broadcast_flag(event: dict[str, Any], cluster_id: str | None) -> None:
        try:
            from api.websocket.publisher import CH_ALERTS, publish

            await publish(
                CH_ALERTS,
                "transaction.flagged",
                {
                    "tx_id": event["tx_id"],
                    "type": event["type"],
                    "amount": float(event["amount"]),
                    "flag_reason": event.get("flag_reason"),
                    "cluster_id": cluster_id,
                },
            )
        except Exception:  # noqa: BLE001 — broadcast is best-effort
            pass
