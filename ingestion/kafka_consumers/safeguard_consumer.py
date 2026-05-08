"""SafeGuard event consumer.

Subscribes to ``fraudnet.safeguard.events``. SafeGuard emits four
fraud-relevant signals:

- ``return_to_sender`` — the recipient reversed an inbound MoMo transfer
  (strong signal; recipient is suspected fraud target → seed the sender).
- ``didnt_know_you`` — the recipient declined a Send-with-Care prompt
  marked "I don't know this person" (similar but lower-strength).
- ``send_with_care_recall`` — the sender cancelled mid-flow after the
  prompt (counts as a near-miss; lighter signal).
- ``ask_me_first_decline`` — Ask-Me-First confirmation declined.

For the strongest two signals (RTS and DIKY) we *seed the sender* — i.e.
boost their risk score, mark for analyst review, and emit an alert. The
mesh expansion uses these as starting points (downstream task in
``core.mesh.expansion``).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select

from config.constants import KAFKA_TOPICS
from config.logging import get_logger
from core.graph.client import get_neo4j_client
from db.models import Alert
from db.session import get_async_session

from ._base import KafkaConsumerBase

logger = get_logger(__name__)


_SEED_EVENTS = {"return_to_sender", "didnt_know_you"}
_LIGHT_EVENTS = {"send_with_care_recall", "ask_me_first_decline"}


class SafeguardConsumer(KafkaConsumerBase):
    topic = KAFKA_TOPICS["safeguard"]
    group_id = "fraudnet.engine.safeguard"

    async def handle(self, event: dict[str, Any]) -> None:
        kind = str(event.get("event_type") or event.get("type") or "").lower()
        if kind not in _SEED_EVENTS and kind not in _LIGHT_EVENTS:
            logger.debug("safeguard.unknown_event", kind=kind)
            return

        sender_wallet = event.get("sender_wallet_id") or event.get("src_wallet_id")
        recipient_wallet = event.get("recipient_wallet_id") or event.get("dst_wallet_id")
        if not sender_wallet:
            raise ValueError("safeguard event missing sender_wallet_id")

        client = get_neo4j_client()

        if kind in _SEED_EVENTS:
            # Strong signal: bump risk + flag the sender, mark as a seed.
            await client.execute_write(
                """
                MERGE (w:Wallet {wallet_id: $wallet_id})
                SET w.status = CASE
                        WHEN w.status = 'frozen' THEN w.status
                        ELSE 'flagged'
                    END,
                    w.risk_score = CASE
                        WHEN coalesce(w.risk_score, 0.0) > 0.85 THEN w.risk_score
                        ELSE coalesce(w.risk_score, 0.0) + 0.15
                    END,
                    w.last_seed_event = $kind,
                    w.last_seed_event_at = datetime()
                """,
                {"wallet_id": sender_wallet, "kind": kind},
            )
            await self._open_alert(
                kind=kind,
                wallet_id=sender_wallet,
                severity="high" if kind == "return_to_sender" else "medium",
                title=(
                    "Return-to-Sender reversal — possible fraud sender"
                    if kind == "return_to_sender"
                    else "Recipient declined transfer (Don't Know You)"
                ),
                description=(
                    f"SafeGuard event '{kind}' from {sender_wallet} "
                    f"→ {recipient_wallet or 'unknown'}."
                ),
                metadata=event,
            )
            await self._broadcast(kind, sender_wallet, recipient_wallet)
        else:
            # Lighter signal: just record on the wallet for analyst visibility.
            await client.execute_write(
                """
                MERGE (w:Wallet {wallet_id: $wallet_id})
                SET w.safeguard_lighter_count =
                        coalesce(w.safeguard_lighter_count, 0) + 1,
                    w.last_safeguard_event = $kind,
                    w.last_safeguard_event_at = datetime()
                """,
                {"wallet_id": sender_wallet, "kind": kind},
            )

    # -- helpers ----------------------------------------------------------

    async def _open_alert(
        self,
        *,
        kind: str,
        wallet_id: str,
        severity: str,
        title: str,
        description: str,
        metadata: dict[str, Any],
    ) -> None:
        # Dedup: if an unacknowledged safeguard alert exists for this wallet
        # in the last hour, skip.
        async with get_async_session() as db:
            existing = (
                await db.execute(
                    select(Alert).where(
                        Alert.target_type == "wallet",
                        Alert.target_id == wallet_id,
                        Alert.type == "safeguard_seed",
                        Alert.acknowledged.is_(False),
                    ).limit(1)
                )
            ).scalar_one_or_none()
            if existing is not None:
                return
            alert = Alert(
                id=f"alert-{uuid.uuid4().hex[:12]}",
                created_at=datetime.now(timezone.utc),
                type="safeguard_seed",
                severity=severity,
                title=title,
                description=description,
                target_type="wallet",
                target_id=wallet_id,
                cluster_id=None,
                acknowledged=False,
                rule_id=None,
                extra={"event_kind": kind, "raw": metadata},
            )
            db.add(alert)
            await db.commit()

    @staticmethod
    async def _broadcast(kind: str, sender: str, recipient: str | None) -> None:
        try:
            from api.websocket.publisher import CH_ALERTS, publish

            await publish(
                CH_ALERTS,
                "safeguard.seed",
                {"event_kind": kind, "sender_wallet_id": sender, "recipient_wallet_id": recipient},
            )
        except Exception:  # noqa: BLE001 — best-effort
            pass
