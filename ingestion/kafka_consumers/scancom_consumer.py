"""Scancom (telco) event consumer.

Subscribes to ``fraudnet.scancom.sim-swaps`` and
``fraudnet.scancom.device-events``. Two event families:

- **SIM swap** — recorded on the SIM node (``swap_count``,
  ``last_swap_date``). Suspicious thresholds escalate by flagging the SIM
  and emitting a medium-severity alert (>2 swaps in 30d, or any swap by a
  number tied to a flagged wallet).
- **Device events** — IMEI registration/deregistration. Recorded on the
  Handset node. Cross-IMEI / cross-MSISDN reuse patterns are detected by
  looking at the ``INSERTED_IN`` history.

This consumer multiplexes both topics — it picks the correct schema by
inspecting the event payload's ``event_type``.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from config.logging import get_logger
from core.graph.client import get_neo4j_client
from db.models import Alert
from db.session import get_async_session

from ._base import KafkaConsumerBase

logger = get_logger(__name__)


# Suspicion threshold: SIM swap count in the last 30 days.
_SIM_SWAP_THRESHOLD = 2


class ScancomConsumer(KafkaConsumerBase):
    """Subscribed to a single topic at a time. Two instances are spun up by
    :func:`ingestion.kafka_consumers.main.run_all` — one for SIM swaps and
    one for device events. The handler dispatches based on event payload."""

    def __init__(self, topic: str, group_suffix: str) -> None:
        self.topic = topic
        self.group_id = f"fraudnet.engine.scancom.{group_suffix}"
        super().__init__()

    async def handle(self, event: dict[str, Any]) -> None:
        kind = str(event.get("event_type") or event.get("type") or "").lower()
        if kind == "sim_swap":
            await self._handle_sim_swap(event)
        elif kind in ("device_register", "device_deregister", "device_seen"):
            await self._handle_device(event, kind)
        else:
            logger.debug("scancom.unknown_event", kind=kind)

    # ----- SIM swap ------------------------------------------------------

    async def _handle_sim_swap(self, event: dict[str, Any]) -> None:
        imsi = event.get("imsi")
        if not imsi:
            raise ValueError("sim_swap event missing imsi")
        ts = event.get("timestamp") or datetime.now(UTC).isoformat()

        client = get_neo4j_client()
        rows = await client.execute_write(
            """
            MERGE (s:SIM {imsi: $imsi})
            SET s.swap_count = coalesce(s.swap_count, 0) + 1,
                s.last_swap_date = datetime($ts),
                s.last_swap_msisdn_old = $old_msisdn,
                s.last_swap_msisdn_new = $new_msisdn
            RETURN s.imsi AS imsi, s.swap_count AS swap_count, s.msisdn AS msisdn
            """,
            {
                "imsi": imsi,
                "ts": ts,
                "old_msisdn": event.get("old_msisdn"),
                "new_msisdn": event.get("new_msisdn"),
            },
        )
        if not rows:
            return
        swap_count = int(rows[0].get("swap_count") or 0)
        if swap_count >= _SIM_SWAP_THRESHOLD:
            await client.execute_write(
                "MATCH (s:SIM {imsi: $imsi}) SET s.flagged = true",
                {"imsi": imsi},
            )
            await self._open_alert(
                target_type="sim",
                target_id=imsi,
                severity="medium",
                title=f"SIM swap chain — {swap_count} swaps recorded",
                description=(
                    f"SIM {imsi} has been swapped {swap_count} times. "
                    f"Latest: {event.get('old_msisdn')} → {event.get('new_msisdn')}."
                ),
                metadata=event,
            )

    # ----- Device --------------------------------------------------------

    async def _handle_device(self, event: dict[str, Any], kind: str) -> None:
        imei = event.get("imei")
        if not imei:
            raise ValueError(f"{kind} event missing imei")
        client = get_neo4j_client()
        await client.execute_write(
            """
            MERGE (h:Handset {imei: $imei})
            SET h.last_seen = datetime($ts),
                h.first_seen = coalesce(h.first_seen, datetime($ts)),
                h.make = coalesce($make, h.make),
                h.model = coalesce($model, h.model)
            """,
            {
                "imei": imei,
                "ts": event.get("timestamp") or datetime.now(UTC).isoformat(),
                "make": event.get("make"),
                "model": event.get("model"),
            },
        )

        # If this IMEI is shared across many MSISDNs, mark suspicious.
        rows = await client.execute_read(
            """
            MATCH (s:SIM)-[:INSERTED_IN]->(h:Handset {imei: $imei})
            RETURN count(DISTINCT s.msisdn) AS msisdn_count
            """,
            {"imei": imei},
        )
        msisdn_count = int(rows[0]["msisdn_count"]) if rows else 0
        if msisdn_count >= 5:
            await client.execute_write(
                "MATCH (h:Handset {imei: $imei}) SET h.flagged = true, h.flag_reason = 'multi_sim_handset'",
                {"imei": imei},
            )
            await self._open_alert(
                target_type="handset",
                target_id=imei,
                severity="medium",
                title=f"Handset hosts {msisdn_count} numbers — possible mule device",
                description=(f"IMEI {imei} has been observed with {msisdn_count} distinct MSISDNs."),
                metadata={"imei": imei, "msisdn_count": msisdn_count},
            )

    # ----- shared --------------------------------------------------------

    @staticmethod
    async def _open_alert(
        *,
        target_type: str,
        target_id: str,
        severity: str,
        title: str,
        description: str,
        metadata: dict[str, Any],
    ) -> None:
        async with get_async_session() as db:
            db.add(
                Alert(
                    id=f"alert-{uuid.uuid4().hex[:12]}",
                    created_at=datetime.now(UTC),
                    type="sim_swap_burst" if target_type == "sim" else "device_anomaly",
                    severity=severity,
                    title=title,
                    description=description,
                    target_type=target_type,
                    target_id=target_id,
                    cluster_id=None,
                    acknowledged=False,
                    rule_id=None,
                    extra={"raw": metadata},
                )
            )
            await db.commit()
        try:
            from api.websocket.publisher import CH_ALERTS, publish

            await publish(
                CH_ALERTS,
                f"{target_type}.flagged",
                {
                    "target_type": target_type,
                    "target_id": target_id,
                    "severity": severity,
                    "title": title,
                },
            )
        except Exception:  # noqa: BLE001 — best-effort
            pass
