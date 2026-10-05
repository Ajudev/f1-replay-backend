"""Minimal validation consumer handler: records that events arrive, nothing more.

It holds no race logic. It exists to prove the transport end to end (publish,
group delivery, ack) and as the template for real consumers.
"""

from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass
from uuid import UUID

from app.streaming.consumer import ReceivedMessage

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class AuditRecord:
    event_id: UUID
    replay_id: UUID
    run_id: UUID
    sequence: int


class AuditHandler:
    """Logs each event at DEBUG and keeps the most recent records in memory."""

    def __init__(self, max_records: int = 1000) -> None:
        self.records: deque[AuditRecord] = deque(maxlen=max_records)
        self.total = 0

    async def handle(self, message: ReceivedMessage) -> None:
        event = message.event
        self.records.append(
            AuditRecord(event.event_id, event.replay_id, event.run_id, event.sequence)
        )
        self.total += 1
        logger.debug(
            "Event audited event_id=%s replay_id=%s run_id=%s sequence=%d message_id=%s",
            event.event_id,
            event.replay_id,
            event.run_id,
            event.sequence,
            message.message_id,
        )
