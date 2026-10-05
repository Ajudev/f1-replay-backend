"""Event envelope carried on Redis Streams.

The envelope is independent of FastF1, SQLAlchemy and Redis. It is serialized as
a few flat header fields (for inspection with ``XRANGE``) plus ``data``, a compact
JSON document holding the full envelope. ``data`` is the source of truth.

Identifiers, deliberately distinct:

- ``event_id``: deterministic ``uuid5(run_id, str(sequence))``. A retried publish of
  the same logical event has the same id, so consumers can deduplicate.
- ``sequence``: position in the replay's timeline (contiguous from 0 per run).
- ``run_id``: one start or restart of a replay. A restart re-emits from sequence 0
  under a new ``run_id``, hence new ``event_id`` values.
- the Redis message id (``1700000000000-0``) is assigned by Redis at XADD; it is
  not part of the envelope and is exposed separately on the consumer side.

Schema compatibility: adding optional fields is non-breaking and needs no version
bump (decoders ignore unknown fields). Removing, renaming or retyping a field, or
changing its meaning, requires bumping ``STREAM_EVENT_SCHEMA_VERSION``.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID, uuid5

from app.replay.events import ReplayEvent
from app.streaming.errors import (
    MalformedEventError,
    StreamSerializationError,
    UnsupportedSchemaVersionError,
)

STREAM_EVENT_SCHEMA_VERSION = 1
DEFAULT_SUPPORTED_VERSIONS: frozenset[int] = frozenset({STREAM_EVENT_SCHEMA_VERSION})


def derive_event_id(run_id: UUID, sequence: int) -> UUID:
    """Deterministic event id: the same run and sequence always yield the same id."""
    return uuid5(run_id, str(sequence))


@dataclass(frozen=True, slots=True)
class StreamEvent:
    """One event as transported on a stream."""

    event_id: UUID
    schema_version: int
    event_type: str
    replay_id: UUID
    run_id: UUID
    session_id: UUID
    sequence: int
    race_time_ms: int
    lap_number: int | None
    driver_id: UUID | None
    driver_abbreviation: str | None
    published_at: datetime
    payload: dict[str, Any]

    @classmethod
    def from_replay_event(
        cls, event: ReplayEvent, *, published_at: datetime | None = None
    ) -> StreamEvent:
        return cls(
            event_id=derive_event_id(event.run_id, event.sequence),
            schema_version=STREAM_EVENT_SCHEMA_VERSION,
            event_type=event.event_type.value,
            replay_id=event.replay_id,
            run_id=event.run_id,
            session_id=event.session_id,
            sequence=event.sequence,
            race_time_ms=event.race_time_ms,
            lap_number=event.lap_number,
            driver_id=event.driver_id,
            driver_abbreviation=event.driver_abbreviation,
            published_at=published_at or datetime.now(UTC),
            payload=event.payload,
        )

    def to_fields(self) -> dict[str, str]:
        """Redis entry fields: flat headers plus the full JSON envelope in ``data``."""
        return {
            "event_id": str(self.event_id),
            "schema_version": str(self.schema_version),
            "event_type": self.event_type,
            "replay_id": str(self.replay_id),
            "run_id": str(self.run_id),
            "sequence": str(self.sequence),
            "data": self.to_json(),
        }

    def to_json(self) -> str:
        document = {
            "event_id": self.event_id,
            "schema_version": self.schema_version,
            "event_type": self.event_type,
            "replay_id": self.replay_id,
            "run_id": self.run_id,
            "session_id": self.session_id,
            "sequence": self.sequence,
            "race_time_ms": self.race_time_ms,
            "lap_number": self.lap_number,
            "driver_id": self.driver_id,
            "driver_abbreviation": self.driver_abbreviation,
            "published_at": self.published_at,
            "payload": self.payload,
        }
        try:
            return json.dumps(
                document,
                default=_json_default,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            )
        except (TypeError, ValueError) as exc:
            raise StreamSerializationError(
                f"Cannot serialize event replay_id={self.replay_id} sequence={self.sequence}: {exc}"
            ) from exc

    @classmethod
    def from_fields(
        cls,
        fields: Mapping[str, str] | None,
        *,
        supported_versions: frozenset[int] = DEFAULT_SUPPORTED_VERSIONS,
    ) -> StreamEvent:
        """Decode a Redis entry. Raises ``MalformedEventError`` or
        ``UnsupportedSchemaVersionError``; error messages never include the payload."""
        if not fields or "data" not in fields:
            raise MalformedEventError("Stream entry has no 'data' field")
        return cls.from_json(fields["data"], supported_versions=supported_versions)

    @classmethod
    def from_json(
        cls,
        raw: str,
        *,
        supported_versions: frozenset[int] = DEFAULT_SUPPORTED_VERSIONS,
    ) -> StreamEvent:
        try:
            document = json.loads(raw)
        except (TypeError, ValueError) as exc:
            raise MalformedEventError(f"Envelope is not valid JSON: {exc}") from exc
        if not isinstance(document, dict):
            raise MalformedEventError("Envelope must be a JSON object")

        version = document.get("schema_version")
        if isinstance(version, bool) or not isinstance(version, int):
            raise MalformedEventError("Field 'schema_version' must be an integer")
        # Checked before the rest: a newer version may legitimately have another shape.
        if version not in supported_versions:
            raise UnsupportedSchemaVersionError(version, supported_versions)

        payload = document.get("payload")
        if not isinstance(payload, dict):
            raise MalformedEventError("Field 'payload' must be an object")
        return cls(
            event_id=_uuid(document, "event_id"),
            schema_version=version,
            event_type=_string(document, "event_type"),
            replay_id=_uuid(document, "replay_id"),
            run_id=_uuid(document, "run_id"),
            session_id=_uuid(document, "session_id"),
            sequence=_integer(document, "sequence"),
            race_time_ms=_integer(document, "race_time_ms"),
            lap_number=_optional_integer(document, "lap_number"),
            driver_id=_optional_uuid(document, "driver_id"),
            driver_abbreviation=_optional_string(document, "driver_abbreviation"),
            published_at=_timestamp(document, "published_at"),
            payload=payload,
        )


def _json_default(value: object) -> Any:
    """Explicit encoders for the types payloads may contain; anything else fails."""
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise TypeError("naive datetime is not allowed; use a timezone-aware value")
        return value.astimezone(UTC).isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    raise TypeError(f"Object of type {type(value).__name__} is not serializable")


def _missing(key: str) -> MalformedEventError:
    return MalformedEventError(f"Field {key!r} is missing or has the wrong type")


def _string(doc: dict[str, Any], key: str) -> str:
    value = doc.get(key)
    if not isinstance(value, str) or not value:
        raise _missing(key)
    return value


def _optional_string(doc: dict[str, Any], key: str) -> str | None:
    value = doc.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise _missing(key)
    return value


def _integer(doc: dict[str, Any], key: str) -> int:
    value = doc.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise _missing(key)
    return value


def _optional_integer(doc: dict[str, Any], key: str) -> int | None:
    return None if doc.get(key) is None else _integer(doc, key)


def _uuid(doc: dict[str, Any], key: str) -> UUID:
    try:
        return UUID(_string(doc, key))
    except ValueError as exc:
        raise _missing(key) from exc


def _optional_uuid(doc: dict[str, Any], key: str) -> UUID | None:
    return None if doc.get(key) is None else _uuid(doc, key)


def _timestamp(doc: dict[str, Any], key: str) -> datetime:
    try:
        value = datetime.fromisoformat(_string(doc, key))
    except ValueError as exc:
        raise _missing(key) from exc
    if value.tzinfo is None:
        raise _missing(key)
    return value.astimezone(UTC)
