"""Envelope encoding, decoding and versioning."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from uuid import uuid4

import pytest

from app.domain.enums import EventType
from app.streaming.envelope import (
    STREAM_EVENT_SCHEMA_VERSION,
    StreamEvent,
    derive_event_id,
)
from app.streaming.errors import (
    MalformedEventError,
    StreamSerializationError,
    UnsupportedSchemaVersionError,
)
from tests.streaming.conftest import make_event


class Colour(Enum):
    RED = "red"


def test_roundtrip_preserves_all_fields() -> None:
    event = make_event(7, payload={"a": [1, 2, {"b": None}], "text": "ünï"})
    envelope = StreamEvent.from_replay_event(event)
    decoded = StreamEvent.from_fields(envelope.to_fields())
    assert decoded == envelope
    assert decoded.event_type == "LAP_COMPLETED"
    assert decoded.schema_version == STREAM_EVENT_SCHEMA_VERSION


def test_fields_have_headers_and_compact_sorted_data() -> None:
    fields = StreamEvent.from_replay_event(make_event(3)).to_fields()
    assert set(fields) == {
        "event_id",
        "schema_version",
        "event_type",
        "replay_id",
        "run_id",
        "sequence",
        "data",
    }
    assert fields["sequence"] == "3" and fields["schema_version"] == "1"
    assert " " not in fields["data"].replace("ünï", "")
    keys = list(json.loads(fields["data"]))
    assert keys == sorted(keys)


def test_special_types_are_serialized_explicitly() -> None:
    ident = uuid4()
    when = datetime(2024, 3, 2, 15, 0, tzinfo=timezone(timedelta(hours=2)))
    payload = {
        "id": ident,
        "when": when,
        "amount": Decimal("1.50"),
        "colour": Colour.RED,
        "kind": EventType.PIT_ENTRY,
        "none": None,
        "nested": {"ids": [ident]},
    }
    decoded = StreamEvent.from_fields(
        StreamEvent.from_replay_event(make_event(0, payload=payload)).to_fields()
    )
    assert decoded.payload == {
        "id": str(ident),
        "when": "2024-03-02T13:00:00+00:00",
        "amount": "1.50",
        "colour": "red",
        "kind": "PIT_ENTRY",
        "none": None,
        "nested": {"ids": [str(ident)]},
    }


@pytest.mark.parametrize(
    "payload",
    [
        {"x": object()},
        {"x": {1, 2}},
        {"x": datetime(2024, 1, 1)},  # naive
        {"x": float("nan")},
    ],
)
def test_unserializable_payload_raises(payload: dict) -> None:
    with pytest.raises(StreamSerializationError):
        StreamEvent.from_replay_event(make_event(0, payload=payload)).to_fields()


def test_event_id_is_deterministic_per_run_and_sequence() -> None:
    run = uuid4()
    assert derive_event_id(run, 5) == derive_event_id(run, 5)
    assert derive_event_id(run, 5) != derive_event_id(run, 6)
    assert derive_event_id(run, 5) != derive_event_id(uuid4(), 5)
    first = StreamEvent.from_replay_event(make_event(5, run_id=run))
    again = StreamEvent.from_replay_event(make_event(5, run_id=run))
    assert first.event_id == again.event_id


def _data(**overrides: object) -> dict[str, str]:
    document = json.loads(StreamEvent.from_replay_event(make_event(1)).to_fields()["data"])
    document.update(overrides)
    return {"data": json.dumps({k: v for k, v in document.items() if v is not ...})}


@pytest.mark.parametrize(
    "fields",
    [
        {},
        {"event_id": "x"},
        {"data": "{not json"},
        {"data": "[1, 2]"},
        _data(schema_version="1"),
        _data(schema_version=True),
        _data(event_id="not-a-uuid"),
        _data(replay_id=...),
        _data(sequence="3"),
        _data(sequence=True),
        _data(payload=[1]),
        _data(published_at="2024-01-01T00:00:00"),  # naive
        _data(published_at="yesterday"),
        _data(lap_number="3"),
        _data(event_type=""),
    ],
)
def test_malformed_entries_raise_malformed_error(fields: dict[str, str]) -> None:
    with pytest.raises(MalformedEventError):
        StreamEvent.from_fields(fields)


def test_unsupported_version_raises_specific_error() -> None:
    with pytest.raises(UnsupportedSchemaVersionError) as info:
        StreamEvent.from_fields(_data(schema_version=2, payload="new shape"))
    assert info.value.version == 2
    assert (
        StreamEvent.from_fields(
            _data(schema_version=2), supported_versions=frozenset({1, 2})
        ).schema_version
        == 2
    )


def test_unknown_extra_fields_are_ignored() -> None:
    decoded = StreamEvent.from_fields(_data(future_field="ok"))
    assert decoded.sequence == 1


def test_error_messages_do_not_leak_payload() -> None:
    secret = "SECRET-PAYLOAD"
    with pytest.raises(MalformedEventError) as info:
        StreamEvent.from_fields(_data(event_id="bad", payload={"s": secret}))
    assert secret not in str(info.value)
