"""Client queue policy and per-replay subscription groups (fake sockets, no network)."""

from __future__ import annotations

import asyncio
import json
from typing import Any
from uuid import UUID

import pytest
from starlette.websockets import WebSocketState

from app.gateway.connections import (
    CLOSE_GOING_AWAY,
    CLOSE_TOO_SLOW,
    ClientConnection,
    ConnectionManager,
)
from app.gateway.messages import WsMessage, WsMessageType

REPLAY_A, REPLAY_B = UUID(int=1), UUID(int=2)
RUN, OTHER_RUN = UUID(int=10), UUID(int=11)


class FakeSocket:
    def __init__(self, *, gate: asyncio.Event | None = None, fail: bool = False) -> None:
        self.sent: list[dict[str, Any]] = []
        self.close_code: int | None = None
        self.application_state = WebSocketState.CONNECTED
        self._gate = gate
        self._fail = fail

    async def send_text(self, text: str) -> None:
        if self._fail:
            raise OSError("reset")
        if self._gate is not None:
            await self._gate.wait()
        self.sent.append(json.loads(text))

    async def close(self, code: int = 1000) -> None:
        self.close_code = code
        self.application_state = WebSocketState.DISCONNECTED

    @property
    def types(self) -> list[str]:
        return [m["type"] for m in self.sent]


def message(
    type_: WsMessageType,
    *,
    sequence: int | None = None,
    run_id: UUID | None = RUN,
    replay_id: UUID = REPLAY_A,
) -> WsMessage:
    return WsMessage(type=type_, replay_id=replay_id, run_id=run_id, sequence=sequence)


def update(sequence: int, **kwargs: Any) -> WsMessage:
    return message(WsMessageType.DRIVER_UPDATE, sequence=sequence, **kwargs)


def clock() -> WsMessage:
    return message(WsMessageType.REPLAY_CLOCK, run_id=None)


def connect(
    socket: FakeSocket, *, size: int = 4, replay_id: UUID = REPLAY_A, timeout: float = 1
) -> ClientConnection:
    connection = ClientConnection(
        socket,  # type: ignore[arg-type]
        replay_id,
        queue_size=size,
        send_timeout=timeout,
    )
    connection.start()
    return connection


def activate(connection: ClientConnection, after: int | None = None) -> None:
    connection.activate(
        message(WsMessageType.SNAPSHOT, sequence=after), run_id=RUN, after_sequence=after
    )


async def flush() -> None:
    for _ in range(20):
        await asyncio.sleep(0)


async def test_messages_are_delivered_in_order() -> None:
    socket = FakeSocket()
    connection = connect(socket)
    activate(connection)
    connection.offer(update(1))
    connection.offer(update(2))
    await flush()

    assert socket.types == ["SNAPSHOT", "DRIVER_UPDATE", "DRIVER_UPDATE"]
    assert [m["sequence"] for m in socket.sent[1:]] == [1, 2]
    connection.close()
    await connection.wait_closed()


async def test_nothing_is_sent_before_the_snapshot() -> None:
    socket = FakeSocket()
    connection = connect(socket)
    connection.offer(update(1))
    await flush()
    assert socket.sent == [] and connection.queued == 1

    activate(connection)
    await flush()

    assert socket.types == ["SNAPSHOT", "DRIVER_UPDATE"]
    connection.close()
    await connection.wait_closed()


async def test_activation_skips_state_messages_the_snapshot_covers() -> None:
    socket = FakeSocket()
    connection = connect(socket, size=16)
    connection.offer(update(3))  # covered
    connection.offer(update(5))  # covered (equal)
    connection.offer(update(6))  # newer
    connection.offer(update(2, run_id=OTHER_RUN))  # another run, no new start: stale, dropped
    connection.offer(message(WsMessageType.DETECTED_EVENT, sequence=1))  # not state: kept
    connection.offer(message(WsMessageType.REPLAY_STATUS, run_id=None))  # lifecycle: kept
    activate(connection, after=5)
    connection.offer(update(4))  # a lagging tail is filtered after activation as well
    connection.offer(update(7))
    await flush()

    assert [(m["type"], m["sequence"]) for m in socket.sent] == [
        ("SNAPSHOT", 5),
        ("DRIVER_UPDATE", 6),
        ("DETECTED_EVENT", 1),
        ("REPLAY_STATUS", None),
        ("DRIVER_UPDATE", 7),
    ]
    connection.close()
    await connection.wait_closed()


async def test_full_queue_drops_clock_ticks_and_keeps_the_client() -> None:
    gate = asyncio.Event()
    connection = connect(FakeSocket(gate=gate), size=2)
    activate(connection)
    await flush()  # the snapshot is now in flight, blocked in send

    assert connection.offer(clock()) and connection.offer(clock())
    assert connection.offer(clock())  # accepted but dropped
    assert connection.queued == 2 and connection.dropped == 1 and not connection.closed
    gate.set()
    connection.close()
    await connection.wait_closed()


async def test_state_message_evicts_queued_clock_ticks_before_anything_else() -> None:
    gate = asyncio.Event()
    connection = connect(FakeSocket(gate=gate), size=3)
    activate(connection)
    await flush()
    for _ in range(3):
        connection.offer(clock())

    assert connection.offer(update(1))

    assert connection.queued == 1 and connection.dropped == 3 and not connection.closed
    gate.set()
    connection.close()
    await connection.wait_closed()


async def test_state_overflow_disconnects_with_client_too_slow() -> None:
    gate = asyncio.Event()
    socket = FakeSocket(gate=gate)
    connection = connect(socket, size=2)
    activate(connection)
    await flush()
    assert connection.offer(update(1)) and connection.offer(update(2))

    assert connection.offer(update(3)) is False

    assert connection.closed and connection.queued == 1
    assert connection.offer(update(4)) is False  # nothing accepted once dropped
    gate.set()
    await connection.wait_closed()
    assert socket.types == ["SNAPSHOT", "ERROR"]
    assert socket.sent[1]["payload"]["code"] == "CLIENT_TOO_SLOW"
    assert socket.close_code == CLOSE_TOO_SLOW


async def test_send_timeout_marks_the_client_dead() -> None:
    socket = FakeSocket(gate=asyncio.Event())
    connection = connect(socket, timeout=0.02)
    activate(connection)

    await connection.wait_closed()

    assert connection.closed and socket.close_code == CLOSE_TOO_SLOW


async def test_send_failure_closes_the_connection() -> None:
    socket = FakeSocket(fail=True)
    connection = connect(socket)
    activate(connection)

    await connection.wait_closed()

    assert connection.closed and connection.offer(update(1)) is False


async def test_send_now_jumps_the_queue_and_close_flushes_the_final_message() -> None:
    socket = FakeSocket()
    connection = connect(socket, size=8)
    connection.offer(update(1))
    connection.send_now(message(WsMessageType.PONG, run_id=None))
    activate(connection)
    await flush()
    # activation puts the snapshot first; the pending PONG was queued ahead of the update
    assert socket.types == ["SNAPSHOT", "PONG", "DRIVER_UPDATE"]

    connection.close(4404, final=message(WsMessageType.ERROR, run_id=None))
    await connection.wait_closed()

    assert socket.types[-1] == "ERROR" and socket.close_code == 4404


async def test_deactivate_buffers_again_until_the_next_snapshot() -> None:
    socket = FakeSocket()
    connection = connect(socket, size=8)
    activate(connection)
    await flush()
    connection.deactivate()
    connection.offer(update(1))
    connection.offer(update(9))
    await flush()
    assert socket.types == ["SNAPSHOT"]

    activate(connection, after=1)
    await flush()

    assert [(m["type"], m["sequence"]) for m in socket.sent] == [
        ("SNAPSHOT", None),
        ("SNAPSHOT", 1),
        ("DRIVER_UPDATE", 9),
    ]
    connection.close()
    await connection.wait_closed()


# -- manager --------------------------------------------------------------------------------------


async def test_manager_isolates_replay_groups() -> None:
    manager = ConnectionManager()
    socket_a, socket_b = FakeSocket(), FakeSocket()
    in_a = connect(socket_a, replay_id=REPLAY_A)
    in_b = connect(socket_b, replay_id=REPLAY_B)
    for connection in (in_a, in_b):
        manager.register(connection)
        activate(connection)

    manager.broadcast(REPLAY_A, [update(1)])
    manager.broadcast(UUID(int=99), [update(2)])  # nobody subscribed
    await flush()

    assert socket_a.types == ["SNAPSHOT", "DRIVER_UPDATE"] and socket_b.types == ["SNAPSHOT"]
    assert manager.connection_count() == 2 and manager.connection_count(REPLAY_A) == 1
    assert set(manager.replay_ids()) == {REPLAY_A, REPLAY_B}
    assert manager.has_subscribers(REPLAY_A) and not manager.has_subscribers(UUID(int=99))

    manager.unregister(in_a)
    manager.unregister(in_a)  # idempotent
    assert not manager.has_subscribers(REPLAY_A) and manager.connection_count() == 1
    await manager.close_all()
    assert socket_b.close_code == CLOSE_GOING_AWAY and manager.connection_count() == 0


async def test_one_slow_client_does_not_block_the_broadcast() -> None:
    manager = ConnectionManager()
    gate = asyncio.Event()
    slow_socket, fast_socket = FakeSocket(gate=gate), FakeSocket()
    slow, fast = connect(slow_socket, size=2), connect(fast_socket, size=2)
    for connection in (slow, fast):
        manager.register(connection)
        activate(connection)
    await flush()

    for sequence in range(1, 6):
        manager.broadcast(REPLAY_A, [update(sequence)])
        await flush()

    assert slow.closed and not fast.closed
    assert [m["sequence"] for m in fast_socket.sent[1:]] == [1, 2, 3, 4, 5]
    gate.set()
    await manager.close_all()


@pytest.mark.parametrize("size", [1, 3])
async def test_queue_never_exceeds_its_bound(size: int) -> None:
    gate = asyncio.Event()
    connection = connect(FakeSocket(gate=gate), size=size)
    activate(connection)
    await flush()

    for sequence in range(20):
        connection.offer(update(sequence))
        connection.offer(clock())
        assert connection.queued <= size
    gate.set()
    await connection.wait_closed()


# -- stale runs and control replies ---------------------------------------------------------------


def full_state(run_id: UUID, reason: str) -> WsMessage:
    return WsMessage(
        type=WsMessageType.RACE_STATE_SNAPSHOT,
        replay_id=REPLAY_A,
        run_id=run_id,
        sequence=0,
        payload={"reason": reason},
    )


def detection(run_id: UUID) -> WsMessage:
    return message(WsMessageType.DETECTED_EVENT, sequence=4, run_id=run_id)


def connect_following(
    socket: FakeSocket, current: list[UUID | None], *, size: int = 16
) -> ClientConnection:
    """A connection whose replay's current run is ``current[0]`` (changes with the list)."""
    connection = ClientConnection(
        socket,  # type: ignore[arg-type]
        REPLAY_A,
        queue_size=size,
        send_timeout=1,
        current_run=lambda _: current[0],
    )
    connection.start()
    return connection


def activate_run(connection: ClientConnection, run_id: UUID | None, after: int | None) -> None:
    connection.activate(
        message(WsMessageType.SNAPSHOT, sequence=after, run_id=run_id),
        run_id=run_id,
        after_sequence=after,
    )


async def test_stale_old_run_entries_including_detections_are_dropped() -> None:
    socket = FakeSocket()
    current: list[UUID | None] = [RUN]
    connection = connect_following(socket, current)
    # buffered while pending: leftovers of the previous run
    connection.offer(full_state(OTHER_RUN, "STATE_INITIALIZED"))
    connection.offer(update(1, run_id=OTHER_RUN))
    connection.offer(detection(OTHER_RUN))
    activate_run(connection, RUN, after=1)
    connection.offer(update(2, run_id=OTHER_RUN))
    connection.offer(full_state(OTHER_RUN, "STATE_REBUILT"))
    connection.offer(detection(OTHER_RUN))
    connection.offer(update(3))
    connection.offer(detection(RUN))
    await flush()

    assert [(m["type"], m["run_id"]) for m in socket.sent] == [
        ("SNAPSHOT", str(RUN)),
        ("DRIVER_UPDATE", str(RUN)),
        ("DETECTED_EVENT", str(RUN)),
    ]
    connection.close()
    await connection.wait_closed()


async def test_a_new_run_is_adopted_however_it_is_announced() -> None:
    socket = FakeSocket()
    current: list[UUID | None] = [RUN]
    connection = connect_following(socket, current)
    activate_run(connection, RUN, after=1)
    connection.offer(update(2))
    current[0] = OTHER_RUN  # the replay service restarted it
    connection.offer(update(3))  # the old run is stale now
    connection.offer(full_state(OTHER_RUN, "STATE_REBUILT"))  # sequence 0 was missed
    connection.offer(update(1, run_id=OTHER_RUN))
    third = UUID(int=12)
    current[0] = third  # a second quick restart
    connection.offer(update(2, run_id=OTHER_RUN))
    connection.offer(full_state(third, "STATE_INITIALIZED"))
    await flush()

    assert [(m["type"], m["sequence"], m["run_id"]) for m in socket.sent[1:]] == [
        ("DRIVER_UPDATE", 2, str(RUN)),
        ("RACE_STATE_SNAPSHOT", 0, str(OTHER_RUN)),
        ("DRIVER_UPDATE", 1, str(OTHER_RUN)),
        ("RACE_STATE_SNAPSHOT", 0, str(third)),
    ]
    connection.close()
    await connection.wait_closed()


async def test_without_a_run_in_this_process_the_snapshot_run_is_followed() -> None:
    socket = FakeSocket()
    connection = connect_following(socket, [None])  # e.g. state served from PostgreSQL
    activate_run(connection, RUN, after=5)
    connection.offer(update(4))  # covered
    connection.offer(update(6))
    connection.offer(update(7, run_id=OTHER_RUN))
    connection.offer(detection(OTHER_RUN))
    await flush()

    assert [(m["type"], m["sequence"]) for m in socket.sent] == [
        ("SNAPSHOT", 5),
        ("DRIVER_UPDATE", 6),
    ]
    connection.close()
    await connection.wait_closed()


async def test_connected_before_the_replay_ran_adopts_its_first_run() -> None:
    socket = FakeSocket()
    current: list[UUID | None] = [None]
    connection = connect_following(socket, current)
    activate_run(connection, None, after=None)  # snapshot without state
    connection.offer(update(1))  # nothing to follow yet
    current[0] = RUN
    connection.offer(full_state(RUN, "STATE_INITIALIZED"))
    connection.offer(update(1))
    await flush()

    assert [(m["type"], m["sequence"]) for m in socket.sent] == [
        ("SNAPSHOT", None),
        ("RACE_STATE_SNAPSHOT", 0),
        ("DRIVER_UPDATE", 1),
    ]
    connection.close()
    await connection.wait_closed()


async def test_resync_reevaluates_against_the_current_run() -> None:
    socket = FakeSocket()
    current: list[UUID | None] = [OTHER_RUN]
    connection = connect_following(socket, current)
    activate_run(connection, OTHER_RUN, after=3)
    await flush()
    connection.deactivate()
    connection.offer(update(9, run_id=OTHER_RUN))  # buffered during the resync
    connection.offer(update(8))
    current[0] = RUN
    activate_run(connection, RUN, after=5)
    await flush()

    assert [(m["type"], m["sequence"], m["run_id"]) for m in socket.sent[1:]] == [
        ("SNAPSHOT", 5, str(RUN)),
        ("DRIVER_UPDATE", 8, str(RUN)),
    ]
    connection.close()
    await connection.wait_closed()


async def test_control_replies_are_bounded() -> None:
    gate = asyncio.Event()
    connection = connect(FakeSocket(gate=gate), size=64)
    activate(connection)
    await flush()

    for _ in range(20_000):
        connection.send_now(message(WsMessageType.PONG, run_id=None))
        connection.send_now(message(WsMessageType.ERROR, run_id=None))

    assert connection.queued <= 8 and not connection.closed
    gate.set()
    connection.close()
    await connection.wait_closed()
