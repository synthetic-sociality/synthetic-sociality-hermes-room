"""Hermes 1.0.115: contract P2c-3f, request discipline per membership.

The universal rule is the contract paragraph P2c-3f, byte-identical in OpenClaw;
the shared case names are ``conformance.json`` ``parityP2c3fCases`` (RDC-01 to
RDC-08), the same eight strings in both connectors. The finding (L1 server
lane, rc2-room-heartbeat-capacity): the Room allows 120 general requests per
membership per 60 s, and Hermes read the Room state again on many paths per
received event, replayed a queued turn's request every 750 ms while it waited
for the grant, and sent ``preparing_response`` on every host call.

Hermes waits for a grant in one place only, ``_wait_for_turn`` (the legacy
direct turn of ``send``; a discussion-cycle attempt is claimed, not waited
for). Its grant-state read is the idempotent replay of the turn request, the
only read that tells a queued turn from a revoked, expired or replaced one.

Every test runs a real ``SyntheticSocialityAdapter`` on a real state file and a
real ``LocalStatus`` in a throwaway directory (the P2c-3e ``Installation``).
The Room is a fake per membership that records every call with its fake time,
and time is a fake clock: ``asyncio.sleep`` and the adapter's ``_clock``
follow it. No assertion depends on wall-clock sleeps.
"""

from __future__ import annotations

import asyncio
import hashlib
import heapq
import os
import sys
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path

from test_delivery_lifecycle import ROOT, adapter
from test_recovery_budget import CONFORMANCE, CONTRACT, INTERVAL, Installation, Room, network_failure, rate_limited

REQUEST_DISCIPLINE_CASES = [
    "RDC-01 an event-read grant ends the wait immediately with zero extra grant-state reads",
    "RDC-02 fallback state-read waits are at least 2, 4, 8, 10, 10 seconds; early reads fail the case",
    "RDC-03 no state read starts at or after the lease deadline, including a lease shorter than 2 seconds and a next wait crossing the deadline",
    "RDC-04 one multi-event batch performs at most one state read; another batch may read again; another membership never shares its answer",
    "RDC-05 preparing_response allows the first send and a send at 5 seconds, refuses another before 5 seconds in a rolling window, and keeps different turns and memberships independent",
    "RDC-06 pending grant/debounce waits do not delay heartbeat, post, acknowledgement or stop; stop cancels the wait and causes no later read/activity",
    "RDC-07 a Room-stopped membership makes no grant-state read or preparing_response and retains its stop reason; sibling memberships continue",
    "RDC-08 a failed contact or Retry-After cannot create a tight poll or shorten the minimum wait; an event grant still ends waiting",
]
PARAGRAPH = """\
P2c-3f Request discipline (2026-11-18). A connector keeps each (Room, membership)'s ordinary Room calls well inside
that membership's request allowance. While it waits for a requested turn to be granted, it learns the grant from the
event read it already runs, without an additional state read for that grant. Where a Room state read is required instead,
the first read starts no earlier than 2 seconds after the turn request; after each successful read that shows no grant,
the minimum wait doubles to 4, 8 and then 10 seconds, remaining at 10 seconds. No such read starts at or after the
turn's lease deadline; if the next eligible read would reach or pass that deadline, the connector stops waiting at the
deadline without a final read. A grant received through the existing event read ends this wait immediately. Failed
contacts retain P2c-3a handling, including Retry-After, and never shorten these minimum waits. For one received event
batch of one (Room, membership), the connector reads the Room state at most once and reuses that answer only within
that batch and membership; it never treats a cached answer as fresh authority for a post or a lease renewal. It sends
at most one `preparing_response` activity per (Room, membership, turn) in any rolling 5 seconds. These bounds never
delay a heartbeat (P2c-3e), a post, an acknowledgement or a stop (P2c-3d); their existing authority, retry and stop rules
still apply. A membership stopped by the Room starts none of these waits or activities until its existing renewal or
re-admission rules permit it.
"""
PARAGRAPH_SHA256 = "d63fe58ed37d91726ffc66dbc2bb98fe4e1fdc83bd43eeaed24016f26d51acb9"  # Addendum 1, final
START = 1000.0  # the fake clock's first instant
ALLOWANCE = 60  # the measured target: half the Room's 120 general requests per membership per 60 s


class DisciplineRoom(Room):
    """The P2c-3e fake Room, answering the receive and turn paths too.

    ``calls`` holds every general-bucket call (fake time, kind); heartbeats
    and registrations stay in ``attempts``. A queued turn stays queued until
    ``grant_at`` (never by default) and expires ``lease`` seconds after its
    request when it carries one.
    """

    def __init__(self, clock, membership_id: str) -> None:
        super().__init__(clock, membership_id)
        self.calls: list[tuple[float, str]] = []
        self.pages: list[list[dict]] = []
        self.turn_failures: list = []
        self.turn_lease: float | None = None
        self.requested: dict[str, float] = {}
        self.epoch = f"epoch-{membership_id}"

    def record(self, kind: str) -> None:
        self.calls.append((self.clock.now, kind))

    def at(self, kind: str) -> list[float]:
        return [at for at, recorded in self.calls if recorded == kind]

    def general(self, start: float, end: float) -> list[tuple[float, str]]:
        return [(at, kind) for at, kind in self.calls if start <= at < end]

    def room_state(self, _room):
        self.record("state")
        return {"headSeq": 0, "roster": [{"membershipId": self.membership_id, "status": "active",
                                          "role": "participant_agent", "displayName": self.membership_id}],
                "activeEpoch": {"id": self.epoch, "startsAtSeq": 1}}

    def room_policy(self, _room):
        self.record("policy")
        return {"coordinationMode": "coordinated"}

    def events(self, _room, _after, _wait=25, *, all_epochs=False):
        self.record("events")
        return {"events": self.pages.pop(0) if self.pages else []}

    def acknowledge(self, _room, seq):
        self.record("ack")
        return {"acknowledgedSeq": seq}

    def acknowledge_peer_contribution(self, _room, _source):
        self.record("peer-ack")
        return {"accepted": True}

    def activity(self, _room, payload):
        self.record(f"activity:{payload.get('status') or payload.get('kind')}")
        return {"accepted": True}

    def post_message(self, *_args, **_kwargs):
        self.record("post")
        return {"id": "posted-1", "seq": 99}

    def request_turn(self, _room, _observed, source_id, **_kwargs):
        """The first call requests the turn; every later one is the grant-state read."""
        turn_id = f"turn-{source_id}"
        if source_id not in self.requested:
            self.requested[source_id] = self.clock.now
            self.record("turn-request")
        else:
            self.record("grant-read")
            if self.turn_failures:
                raise self.turn_failures.pop(0)
        state = "queued"
        if self.turn_lease is not None and self.clock.now >= self.requested[source_id] + self.turn_lease:
            state = "expired"
        return {"turnId": turn_id, "state": state}


def room_id_of(index: int) -> str:
    return f"room-{index}"


class Gateway(Installation):
    """The P2c-3e installation with ``DisciplineRoom`` fakes."""

    def __init__(self, directory: str, count: int) -> None:
        super().__init__(directory, count)
        self.rooms = {room_id: DisciplineRoom(self.clock, room.membership_id) for room_id, room in self.rooms.items()}
        self.activities: list[tuple[float, str, str, str]] = []  # (fake time, room, source, status)
        self.seqs: dict[str, int] = {}
        self.waits: list[asyncio.Task] = []
        real_publish = adapter.SyntheticSocialityAdapter._publish

        async def publish(binding, source_id, kind, **options):
            self.activities.append((self.clock.now, binding.room_id, source_id, options.get("status") or kind))
            await real_publish(self.instance, binding, source_id, kind, **options)

        self.instance._publish = publish

    def wait(self, index: int, source: str = "source-1", *, timeout: float = 120.0, lease: float | None = None,
             nested: bool = False):
        """``send``'s grant wait for a turn this membership just requested and the Room queued.

        The turn record carries the server's ``turnPayload`` keys; its lease is
        ``expiresAt``. ``nested`` wraps it as ``{"turn": …}``, the event payload's shape.
        """
        turn = {"turnId": f"turn-{source}", "state": "queued", "holderMembershipId": self.binding(index).membership_id,
                "topicId": "topic-1"}
        if lease is not None:
            turn["expiresAt"] = datetime.fromtimestamp(time.time() + lease, timezone.utc).isoformat()
        if nested:
            turn = {"turn": turn}
        room = self.room(index)
        room.requested.setdefault(source, self.clock.now)
        task = asyncio.create_task(self.instance._wait_for_turn(self.binding(index), turn, 1, source, timeout))
        self.waits.append(task)
        return task

    async def deliver(self, index: int, *events: dict) -> None:
        """One long-poll page of the existing event read, carrying ``events``."""
        self.room(index).pages.append(list(events))
        await self.instance._long_poll_once(self.binding(index))
        await self.clock.settle()

    def next_seq(self, index: int = 1) -> int:
        """The next canonical sequence of membership ``index``'s Room (its cursor starts at 0)."""
        key = room_id_of(index)
        self.seqs[key] = self.seqs.get(key, 0) + 1
        return self.seqs[key]

    def typing(self, index: int, source: str = "source-1"):
        return self.instance.send_typing(f"room-{index}", {"reply_to": source})


def peer_post(gateway: Gateway, index: int = 1, *, to: str | None = "member-8", member: str = "member-9") -> dict:
    """A general post of another agent in membership ``index``'s Room, to another agent or (``to=None``) to everyone."""
    seq = gateway.next_seq(index)
    recipients = [to] if to else []
    return {"id": f"peer-{seq}", "seq": seq, "type": "message.posted", "actorId": member,
            "actorRole": "participant_agent", "payload": {"body": "my view", "resolvedRecipientMembershipIds": recipients}}


def grant(gateway: Gateway, membership: str, source: str = "source-1", *, flat: bool = False) -> dict:
    """The Room's turn grant on the existing event read, in the server's exact shape.

    Room server bc9ab0f: ``turn.granted`` (server/domain/event.go:42) with the
    payload of ``turnPayload`` (turnservice/service.go:815-831):
    ``{"turn": {"turnId", "state", "holderMembershipId", "topicId", "grantedAtSeq"?, "expiresAt"?}, "reason"?}``.
    ``flat`` is the older flat shape, tolerated as a fallback only.
    """
    seq = gateway.next_seq()
    turn = {"turnId": f"turn-{source}", "state": "granted", "holderMembershipId": membership, "topicId": "topic-1",
            "grantedAtSeq": seq,
            "expiresAt": datetime.fromtimestamp(time.time() + 120, timezone.utc).isoformat()}
    payload = turn if flat else {"turn": turn, "reason": "queue_advanced"}
    return {"id": f"turn-{seq}", "seq": seq, "type": "turn.granted", "payload": payload}


def ended(task: asyncio.Task):
    """The wait's outcome: its grant, or its error code."""
    if not task.done():
        return "pending"
    error = task.exception()
    if error is None:
        return task.result().get("state")
    return getattr(error, "code", "") or type(error).__name__


def report(label: str, **values) -> None:
    """The request counts for the HANDOVER (stderr, only when asked for)."""
    if os.environ.get("RDC_REPORT"):
        print(f"RDC {label}: " + ", ".join(f"{key}={value}" for key, value in values.items()), file=sys.stderr)


def run(gateway: Gateway, body) -> None:
    async def main():
        try:
            await body()
        finally:
            for task in gateway.waits:
                task.cancel()
            await asyncio.gather(*gateway.waits, return_exceptions=True)
            await gateway.stop()

    with gateway.running():
        asyncio.run(main())


class RequestDisciplineConformanceTests(unittest.TestCase):
    def test_the_paragraph_follows_p2c_3e_byte_identical(self):
        self.assertEqual(hashlib.sha256(PARAGRAPH.encode("utf-8")).hexdigest(), PARAGRAPH_SHA256)
        lines = CONTRACT.split("\n")
        start = next(index for index, line in enumerate(lines) if line.startswith("P2c-3e Recovery budget "))
        end = next(index for index in range(start, len(lines)) if not lines[index])
        self.assertTrue(lines[end - 1].endswith("membership stopped by the Room (P2c-3d) does not use it."))
        block = PARAGRAPH.split("\n")[:-1]
        self.assertEqual(lines[end + 1:end + 1 + len(block)], block)
        self.assertEqual(lines[end + 1 + len(block)], "")
        self.assertEqual(lines[end + 2 + len(block)], "<<<P2c-3h")
        self.assertEqual(CONTRACT.count("P2c-3f Request discipline"), 1)

    def test_the_case_names_are_the_shared_ones_and_each_has_a_test(self):
        self.assertEqual(CONFORMANCE["parityP2c3fCases"], REQUEST_DISCIPLINE_CASES)
        source = Path(__file__).read_text(encoding="utf-8")
        for index, name in enumerate(REQUEST_DISCIPLINE_CASES, start=1):
            self.assertTrue(name.startswith(f"RDC-{index:02d} "), name)
            self.assertGreaterEqual(source.count(f"    def test_rdc_{index:02d}_"), 1, name)

    def test_the_contract_hash_is_the_pinned_one(self):
        digest = hashlib.sha256((ROOT / "docs" / "delivery-lifecycle-contract-v1.md").read_bytes()).hexdigest()
        self.assertEqual(CONFORMANCE["contractSha256"], digest)

    def test_the_bounds_are_the_contract_ones(self):
        self.assertEqual((adapter.TURN_GRANT_FIRST_READ_SECONDS, adapter.TURN_GRANT_MAX_READ_SECONDS,
                          adapter.PREPARING_RESPONSE_INTERVAL_SECONDS), (2.0, 10.0, 5.0))


class RequestDisciplineTests(unittest.TestCase):
    def test_rdc_01_an_event_read_grant_ends_the_wait_at_once_with_no_grant_state_read(self):
        # The server's nested payload.turn first; the flat shape is a tolerated fallback.
        for flat in (False, True):
            with self.subTest(payload="flat fallback" if flat else "server payload.turn"), \
                    tempfile.TemporaryDirectory() as directory:
                gateway = Gateway(directory, 1)
                seen = {}

                async def body():
                    waiting = gateway.wait(1)
                    await gateway.clock.advance(1.0)
                    await gateway.deliver(1, grant(gateway, "member-1", flat=flat))
                    seen["at"], seen["outcome"] = gateway.clock.now, ended(waiting)
                    seen["turn"] = waiting.result() if seen["outcome"] == "granted" else {}

                run(gateway, body)
                self.assertEqual((seen["at"], seen["outcome"]), (START + 1.0, "granted"))
                self.assertEqual(gateway.room(1).at("grant-read"), [], "the grant came from the event read")
                self.assertEqual((seen["turn"].get("turnId"), seen["turn"].get("topicId")), ("turn-source-1", "topic-1"),
                                 "the wait returns the granted turn record")

    def test_rdc_01_turn_changed_is_not_a_server_event_and_ends_no_wait(self):
        # The Room server emits no turn.changed; an event of that name is ignored.
        with tempfile.TemporaryDirectory() as directory:
            gateway = Gateway(directory, 1)
            seen = {}

            async def body():
                waiting = gateway.wait(1)
                await gateway.clock.advance(1.0)
                seq = gateway.next_seq()
                await gateway.deliver(1, {"id": f"turn-{seq}", "seq": seq, "type": "turn.changed", "payload": {
                    "activeTurn": {"turnId": "turn-source-1", "holderMembershipId": "member-1"}}})
                seen["outcome"] = ended(waiting)

            run(gateway, body)
            self.assertEqual(seen["outcome"], "pending")

    def test_rdc_01_a_grant_for_another_membership_or_turn_does_not_end_the_wait(self):
        with tempfile.TemporaryDirectory() as directory:
            gateway = Gateway(directory, 1)
            seen = {}

            async def body():
                waiting = gateway.wait(1)
                await gateway.clock.advance(1.0)
                await gateway.deliver(1, grant(gateway, "member-2"), grant(gateway, "member-1", source="source-other"))
                seen["outcome"] = ended(waiting)

            run(gateway, body)
            self.assertEqual(seen["outcome"], "pending")

    def test_rdc_02_fallback_reads_wait_at_least_2_4_8_10_10_seconds(self):
        with tempfile.TemporaryDirectory() as directory:
            gateway = Gateway(directory, 1)

            async def body():
                gateway.wait(1)
                await gateway.clock.advance(35.0)

            run(gateway, body)
            reads = gateway.room(1).at("grant-read")
            self.assertEqual(reads, [START + 2, START + 6, START + 14, START + 24, START + 34])
            waits = [later - earlier for earlier, later in zip([START, *reads], reads)]
            for wait, minimum in zip(waits, (2, 4, 8, 10, 10)):
                self.assertGreaterEqual(wait, minimum, f"an early read: waits {waits}")

    def test_rdc_03_no_read_starts_at_or_after_the_lease_deadline(self):
        # (lease seconds, the reads it allows): a lease under 2 s allows none; a
        # 6 s lease allows the 2 s read, and the next (at 6 s) would reach the
        # deadline; a 10 s lease allows 2 s and 6 s, the next (14 s) crosses it.
        cases = [(lease, expected, nested) for nested in (False, True)
                 for lease, expected in ((1.0, []), (1.9, []), (6.0, [2.0]), (10.0, [2.0, 6.0]))]
        for lease, expected, nested in cases:
            with self.subTest(lease=lease, turn="nested turn" if nested else "turn record"), \
                    tempfile.TemporaryDirectory() as directory:
                gateway = Gateway(directory, 1)
                seen = {}

                async def body():
                    waiting = gateway.wait(1, lease=lease, nested=nested)
                    await gateway.clock.advance(lease - 0.01)
                    seen["before"] = ended(waiting)
                    await gateway.clock.advance(0.02)
                    seen["after"] = ended(waiting)
                    await gateway.clock.advance(30.0)

                run(gateway, body)
                reads = [at - START for at in gateway.room(1).at("grant-read")]
                self.assertEqual(reads, expected)
                self.assertTrue(all(at < lease for at in reads))
                self.assertEqual(seen["before"], "pending", "the wait lasts to the deadline")
                self.assertNotEqual(seen["after"], "pending", "the wait ends at the deadline, without a final read")

    def test_rdc_03_a_wake_serviced_after_the_deadline_starts_no_read(self):
        # Paula's RD-H-03-01: a busy event loop services the 2 s wake of a turn
        # with a 6 s lease only at 7 s, after the deadline: no read may start
        # then. Serviced at 3 s (the control), the eligible read happens at 3 s.
        for lag, expected in ((7.0, []), (3.0, [3.0])):
            with self.subTest(serviced_at=lag), tempfile.TemporaryDirectory() as directory:
                gateway = Gateway(directory, 1)
                seen = {}

                async def body():
                    waiting = gateway.wait(1, lease=6.0)
                    await gateway.clock.settle()
                    clock = gateway.clock
                    clock.now = START + lag  # the timer fires late, all at once
                    while clock.sleepers and clock.sleepers[0][0] <= clock.now:
                        _wake, _order, future = heapq.heappop(clock.sleepers)
                        if not future.done():
                            future.set_result(None)
                    await clock.settle()
                    seen["outcome"] = ended(waiting)

                run(gateway, body)
                self.assertEqual([at - START for at in gateway.room(1).at("grant-read")], expected)
                if lag >= 6.0:
                    self.assertNotEqual(seen["outcome"], "pending", "the wait ends at the deadline without a read")
                else:
                    self.assertEqual(seen["outcome"], "pending")

    def test_rdc_03_the_wait_without_a_lease_ends_at_its_timeout_without_a_final_read(self):
        with tempfile.TemporaryDirectory() as directory:
            gateway = Gateway(directory, 1)

            async def body():
                gateway.wait(1, timeout=15.0)
                await gateway.clock.advance(40.0)

            run(gateway, body)
            self.assertEqual([at - START for at in gateway.room(1).at("grant-read")], [2.0, 6.0, 14.0])

    def test_rdc_04_one_batch_reads_the_state_once_another_batch_again_and_memberships_never_share(self):
        with tempfile.TemporaryDirectory() as directory:
            gateway = Gateway(directory, 2)
            counts = {}

            async def body():
                await gateway.deliver(1, peer_post(gateway), peer_post(gateway), peer_post(gateway))
                counts["first"] = len(gateway.room(1).at("state"))
                counts["first acks"] = len(gateway.room(1).at("ack"))
                await gateway.deliver(2, peer_post(gateway, 2), peer_post(gateway, 2))
                counts["other membership"] = len(gateway.room(2).at("state"))
                await gateway.deliver(1, peer_post(gateway), peer_post(gateway))
                counts["second"] = len(gateway.room(1).at("state"))

            run(gateway, body)
            self.assertEqual(counts["first acks"], 3, "every event of the batch was processed and acknowledged")
            self.assertEqual(counts["first"], 1, "one batch of three events, one state read")
            self.assertEqual(counts["other membership"], 1, "another membership reads its own state")
            self.assertEqual(counts["second"], 2, "the next batch reads again")

    def test_rdc_04_events_received_together_on_the_stream_are_one_batch(self):
        with tempfile.TemporaryDirectory() as directory:
            gateway = Gateway(directory, 1)
            room = gateway.room(1)
            events = [peer_post(gateway), peer_post(gateway), peer_post(gateway)]

            def stream_events(_room, _after, on_event):
                for event in events:
                    on_event(event)
                return {"cursor": events[-1]["seq"]}

            room.stream_events = stream_events

            async def stream_call(binding, operation):
                return operation(gateway.rooms[binding.room_id])

            gateway.instance._stream_call = stream_call

            async def body():
                await gateway.instance._stream_once(gateway.binding(1))
                await gateway.clock.settle()

            run(gateway, body)
            self.assertEqual(len(room.at("ack")), 3)
            self.assertEqual(len(room.at("state")), 1)

    def test_rdc_04_a_state_changing_call_of_the_membership_ends_the_reuse(self):
        with tempfile.TemporaryDirectory() as directory:
            gateway = Gateway(directory, 1)
            binding = gateway.binding(1)
            seen = {}

            async def body():
                token = gateway.instance._begin_receive_batch(binding)
                await gateway.instance._batch_room_state(binding)
                await gateway.instance._batch_room_state(binding)
                seen["reused"] = len(gateway.room(1).at("state"))
                await adapter._changing_call(gateway.instance, binding, lambda api: api.post_message())
                await gateway.instance._batch_room_state(binding)
                seen["after change"] = len(gateway.room(1).at("state"))
                gateway.instance._end_receive_batch(binding, token)
                await gateway.instance._batch_room_state(binding)
                seen["outside"] = len(gateway.room(1).at("state"))

            run(gateway, body)
            self.assertEqual((seen["reused"], seen["after change"], seen["outside"]), (1, 2, 3))

    def test_rdc_05_preparing_response_once_per_turn_in_any_rolling_5_seconds(self):
        with tempfile.TemporaryDirectory() as directory:
            gateway = Gateway(directory, 2)

            async def body():
                for at in (0.0, 2.0, 4.9, 5.0, 7.0, 9.9, 10.0):
                    await gateway.clock.advance(at - (gateway.clock.now - START))
                    await gateway.typing(1)
                    if at == 2.0:
                        await gateway.typing(1, "source-2")  # another turn of the same membership
                        await gateway.typing(2)  # the same source id in another membership

            run(gateway, body)
            sent = [(at - START, room, source) for at, room, source, status in gateway.activities
                    if status == "preparing_response"]
            self.assertEqual(sent, [(0.0, "room-1", "source-1"), (2.0, "room-1", "source-2"),
                                    (2.0, "room-2", "source-1"), (5.0, "room-1", "source-1"),
                                    (10.0, "room-1", "source-1")])
            self.assertEqual(len(gateway.room(1).at("activity:preparing_response")), 4)

    def test_rdc_06_waits_delay_no_heartbeat_post_or_acknowledgement_and_a_stop_ends_them(self):
        with tempfile.TemporaryDirectory() as directory:
            gateway = Gateway(directory, 1)
            room = gateway.room(1)
            seen = {}

            async def body():
                gateway.beat(1)
                waiting = gateway.wait(1)
                await gateway.typing(1)
                await gateway.clock.advance(1.0)
                await gateway.typing(1)  # debounced: it waits for nothing and holds nothing up
                await gateway.deliver(1, peer_post(gateway))  # an acknowledgement at 1001
                await gateway.instance._call(gateway.binding(1), lambda api: api.post_message())  # a post at 1001
                await gateway.clock.advance(44.0)
                seen["pending"] = ended(waiting)
                gateway.instance._stop.set()
                await gateway.clock.settle()
                seen["stopped at"], seen["outcome"] = gateway.clock.now, ended(waiting)
                await gateway.clock.advance(60.0)

            run(gateway, body)
            self.assertEqual(room.heartbeats()[:2], [(START + INTERVAL, True), (START + 2 * INTERVAL, True)])
            self.assertEqual((room.at("ack"), room.at("post")), ([START + 1.0], [START + 1.0]))
            self.assertEqual(seen["pending"], "pending")
            self.assertEqual((seen["stopped at"], seen["outcome"]), (START + 45.0, "connector_stopped"))
            later = [(at, kind) for at, kind in room.calls if at > START + 45.0 and kind != "heartbeat"]
            self.assertEqual(later, [], "no read or activity after the stop")

    def test_rdc_06_a_room_stop_ends_the_wait_at_once(self):
        with tempfile.TemporaryDirectory() as directory:
            gateway = Gateway(directory, 1)
            seen = {}

            async def body():
                waiting = gateway.wait(1)
                await gateway.clock.advance(3.0)
                gateway.instance._revoke(gateway.binding(1))
                await gateway.clock.settle()
                seen["outcome"] = ended(waiting)
                await gateway.clock.advance(60.0)

            run(gateway, body)
            self.assertEqual(seen["outcome"], "credential_revoked")
            self.assertEqual(gateway.room(1).at("grant-read"), [START + 2.0])

    def test_rdc_07_a_room_stopped_membership_reads_nothing_and_sends_no_activity_its_sibling_continues(self):
        with tempfile.TemporaryDirectory() as directory:
            gateway = Gateway(directory, 2)
            seen = {}

            async def body():
                gateway.instance._expire(gateway.binding(1))
                stopped = gateway.wait(1)
                sibling = gateway.wait(2)
                await gateway.typing(1)
                await gateway.typing(2)
                await gateway.clock.advance(7.0)
                await gateway.typing(1)
                seen["stopped"], seen["sibling"] = ended(stopped), ended(sibling)

            run(gateway, body)
            self.assertEqual(seen["stopped"], "credential_expired", "the wait ends with its stop reason")
            self.assertEqual([kind for _at, kind in gateway.room(1).calls], [], "no grant-state read, no activity")
            entry = gateway.entry(1)
            self.assertEqual((entry["connection"], entry["lastErrorCode"]), ("stopped", "credential_expired"))
            self.assertEqual(seen["sibling"], "pending")
            self.assertEqual(gateway.room(2).at("grant-read"), [START + 2.0, START + 6.0])
            self.assertEqual(gateway.room(2).at("activity:preparing_response"), [START])

    def test_rdc_08_failures_and_retry_after_never_shorten_the_wait_and_an_event_grant_still_ends_it(self):
        with tempfile.TemporaryDirectory() as directory:
            gateway = Gateway(directory, 1)
            room = gateway.room(1)
            room.turn_failures = [rate_limited(header="30"), network_failure(), network_failure(), network_failure()]
            seen = {}

            async def body():
                waiting = gateway.wait(1)
                await gateway.clock.advance(80.0)
                seen["pending"] = ended(waiting)
                await gateway.deliver(1, grant(gateway, "member-1"))
                seen["at"], seen["outcome"] = gateway.clock.now, ended(waiting)

            run(gateway, body)
            reads = room.at("grant-read")
            self.assertEqual(reads[0], START + 2.0)
            self.assertGreaterEqual(reads[1] - reads[0], 30.0, "the Retry-After holds")
            waits = [later - earlier for earlier, later in zip(reads, reads[1:])]
            self.assertTrue(all(wait >= 4.0 for wait in waits), f"a tight poll after a failure: {waits}")
            self.assertEqual(seen["pending"], "pending", "a failed read does not end the wait")
            self.assertEqual((seen["at"], seen["outcome"]), (START + 80.0, "granted"))


class BusyRoomRequestCountTests(unittest.TestCase):
    """The measured target (common brief, Addendum 1): not contract text.

    The fixture: one membership in a coordinated Room, the fake minute
    [1000, 1060). Ten addressed sources are queued for it; Hermes dispatches
    one source per Room at a time, so the first requests its turn at 1000 and
    waits for the grant while the other nine wait in the local queue (the Room
    lease of a queued turn is 120 s, send's timeout, and nothing grants it
    this minute). The room is busy: every 5 s the existing event read returns
    a page with two posts of other agents to each other. The heartbeat loop
    runs at the Room's 20 s interval. Every call the fake Room receives is
    counted; heartbeats separately.
    """

    def measure(self, *, to: str | None) -> tuple[int, dict[str, int], int]:
        with tempfile.TemporaryDirectory() as directory:
            gateway = Gateway(directory, 1)
            room = gateway.room(1)

            async def queued_turns():
                binding = gateway.binding(1)
                for index in range(10):
                    source = f"queued-{index}"
                    try:
                        turn, observed = await gateway.instance._request_turn_with_fresh_context(
                            binding, "room-1", 0, source)
                        await gateway.instance._wait_for_turn(binding, turn, observed, source, 120)
                    except adapter.ProtocolError:
                        continue

            async def body():
                gateway.beat(1)
                turns = asyncio.create_task(queued_turns())
                for second in range(0, 60, 5):
                    await gateway.clock.advance(second - (gateway.clock.now - START))
                    await gateway.deliver(1, peer_post(gateway, to=to), peer_post(gateway, to=to))
                await gateway.clock.advance(60.0 - (gateway.clock.now - START))
                turns.cancel()
                await asyncio.gather(turns, return_exceptions=True)

            run(gateway, body)
            minute = room.general(START, START + 60.0)
            kinds: dict[str, int] = {}
            for _at, kind in minute:
                kinds[kind] = kinds.get(kind, 0) + 1
            beats = [at for at, _ok in room.heartbeats() if START <= at < START + 60.0]
            return len(minute), kinds, len(beats)

    def test_ten_queued_turns_in_a_busy_room_stay_at_or_below_60_calls_per_minute(self):
        general, kinds, beats = self.measure(to="member-8")
        report("busy room [1000, 1060) member-1, posts between other agents", general=general,
               heartbeats=beats, **{kind.replace(":", "_").replace("-", "_"): count for kind, count in sorted(kinds.items())})
        self.assertEqual((kinds.get("events"), kinds.get("ack")), (12, 24), kinds)
        self.assertLessEqual(general, ALLOWANCE, kinds)

    def test_variant_posts_to_everyone_reported(self):
        # Reported, not asserted: a post to everyone also costs this membership
        # a context acknowledgement, a peer acknowledgement and a policy read per
        # event, which P2c-3f does not bound (the HANDOVER gives both numbers).
        general, kinds, beats = self.measure(to=None)
        report("busy room [1000, 1060) member-1, posts to everyone", general=general,
               heartbeats=beats, **{kind.replace(":", "_").replace("-", "_"): count for kind, count in sorted(kinds.items())})
        self.assertEqual(kinds.get("events"), 12, kinds)
