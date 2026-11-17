"""Hermes 1.0.114: contract P2c-3e, one recovery budget per installation.

The universal rule is the contract paragraph P2c-3e, byte-identical in OpenClaw;
the shared case names are ``conformance.json`` ``parityP2c3eCases`` (RRB-01 to
RRB-04), the same four strings in both connectors. The fault (Paula, 17 Nov
2026): the Mac128 path to the Room dropped TLS; each of 8-10 memberships of one
installation recovered on its own schedule, and between 12:51 and 14:53Z one
gateway logged 357 heartbeat warnings and 210 re-registrations.

Every test runs a real ``SyntheticSocialityAdapter`` on a real state file and a
real ``LocalStatus`` in a throwaway directory. The Room is a fake per
membership, answered in the event loop, and time is a fake clock: ``asyncio.sleep``
and the adapter's ``_clock`` follow it, so minutes of an outage run in
milliseconds and every registration start carries its fake time.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import heapq
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from test_delivery_lifecycle import ROOT, adapter
from test_parity_p7a import REAL_SLEEP

protocol = __import__(adapter.__package__ + ".protocol", fromlist=["RoomProtocol"])
adapter_state = __import__(adapter.__package__ + ".state", fromlist=["save"])
local_status = adapter.local_status
CONFORMANCE = json.loads((ROOT / "conformance.json").read_text(encoding="utf-8"))
CONTRACT = (ROOT / "docs" / "delivery-lifecycle-contract-v1.md").read_text(encoding="utf-8")

RECOVERY_BUDGET_CASES = [
    "RRB-01 a persistent network failure across many memberships starts at most 6 registrations in any 60 seconds per installation",
    "RRB-02 a membership's recovery delay grows across consecutive failed registrations and heartbeats and resets only after an accepted heartbeat",
    "RRB-03 an HTTP 429 delays that membership at least by its Retry-After",
    "RRB-04 heartbeats of current sessions are never delayed by the budget, and a membership stopped by the Room does not use it",
    "RRB-05 a 429 on a heartbeat of a current session is retried after Retry-After and, while the lease holds, is not counted as a failure",
]
PARAGRAPH = """\
P2c-3e Recovery budget (2026-11-17). All memberships of one connector installation share one recovery budget: across
the installation at most 6 connector-session registrations start in any 60 seconds (P2c-3), and a membership that is
over the budget waits for the next free slot instead of failing. Each membership's recovery delay follows the P2c-3a
schedule with its jitter and persists across consecutive failed registrations and failed heartbeats of the same
outage; it is reset only by a heartbeat that the Room accepts on the new session. An HTTP 429 answer to a registration
delays that membership's next registration at least by its `Retry-After` (or `retryAfterSeconds`); an HTTP 429 answer
to a heartbeat of a current session is retried after its `Retry-After` and does not count as a failed heartbeat unless
the lease expires first. Heartbeats of a membership whose session is current are never delayed by the budget, and a
membership stopped by the Room (P2c-3d) does not use it.
"""
PARAGRAPH_SHA256 = "c6e14bbdcd748cb21e3abd67a834bdabc89e7c365c187e65029d0b5b2b58357d"  # Addendum 1, final
BUDGET, WINDOW = 6, 60.0
INTERVAL = 20  # the heartbeat interval every fake Room instructs
FAR_LEASE = "2099-01-01T00:00:00Z"


class FakeTime:
    """A fake clock: ``sleep`` waits for ``advance`` to reach its wake time."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.sleepers: list[tuple[float, int, asyncio.Future]] = []
        self.order = 0

    async def sleep(self, delay, result=None):
        if delay <= 0:
            await REAL_SLEEP(0)
            return result
        future = asyncio.get_running_loop().create_future()
        heapq.heappush(self.sleepers, (self.now + float(delay), self.order, future))
        self.order += 1
        await future
        return result

    @staticmethod
    async def settle() -> None:
        for _ in range(60):
            await REAL_SLEEP(0)

    async def advance(self, seconds: float) -> None:
        end = self.now + seconds
        await self.settle()
        while self.sleepers and self.sleepers[0][0] <= end:
            wake = self.sleepers[0][0]
            self.now = max(self.now, wake)
            while self.sleepers and self.sleepers[0][0] <= wake:
                _wake, _order, future = heapq.heappop(self.sleepers)
                if not future.done():
                    future.set_result(None)
            await self.settle()
        self.now = end


def network_failure() -> adapter.ProtocolError:
    """What the real client raises when TLS drops (protocol.py normalizes every OSError)."""
    return adapter.ProtocolError(
        "Room API unavailable: [SSL: UNEXPECTED_EOF_WHILE_READING] EOF occurred in violation of protocol",
        retryable=True)


def rate_limited(*, header: str | None = None, detail: int | None = None) -> adapter.ProtocolError:
    """The ProtocolError the real client raises for the Room's 429 (server api/limits.go)."""
    body: dict = {"code": "rate_limited", "message": "request rate limit exceeded", "retryable": True}
    if detail is not None:
        body["details"] = {"retryAfterSeconds": detail}
    headers = {"Retry-After": header} if header is not None else {}
    failure = protocol.urllib.error.HTTPError(
        "https://room.example/api/rooms/room-1/connectors", 429, "Too Many Requests", headers,
        io.BytesIO(json.dumps(body).encode()))
    with mock.patch.object(protocol, "_urlopen", side_effect=failure):
        try:
            protocol.RoomProtocol("https://room.example/api", "credential").request("POST", "/rooms/room-1/connectors")
        except adapter.ProtocolError as error:
            return error
    raise AssertionError("no error raised")


class Room:
    """The Room for one membership. ``down`` fails every registration and heartbeat
    as a dropped TLS connection does; scripted answers (None accepts) come first."""

    def __init__(self, clock: FakeTime, membership_id: str) -> None:
        self.clock = clock
        self.membership_id = membership_id
        self.down = False
        self.register_script: list = []
        self.heartbeat_script: list = []
        self.register_lease = FAR_LEASE
        self.attempts: list[tuple[float, str, bool]] = []  # (fake time, contact, accepted)

    def starts(self) -> list[float]:
        return [at for at, contact, _ok in self.attempts if contact == "register"]

    def heartbeats(self) -> list[tuple[float, bool]]:
        return [(at, ok) for at, contact, ok in self.attempts if contact == "heartbeat"]

    def _answer(self, contact: str, script: list):
        failure = script.pop(0) if script else (network_failure() if self.down else None)
        self.attempts.append((self.clock.now, contact, failure is None))
        if failure is not None:
            raise failure

    def status(self):
        return {"protocolCapabilities": []}

    def register_connector(self, _room, _installation, _metadata):
        self._answer("register", self.register_script)
        return {"sessionId": f"session-{len(self.starts())}", "leaseExpiresAt": self.register_lease,
                "heartbeatIntervalSeconds": INTERVAL}

    def heartbeat(self, _room, _session):
        self._answer("heartbeat", self.heartbeat_script)
        return {"leaseExpiresAt": FAR_LEASE}

    def connector_enrollments(self, _installation):
        return []

    def room_state(self, _room):
        return {"roster": [{"membershipId": self.membership_id, "status": "active"}],
                "activeEpoch": {"id": "epoch-1", "startsAtSeq": 1}}


def room_id(index: int) -> str:
    return f"room-{index}"


def stored(index: int, **fields) -> adapter.RoomBinding:
    return adapter.RoomBinding("https://room.example/api", room_id(index), f"member-{index}",
                               f"credential-{index}", installation_id="inst-1", **fields)


class Installation:
    """One real adapter (one installation, one status file) holding ``count`` memberships."""

    def __init__(self, directory: str, count: int) -> None:
        self.clock = FakeTime()
        self.path = Path(directory) / "state.json"
        self.status_path = self.path.parent / "status.json"
        adapter_state.save(adapter.PluginState(bindings=[stored(index) for index in range(1, count + 1)]), self.path)
        self.rooms = {room_id(index): Room(self.clock, f"member-{index}") for index in range(1, count + 1)}
        with self.scope():
            self.instance = adapter.SyntheticSocialityAdapter(adapter.PlatformConfig())
        instance = self.instance
        instance._state_file = self.path

        async def noop(*_args, **_kwargs):
            return None

        async def fresh(_binding):
            return True

        async def held_stream(_binding):
            await asyncio.sleep(3600)  # the event stream stays open

        async def call(binding, operation, **_options):
            return operation(self.rooms[binding.room_id])

        instance._refresh_binding = fresh
        instance._expire_stale_pending = noop
        instance._ensure_epoch_session_routing = noop
        instance._repair_pending_lifecycles = noop
        instance._apply_recovery_closures = noop
        instance._publish = noop
        instance._transport_cycle = held_stream
        instance._persist_binding = lambda _binding: True
        instance._mark_connected = lambda: None
        instance._mark_disconnected = lambda: None
        instance._call = call
        instance._raw_call = call

    @contextlib.contextmanager
    def scope(self):
        with adapter_state.state_path_scope(self.path), \
                mock.patch.object(adapter.secret_isolation, "store_reachability", lambda *_a, **_k: []):
            yield

    @contextlib.contextmanager
    def running(self):
        """The fake clock drives every wait; the log is muted (it carries no secret)."""
        with self.scope(), mock.patch.object(asyncio, "sleep", self.clock.sleep), \
                mock.patch.object(adapter, "_clock", lambda: self.clock.now), \
                mock.patch.object(adapter, "logger", mock.Mock()):
            yield

    def binding(self, index: int) -> adapter.RoomBinding:
        return self.instance._state.binding(room_id(index))

    def room(self, index: int) -> Room:
        return self.rooms[room_id(index)]

    def watch(self, index: int) -> asyncio.Task:
        binding = self.binding(index)
        task = asyncio.create_task(self.instance._watch(binding, is_reconnect=False))
        self.instance._tasks[binding.room_id] = task
        return task

    def beat(self, index: int, *, session: str = "session-current", lease_deadline: float | None = None) -> asyncio.Task:
        """The heartbeat loop of a membership whose session the Room accepted."""
        binding = self.binding(index)
        binding.connector_session_id = session
        registered = getattr(self.instance, "_registered_rooms", None)
        if registered is None:
            registered = self.instance._registered_rooms = set()
        registered.add(binding.room_id)
        self.instance._room_map("_heartbeat_intervals")[binding.room_id] = float(INTERVAL)
        self.instance._lease_deadline[binding.room_id] = (
            lease_deadline if lease_deadline is not None else asyncio.get_running_loop().time() + 3600)
        task = asyncio.create_task(self.instance._heartbeat_loop(binding, float(INTERVAL)))
        self.instance._heartbeat_tasks[binding.room_id] = task
        return task

    def all_starts(self) -> list[float]:
        return sorted(at for room in self.rooms.values() for at in room.starts())

    def entry(self, index: int) -> dict:
        return local_status.read(self.status_path).get("memberships", {})[f"member-{index}"]

    async def stop(self) -> None:
        instance = self.instance
        instance._stop.set()
        pending = [*instance._tasks.values(), *instance._heartbeat_tasks.values(),
                   *instance._room_map("_enrollment_tasks").values()]
        if instance._renewal_task is not None:
            pending.append(instance._renewal_task)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)


def most_in_a_window(starts: list[float], window: float = WINDOW) -> int:
    """The largest number of starts in any half-open window [t, t + 60 s)."""
    ordered = sorted(starts)
    return max((sum(1 for other in ordered if at <= other < at + window) for at in ordered), default=0)


def gaps(attempts: list[tuple[float, str, bool]]) -> list[tuple[str, bool, float]]:
    """For each attempt but the last: its contact, whether it was accepted, and the wait to the next attempt."""
    return [(contact, ok, attempts[index + 1][0] - at)
            for index, (at, contact, ok) in enumerate(attempts[:-1])]


def report(label: str, **values) -> None:
    """The storm counts for the HANDOVER (stderr, only when asked for)."""
    if os.environ.get("RRB_REPORT"):
        print(f"RRB {label}: " + ", ".join(f"{key}={value}" for key, value in values.items()), file=sys.stderr)


class RecoveryBudgetConformanceTests(unittest.TestCase):
    def test_the_paragraph_follows_p2c_3d_byte_identical(self):
        self.assertEqual(hashlib.sha256(PARAGRAPH.encode("utf-8")).hexdigest(), PARAGRAPH_SHA256)
        lines = CONTRACT.split("\n")
        start = next(index for index, line in enumerate(lines) if line.startswith("P2c-3d Stopped by the Room "))
        end = next(index for index in range(start, len(lines)) if not lines[index])
        self.assertTrue(lines[end - 1].endswith("The per-Room summary follows the membership entries (P2c-4)."))
        block = PARAGRAPH.split("\n")[:-1]
        self.assertEqual(lines[end + 1:end + 1 + len(block)], block)
        self.assertEqual(lines[end + 1 + len(block)], "")
        self.assertTrue(lines[end + 2 + len(block)].startswith("P2c-4 Local status. "))
        self.assertEqual(CONTRACT.count("P2c-3e Recovery budget"), 1)

    def test_the_case_names_are_the_shared_ones_and_each_has_a_test(self):
        self.assertEqual(CONFORMANCE["parityP2c3eCases"], RECOVERY_BUDGET_CASES)
        source = Path(__file__).read_text(encoding="utf-8")
        for index, name in enumerate(RECOVERY_BUDGET_CASES, start=1):
            self.assertTrue(name.startswith(f"RRB-{index:02d} "), name)
            self.assertGreaterEqual(source.count(f"    def test_rrb_{index:02d}_"), 1, name)

    def test_the_contract_hash_and_the_p2c_section_pin_are_the_pinned_ones(self):
        digest = hashlib.sha256((ROOT / "docs" / "delivery-lifecycle-contract-v1.md").read_bytes()).hexdigest()
        self.assertEqual(CONFORMANCE["contractSha256"], digest)
        self.assertIn("P2c", CONFORMANCE["parityContractSectionSha256"])

    def test_the_budget_values_are_the_contract_ones(self):
        self.assertEqual((adapter.RECOVERY_BUDGET_STARTS, adapter.RECOVERY_BUDGET_WINDOW_SECONDS), (BUDGET, WINDOW))


class RecoveryBudgetTests(unittest.TestCase):
    def test_rrb_01_twelve_memberships_in_a_five_minute_outage_start_at_most_6_registrations_per_60_s(self):
        # Paula's finding: a TLS drop on the path to the Room, 8-10 memberships
        # per installation, each recovering on its own schedule. Here 12
        # memberships start into a persistent failure that lasts 5 minutes.
        count, outage = 12, 300.0
        with tempfile.TemporaryDirectory() as directory:
            gateway = Installation(directory, count)
            for room in gateway.rooms.values():
                room.down = True
            observed = {}

            async def run():
                try:
                    tasks = [gateway.watch(index) for index in range(1, count + 1)]
                    await gateway.clock.advance(outage)
                    observed["outage"] = gateway.all_starts()
                    observed["running"] = [not task.done() for task in tasks]
                    # The network comes back: every membership connects, still within the budget.
                    for room in gateway.rooms.values():
                        room.down = False
                    await gateway.clock.advance(240.0)
                    observed["all"] = gateway.all_starts()
                    observed["connected"] = [gateway.entry(index)["connection"] for index in range(1, count + 1)]
                finally:
                    await gateway.stop()

            with gateway.running():
                asyncio.run(run())
            starts = observed["outage"]
            report("RRB-01 storm", memberships=count, outage_seconds=int(outage), registration_starts=len(starts),
                   most_in_any_60s=most_in_a_window(starts),
                   first_60s=sum(1 for at in starts if at < starts[0] + WINDOW) if starts else 0)
            self.assertLessEqual(most_in_a_window(starts), BUDGET,
                                 f"{len(starts)} registration starts in {int(outage)} s, "
                                 f"{most_in_a_window(starts)} in one 60 s window")
            # Over the budget a membership waits for the next free slot instead of failing:
            # every watch keeps running and every membership keeps trying.
            self.assertEqual(observed["running"], [True] * count)
            for index in range(1, count + 1):
                self.assertGreaterEqual(len([at for at in gateway.room(index).starts() if at < 1000.0 + outage]), 1,
                                        f"member-{index} never started a registration")
            # The budget is used, not idle: about 6 per minute over the outage.
            self.assertGreaterEqual(len(starts), BUDGET * int(outage // WINDOW) - BUDGET)
            self.assertLessEqual(most_in_a_window(observed["all"]), BUDGET)
            self.assertEqual(observed["connected"], ["connected"] * count)

    def check_delays(self, attempts: list[tuple[float, str, bool]], expected: list[tuple[str, bool, tuple[float, float]]]):
        observed = gaps(attempts)[:len(expected)]
        self.assertEqual(len(observed), len(expected), attempts)
        for number, ((contact, ok, wait), (want_contact, want_ok, (low, high))) in enumerate(
                zip(observed, expected), start=1):
            self.assertEqual((contact, ok), (want_contact, want_ok), f"attempt {number}: {observed}")
            self.assertTrue(low - 1e-9 <= wait <= high + 1e-9,
                            f"attempt {number} ({contact}, accepted={ok}): waited {wait:.2f} s, "
                            f"expected {low}-{high} s; {observed}")

    def test_rrb_02_the_delay_grows_across_failed_heartbeats_and_registrations_and_resets_only_after_an_accepted_heartbeat(self):
        # A current session, then an outage: its heartbeat fails, the lease has
        # run out, two registrations fail, the third is accepted, the next two
        # heartbeats on the new session fail, then one is accepted.
        with tempfile.TemporaryDirectory() as directory:
            gateway = Installation(directory, 1)
            room = gateway.room(1)
            fail = network_failure
            room.heartbeat_script = [fail(), fail(), fail(), None, fail(), None, None]
            room.register_script = [fail(), fail(), None]

            async def run():
                try:
                    gateway.beat(1, lease_deadline=1.0)  # the lease ran out during the outage
                    await gateway.clock.advance(300.0)
                finally:
                    await gateway.stop()

            with gateway.running():
                asyncio.run(run())
            step = lambda n: (0.8 * min(2 ** n, 15), min(2 ** n, 15))  # noqa: E731 - P2c-3a with its jitter
            self.check_delays(room.attempts, [
                ("heartbeat", False, step(0)),
                ("register", False, step(1)),
                ("register", False, step(2)),
                ("register", True, (INTERVAL, INTERVAL)),  # an accepted registration alone resets nothing ...
                ("heartbeat", False, step(3)),             # ... so the outage's delay goes on growing
                ("heartbeat", False, step(4)),
                ("heartbeat", True, (INTERVAL, INTERVAL)),  # the accepted heartbeat on the new session resets it
                ("heartbeat", False, step(0)),
                ("heartbeat", True, (INTERVAL, INTERVAL)),
            ])

    def test_rrb_02_failed_registrations_of_the_event_stream_carry_over_to_the_heartbeat(self):
        # Gateway start in the outage: the event stream's two registrations fail,
        # the third is accepted, and the first heartbeat on the new session fails.
        with tempfile.TemporaryDirectory() as directory:
            gateway = Installation(directory, 1)
            room = gateway.room(1)
            room.register_script = [network_failure(), network_failure(), None]
            room.heartbeat_script = [network_failure(), None]

            async def run():
                try:
                    gateway.watch(1)
                    await gateway.clock.advance(120.0)
                finally:
                    await gateway.stop()

            with gateway.running():
                asyncio.run(run())
            step = lambda n: (0.8 * min(2 ** n, 15), min(2 ** n, 15))  # noqa: E731 - P2c-3a with its jitter
            self.check_delays(room.attempts, [
                ("register", False, step(0)),
                ("register", False, step(1)),
                ("register", True, (INTERVAL, INTERVAL)),
                ("heartbeat", False, step(2)),
            ])

    def test_rrb_03_a_429_on_registration_delays_that_membership_at_least_by_its_retry_after(self):
        for label, answer in (("Retry-After header", lambda: rate_limited(header="30")),
                              ("retryAfterSeconds detail", lambda: rate_limited(detail=30)),
                              ("both", lambda: rate_limited(header="45", detail=45))):
            with self.subTest(label), tempfile.TemporaryDirectory() as directory:
                gateway = Installation(directory, 2)
                failure = answer()
                wait = failure.retry_after or 0.0
                room = gateway.room(1)
                room.register_script = [failure, None]

                async def run():
                    try:
                        gateway.watch(1)
                        gateway.watch(2)
                        await gateway.clock.advance(180.0)
                    finally:
                        await gateway.stop()

                with gateway.running():
                    asyncio.run(run())
                starts = room.starts()
                self.assertEqual(len(starts), 2, room.attempts)
                expected = 45.0 if label == "both" else 30.0
                self.assertEqual(wait, expected, "the client reads the Room's Retry-After")
                self.assertGreaterEqual(starts[1] - starts[0], expected, room.attempts)
                self.assertEqual(gateway.entry(1)["connection"], "connected")
                # Only that membership: its sibling registered at once.
                self.assertEqual(gateway.room(2).starts(), [1000.0])

    def test_rrb_05_a_429_on_a_current_heartbeat_is_retried_after_retry_after_and_is_not_a_failure(self):
        # Paula's paste check (Addendum 1): the Room's per-membership allowance
        # answers 429 to a heartbeat; the session is current and its lease holds.
        # The heartbeat is retried after the Retry-After, the membership's
        # recovery step does not move, and nothing reports it as failing: the
        # next network failure still waits the schedule's first step.
        for label, answer, wait in (("Retry-After header", lambda: rate_limited(header="30"), 30.0),
                                    ("retryAfterSeconds detail", lambda: rate_limited(detail=40), 40.0)):
            with self.subTest(label), tempfile.TemporaryDirectory() as directory:
                gateway = Installation(directory, 1)
                room = gateway.room(1)
                room.heartbeat_script = [answer(), answer(), network_failure(), None]
                writes: list[tuple] = []
                status_room = gateway.instance._status.room

                def recorded(room_id, membership_id="", **fields):
                    writes.append((fields.get("connection"), fields.get("error_code")))
                    return status_room(room_id, membership_id, **fields)

                gateway.instance._status.room = recorded
                observed = {}

                async def run():
                    try:
                        gateway.beat(1)
                        await gateway.clock.advance(INTERVAL + 2 * wait + 0.5)
                        observed["step"] = gateway.instance._room_map("_recovery_steps").get(room_id(1), 0)
                        await gateway.clock.advance(120.0)
                    finally:
                        await gateway.stop()

                with gateway.running():
                    asyncio.run(run())
                self.check_delays(room.attempts, [
                    ("heartbeat", False, (wait, wait)),  # retried after its Retry-After ...
                    ("heartbeat", False, (wait, wait)),  # ... again, still not a failure
                    ("heartbeat", False, (0.8, 1.0)),    # a real failure: the schedule's first step
                    ("heartbeat", True, (INTERVAL, INTERVAL)),
                ])
                self.assertEqual(observed["step"], 0, "a 429 on a current heartbeat moved the recovery step")
                self.assertEqual(room.starts(), [], "the session stayed current: no registration")
                # Only the network failure reports reconnecting; the two 429s report nothing failing.
                self.assertEqual([write for write in writes if write[0] == "reconnecting"],
                                 [("reconnecting", "network")], writes)
                self.assertEqual(gateway.entry(1)["connection"], "connected")

    def test_rrb_05_once_the_lease_has_expired_a_429_on_a_heartbeat_takes_the_normal_recovery_path(self):
        # The lease ran out before the 429: the heartbeat counts as failed, the
        # next contact registers after the Retry-After, and the step has moved.
        with tempfile.TemporaryDirectory() as directory:
            gateway = Installation(directory, 1)
            room = gateway.room(1)
            room.heartbeat_script = [rate_limited(header="30")]
            room.register_script = [network_failure(), None]

            async def run():
                try:
                    gateway.beat(1, lease_deadline=1.0)
                    await gateway.clock.advance(120.0)
                finally:
                    await gateway.stop()

            with gateway.running():
                asyncio.run(run())
            self.check_delays(room.attempts, [
                ("heartbeat", False, (30.0, 30.0)),
                ("register", False, (1.6, 2.0)),  # the 429 counted: this is the schedule's second step
                ("register", True, (INTERVAL, INTERVAL)),
            ])

    def test_rrb_04_heartbeats_of_current_sessions_keep_their_interval_while_the_budget_is_spent(self):
        # Ten memberships in the outage spend the budget; two others hold current
        # sessions whose heartbeats the Room accepts.
        count = 12
        with tempfile.TemporaryDirectory() as directory:
            gateway = Installation(directory, count)
            for index in range(1, 11):
                gateway.room(index).down = True

            async def run():
                try:
                    for index in range(1, 11):
                        gateway.watch(index)
                    gateway.beat(11)
                    gateway.beat(12)
                    await gateway.clock.advance(300.0)
                finally:
                    await gateway.stop()

            with gateway.running():
                asyncio.run(run())
            self.assertLessEqual(most_in_a_window(gateway.all_starts()), BUDGET)
            for index in (11, 12):
                beats = gateway.room(index).heartbeats()
                self.assertEqual(beats, [(1000.0 + INTERVAL * n, True) for n in range(1, 16)],
                                 f"member-{index}'s heartbeats were delayed")
                self.assertEqual(gateway.room(index).starts(), [])

    def test_rrb_04_a_membership_stopped_by_the_room_does_not_use_the_budget(self):
        # Six memberships take the first minute's slots; the seventh waits for a
        # free slot and is stopped by the Room (credential_expired) meanwhile.
        count = 10
        with tempfile.TemporaryDirectory() as directory:
            gateway = Installation(directory, count)
            for room in gateway.rooms.values():
                room.down = True
            observed = {}

            async def run():
                try:
                    for index in range(1, 7):
                        gateway.watch(index)
                    await gateway.clock.settle()
                    stopped = gateway.watch(7)
                    await gateway.clock.advance(10.0)
                    observed["before"] = list(gateway.room(7).starts())
                    gateway.instance._expire(gateway.binding(7))
                    for index in range(8, count + 1):
                        gateway.watch(index)
                    await gateway.clock.advance(290.0)
                    observed["ended"] = stopped.done()
                finally:
                    await gateway.stop()

            with gateway.running():
                asyncio.run(run())
            self.assertEqual(observed["before"], [], "the seventh registration waited for a free slot")
            self.assertEqual(gateway.room(7).starts(), [], "a stopped membership never starts a registration")
            self.assertTrue(observed["ended"])
            entry = gateway.entry(7)
            self.assertEqual((entry["connection"], entry["lastErrorCode"]), ("stopped", "credential_expired"))
            self.assertLessEqual(most_in_a_window(gateway.all_starts()), BUDGET)
            # Its place went to the others: the budget stays in use.
            self.assertGreaterEqual(len(gateway.all_starts()), BUDGET * 4)


if __name__ == "__main__":
    unittest.main()
