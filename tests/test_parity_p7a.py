"""Parity P7a "Stay connected": contract P2c-3a, P2c-3b, P2c-3c, P2c-6 and the
P2c-4 additions (conformance ``parityP7aCases``; OpenClaw implements the same
names).

The Room server restart of 2026-10-04 22:16Z (FINDING-20261004-reconnect-after-
server-restart): the public-route Rooms were retried only every 60 s because a
502's Retry-After was honoured up to 120 s, and the event stream backed off to
30 s. The schedule here is one rule: 1, 2, 4, 8, 15 s, jitter [0.8, 1.0], no
attempt limit, Retry-After up to 15 s on a 5xx and up to 120 s on a 429.

Waits are recorded through ``asyncio.sleep`` (patched for the case, keyed by
the calling function) and compressed to milliseconds.
"""

from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import threading
import time
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

from test_delivery_lifecycle import ROOT, adapter
from test_parity_p1 import cycle_attempt, room_instance

CONFORMANCE = json.loads((ROOT / "conformance.json").read_text())
local_status = adapter.local_status
protocol = sys.modules[adapter.__name__.rsplit(".", 1)[0] + ".protocol"]
REAL_SLEEP = asyncio.sleep
CAP = 15.0
LEASE = "2099-01-01T00:00:00Z"


class Waits:
    """asyncio.sleep, recorded per calling function and compressed."""

    def __init__(self):
        self.calls: list[tuple[str, float]] = []

    def __call__(self, delay, result=None):
        caller = sys._getframe(1).f_code.co_name
        self.calls.append((caller, float(delay)))
        return REAL_SLEEP(0.002 if delay > 0 else 0, result)

    def of(self, caller):
        return [delay for name, delay in self.calls if name == caller]


def http_error(status, *, code="", retry_after="", message=""):
    return adapter.ProtocolError(
        message or f"Room API returned HTTP {status}", status=status, code=code,
        retryable=status == 429 or status >= 500 or code == "connector_not_active",
        retry_after=protocol.parse_retry_after(retry_after),
    )


class FakeRoom:
    """The Room as the connector sees it: scripted failures, then answers."""

    def __init__(self):
        self.lock = threading.Lock()
        self.register_errors: list[Exception] = []
        self.heartbeat_errors: list[Exception] = []
        self.stream_errors: list[Exception] = []
        self.enrollment_error: Exception | None = None
        self.member_status = "active"
        self.registrations = 0
        self.heartbeats = 0
        self.enrollments = 0
        self.state_reads = 0
        self.pending_events: list[dict] = []

    def status(self):
        return {"protocolCapabilities": []}

    def register_connector(self, _room, _installation, _metadata):
        with self.lock:
            if self.register_errors:
                raise self.register_errors.pop(0)
            self.registrations += 1
            return {"sessionId": f"session-{self.registrations}", "leaseExpiresAt": LEASE, "heartbeatIntervalSeconds": 15}

    def heartbeat(self, _room, _session):
        with self.lock:
            if self.heartbeat_errors:
                raise self.heartbeat_errors.pop(0)
            self.heartbeats += 1
        return {"leaseExpiresAt": LEASE}

    def connector_enrollments(self, _installation):
        with self.lock:
            self.enrollments += 1
            if self.enrollment_error is not None:
                raise self.enrollment_error
        return []

    def room_state(self, _room):
        with self.lock:
            self.state_reads += 1
            return {"roster": [{"membershipId": "member-1", "status": self.member_status}],
                    "activeEpoch": {"id": "epoch-1", "startsAtSeq": 1}}

    def stream_events(self, _room, after, on_event):
        with self.lock:
            if self.stream_errors:
                raise self.stream_errors.pop(0)
            events, self.pending_events = self.pending_events, []
        for offset, event in enumerate(events, start=1):
            on_event({**event, "seq": after + offset})
        time.sleep(0.005)
        return {"cursor": after, "ready": {"ok": True}}


def watched(room, status_path=None):
    """A connector instance whose Room calls go to ``room``."""
    binding = adapter.RoomBinding("https://room.example/api", "room-1", "member-1", "credential",
                                  installation_id="inst-1", connector_session_id="session-stored")
    instance = object.__new__(adapter.SyntheticSocialityAdapter)
    for name, value in {
        "_stop": asyncio.Event(), "_tasks": {}, "_heartbeat_tasks": {}, "_lease_deadline": {},
        "_registered_rooms": set(), "_message_contract_checked": set(), "_effective_transports": {},
        "_stream_generations": {}, "_receive_locks": {}, "_renewing_rooms": set(), "_host_label": "host-now",
        "_status": None if status_path is None else local_status.LocalStatus(
            status_path, plugin="hermes", version=adapter.CONNECTOR_VERSION, host_label="host-now"),
    }.items():
        setattr(instance, name, value)
    instance.consumed = []

    async def consume(_binding, event):
        instance.consumed.append(event["id"])

    noop = lambda *_args, **_kwargs: asyncio.sleep(0)
    instance._refresh_binding = lambda _binding: REAL_SLEEP(0, True)
    instance._expire_stale_pending = noop
    instance._ensure_epoch_session_routing = noop
    instance._repair_pending_lifecycles = noop
    instance._publish = noop
    instance._persist_binding = lambda _binding: True
    instance._consume = consume
    instance._mark_connected = lambda: None
    instance._mark_disconnected = lambda: None
    instance._call = lambda _binding, operation: asyncio.to_thread(operation, room)
    instance._stream_call = lambda _binding, operation: asyncio.to_thread(operation, room)
    instance.connects = []
    instance.connect = lambda **_kwargs: instance.connects.append(True)
    return instance, binding


def run_watch(instance, binding, until, *, timeout=20.0, after=None):
    """Run the watch loop until ``until()`` holds; return whether it did and the task's state."""
    outcome = {}

    async def run():
        task = asyncio.create_task(instance._watch(binding, is_reconnect=False))
        instance._tasks[binding.room_id] = task
        deadline = time.monotonic() + timeout
        while not until() and time.monotonic() < deadline and not task.done():
            await REAL_SLEEP(0.01)
        outcome["reached"] = until()
        if after is not None and outcome["reached"]:
            await after()
        outcome["exited"] = task.done()
        instance._stop.set()
        for pending in (task, *instance._heartbeat_tasks.values()):
            pending.cancel()
        await asyncio.gather(task, *instance._heartbeat_tasks.values(), return_exceptions=True)

    asyncio.run(run())
    return outcome


class P7aConformanceTests(unittest.TestCase):
    def test_case_names_resolve_to_tests(self):
        cases = CONFORMANCE["parityP7aCases"]
        self.assertEqual(len(cases), 9)
        sources = "\n".join(path.read_text() for path in (ROOT / "tests").glob("test_*.py"))
        for index, (name, method) in enumerate(cases.items(), start=1):
            self.assertTrue(name.startswith(f"P7a-{index:02d} "), name)
            self.assertEqual(sources.count(f"def {method}(self"), 1, method)
        self.assertEqual(CONFORMANCE["parityP7aContract"],
                         "docs/delivery-lifecycle-contract-v1.md#p2c--secrets-late-replies-self-recovery-2026-10-02")

    def test_schedule_and_names_are_the_shared_values(self):
        schedule = CONFORMANCE["parityP7aReconnectSchedule"]
        self.assertEqual(schedule, {
            "firstSeconds": protocol.RECONNECT_FIRST_SECONDS, "capSeconds": protocol.RECONNECT_CAP_SECONDS,
            "jitterFactor": [protocol.RECONNECT_JITTER_LOW, 1.0],
            "retryAfter5xxMaxSeconds": protocol.RECONNECT_CAP_SECONDS,
            "retryAfter429MaxSeconds": protocol.RETRY_AFTER_RATE_LIMIT_MAX_SECONDS,
            "attemptRenewalCapSeconds": adapter.SyntheticSocialityAdapter.ATTEMPT_RENEWAL_RETRY_SECONDS,
            "inactiveRecheckSeconds": adapter.INACTIVE_RECHECK_SECONDS,
        })
        self.assertEqual(CONFORMANCE["parityP7aWebReadToolNames"], list(local_status.WEB_READ_TOOL_NAMES))

    def test_the_schedule_bounds(self):
        low = lambda a, _b: a
        high = lambda _a, b: b
        self.assertEqual([protocol.reconnect_delay(step, rng=high) for step in range(7)], [1, 2, 4, 8, 15, 15, 15])
        self.assertEqual([protocol.reconnect_delay(step, rng=low) for step in range(5)], [0.8, 1.6, 3.2, 6.4, 12.0])
        self.assertEqual(protocol.reconnect_delay(0, retry_after=300, status=502, rng=low), 15.0)
        self.assertEqual(protocol.reconnect_delay(0, retry_after=300, status=429, rng=low), 120.0)
        self.assertEqual(protocol.reconnect_delay(4, retry_after=2, status=503, rng=high), 15.0)  # never shortened
        self.assertEqual(protocol.reconnect_delay(9, cap=5.0, rng=high), 5.0)


class P7aTests(unittest.TestCase):
    def test_p7a_01_502_then_200_reconnects_within_the_cap_without_exit(self):
        # 22:16-22:19Z: the server answered 502 for minutes; Hermes' stream
        # loop backed off 1, 2, 4, 8, 16, 30 s and the public route waited 60 s.
        room = FakeRoom()
        room.register_errors = [http_error(502) for _ in range(7)]
        instance, binding = watched(room)
        waits = Waits()
        with mock.patch.object(asyncio, "sleep", waits), self.assertLogs(adapter.logger, level="INFO"):
            outcome = run_watch(instance, binding, lambda: room.registrations >= 1 and room.heartbeats >= 0 and len(waits.of("_watch")) >= 7)
        self.assertTrue(outcome["reached"])
        self.assertFalse(outcome["exited"])
        self.assertEqual(instance.connects, [])
        failures = waits.of("_watch")[:7]
        self.assertLessEqual(failures[0], 1.0)
        # The first 200 is answered at the next attempt, at most the cap later.
        self.assertTrue(all(delay <= CAP for delay in failures), failures)

    def test_p7a_02_a_malformed_event_page_is_retried_inside_the_connector(self):
        room = FakeRoom()
        room.stream_errors = [adapter.ProtocolError("SSE returned invalid JSON: Expecting value", retryable=True),
                              ValueError("Room event page active epoch metadata is invalid")]
        room.pending_events = [{"id": "evt-after-malformed", "type": "message.posted"}]
        instance, binding = watched(room)
        waits = Waits()
        with mock.patch.object(asyncio, "sleep", waits), self.assertLogs(adapter.logger, level="INFO"):
            outcome = run_watch(instance, binding, lambda: instance.consumed == ["evt-after-malformed"])
        self.assertTrue(outcome["reached"], instance.consumed)
        self.assertFalse(outcome["exited"])
        self.assertEqual(instance.connects, [])
        self.assertLessEqual(waits.of("_watch")[0], 1.0)

    def test_p7a_03_5xx_retry_after_300_waits_at_most_the_cap(self):
        # The stream and registration path, and the heartbeat.
        room = FakeRoom()
        room.register_errors = [http_error(502, retry_after="300"), http_error(503, retry_after="300")]
        instance, binding = watched(room)
        waits = Waits()
        with mock.patch.object(asyncio, "sleep", waits), self.assertLogs(adapter.logger, level="INFO"):
            outcome = run_watch(instance, binding, lambda: room.registrations >= 1)
        self.assertTrue(outcome["reached"])
        self.assertTrue(all(delay <= CAP for delay in waits.of("_watch")[:2]), waits.of("_watch"))

        room = FakeRoom()
        room.heartbeat_errors = [http_error(503, retry_after="300")]
        instance, binding = watched(room)
        instance._registered_rooms.add(binding.room_id)
        waits = Waits()

        async def heartbeat():
            task = asyncio.create_task(instance._heartbeat_loop(binding, 15))
            while room.heartbeats < 1:
                await REAL_SLEEP(0.01)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        with mock.patch.object(asyncio, "sleep", waits), self.assertLogs(adapter.logger, level="INFO"):
            asyncio.run(asyncio.wait_for(heartbeat(), 20))
        # interval, then the failed heartbeat's wait: within the cap.
        self.assertLessEqual(waits.of("_heartbeat_loop")[1], CAP, waits.of("_heartbeat_loop"))

    def test_p7a_04_429_retry_after_keeps_the_120_s_bound(self):
        room = FakeRoom()
        room.register_errors = [http_error(429, retry_after="30"), http_error(429, retry_after="300")]
        instance, binding = watched(room)
        waits = Waits()
        with mock.patch.object(asyncio, "sleep", waits), self.assertLogs(adapter.logger, level="INFO"):
            outcome = run_watch(instance, binding, lambda: room.registrations >= 1)
        self.assertTrue(outcome["reached"])
        first, second = waits.of("_watch")[:2]
        self.assertEqual(first, 30.0)
        self.assertEqual(second, 120.0)

    def test_p7a_05_a_muted_membership_is_inactive_and_rechecked_slowly(self):
        # RECEIPT-20261004-parity-P6 §4: Doctor, Patient and Pharmacist are
        # muted in one Room; the enrollment listing answered 401
        # connector_not_active and the connector registered again every 3-16 s.
        with tempfile.TemporaryDirectory() as scratch:
            path = Path(scratch) / "status.json"
            room = FakeRoom()
            room.member_status = "muted"
            room.enrollment_error = http_error(401, code="connector_not_active",
                                               message="an active connector session is required")
            instance, binding = watched(room, path)
            observed = {}

            async def unmute():
                await REAL_SLEEP(1.0)  # several heartbeats and rechecks while muted
                with room.lock:
                    observed["muted"] = (room.registrations, room.enrollments, room.state_reads, room.heartbeats)
                observed["status_muted"] = local_status.read(path)["rooms"]["room-1"]
                with room.lock:
                    room.member_status, room.enrollment_error = "active", None
                deadline = time.monotonic() + 10
                while room.enrollments < 2 and time.monotonic() < deadline:
                    await REAL_SLEEP(0.02)
                await REAL_SLEEP(0.1)
                observed["status_active"] = local_status.read(path)["rooms"]["room-1"]

            with mock.patch.object(adapter, "HEARTBEAT_MIN_INTERVAL_SECONDS", 0.05), \
                    mock.patch.object(adapter, "INACTIVE_RECHECK_SECONDS", 0.4, create=True), \
                    mock.patch.object(asyncio, "sleep", Waits()), \
                    self.assertLogs(adapter.logger, level="INFO"):
                outcome = run_watch(instance, binding, lambda: room.enrollments >= 1, after=unmute)
            self.assertTrue(outcome["reached"])
            registrations, enrollments, state_reads, heartbeats = observed["muted"]
            self.assertEqual(registrations, 1)  # no registration again
            self.assertGreater(heartbeats, 5)  # the session and heartbeat continue
            self.assertEqual(enrollments, 1)  # no listing while muted
            self.assertLessEqual(state_reads, 1 + 1.0 / 0.4 + 1)  # one read, then the slow re-check
            self.assertEqual((observed["status_muted"]["connection"], observed["status_muted"]["lastErrorCode"]),
                             ("inactive", "connector_not_active"))
            self.assertGreaterEqual(room.enrollments, 2)  # active again: the listing resumes
            self.assertEqual(observed["status_active"]["connection"], "connected")
            self.assertEqual(instance.connects, [])

    def test_p7a_06_more_than_10_failures_still_retrying(self):
        room = FakeRoom()
        room.register_errors = [adapter.ProtocolError("Room API unavailable: [Errno 111] Connection refused",
                                                      retryable=True) for _ in range(12)]
        instance, binding = watched(room)
        waits = Waits()
        with mock.patch.object(asyncio, "sleep", waits), self.assertLogs(adapter.logger, level="INFO"):
            outcome = run_watch(instance, binding, lambda: room.registrations >= 1)
        self.assertTrue(outcome["reached"])
        self.assertFalse(outcome["exited"])
        failures = waits.of("_watch")[:12]
        self.assertEqual(len(failures), 12)
        self.assertTrue(all(delay <= CAP for delay in failures), failures)
        self.assertEqual(instance.connects, [])

    def test_p7a_07_attempt_renewal_retries_within_1_s_and_never_past_the_lease(self):
        # ISSUE-023: on the overloaded VPS a renewal answer was late; the next
        # try came only 5 s later.
        def lease_in(seconds):
            at = datetime.now(timezone.utc) + timedelta(seconds=seconds)
            return at.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        attempt = cycle_attempt(lease=lease_in(60))
        instance, binding, _api, _published, _dispatched = room_instance(attempt)
        outcomes = [http_error(503), http_error(503), http_error(503), attempt]

        async def claim(_binding, _cycle, _bound=None):
            outcome = outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        instance._claim_discussion_attempt = claim
        waits = Waits()
        with mock.patch.object(asyncio, "sleep", waits), self.assertLogs(adapter.logger, level="WARNING"):
            refreshed = asyncio.run(instance._claim_held_attempt(binding, "evt-5", attempt))
        self.assertIs(refreshed, attempt)
        retries = waits.of("_claim_held_attempt")
        self.assertEqual(len(retries), 3)
        self.assertLessEqual(retries[0], 1.0)
        self.assertTrue(all(delay <= 5.0 for delay in retries), retries)

        # Never past the lease: with 0.6 s left, it gives up when it runs out.
        short = cycle_attempt(lease=lease_in(0.6))

        async def refuse(_binding, _cycle, _bound=None):
            raise http_error(503)

        instance._claim_discussion_attempt = refuse
        started = time.monotonic()
        with self.assertLogs(adapter.logger, level="WARNING"), self.assertRaises(adapter.ProtocolError):
            asyncio.run(instance._claim_held_attempt(binding, "evt-5", short))
        self.assertLess(time.monotonic() - started, 1.5)

    def test_p7a_08_status_reconnect_time_outage_and_gate_wait_reason(self):
        with tempfile.TemporaryDirectory() as scratch:
            path = Path(scratch) / "status.json"
            room = FakeRoom()
            room.register_errors = [http_error(502), http_error(502)]
            instance, binding = watched(room, path)
            with mock.patch.object(asyncio, "sleep", Waits()), self.assertLogs(adapter.logger, level="INFO"):
                outcome = run_watch(instance, binding, lambda: room.registrations >= 1)
            self.assertTrue(outcome["reached"])
            entry = local_status.read(path)["rooms"]["room-1"]
            self.assertIsNotNone(entry["lastReconnectedAt"])
            self.assertGreaterEqual(entry["lastOutageSeconds"], 0)
            self.assertEqual(entry["lastConnectedAt"], entry["lastReconnectedAt"])

            # The outage length; a heartbeat that survived is not a reconnect.
            status = local_status.LocalStatus(Path(scratch) / "direct.json", plugin="hermes",
                                              version=adapter.CONNECTOR_VERSION, host_label="h")
            status.room("room-2", "member-2", connected=True, registered=True)
            status.room("room-2", connection="reconnecting", error_code="http_502")
            time.sleep(0.3)
            status.room("room-2", connected=True, registered=True)
            first = local_status.read(Path(scratch) / "direct.json")["rooms"]["room-2"]
            self.assertGreaterEqual(first["lastOutageSeconds"], 0.3)
            status.room("room-2", connection="reconnecting", error_code="network")
            status.room("room-2", connected=True)
            second = local_status.read(Path(scratch) / "direct.json")["rooms"]["room-2"]
            self.assertEqual(second["lastReconnectedAt"], first["lastReconnectedAt"])
            self.assertEqual(second["lastOutageSeconds"], first["lastOutageSeconds"])

            # The coded dispatch-wait reason.
            instance._status = local_status.LocalStatus(path, plugin="hermes", version=adapter.CONNECTOR_VERSION,
                                                        host_label="h")
            instance._session_wait_started = {"room-1": time.time() - 2.0}
            instance._end_session_wait("room-1")
            entry = local_status.read(path)["rooms"]["room-1"]
            self.assertEqual(entry["lastGateWaitReason"], "host_run_busy")
            self.assertGreaterEqual(entry["lastGateWaitSeconds"], 1.9)
            # A guard core cannot report on (P2a-1), waited past its bound.
            instance._room_session_keys = {"room-1": "key-1"}
            instance._active_sessions = {"key-1": object()}
            instance._session_tasks = {}
            instance._session_wait_started = {"room-1": time.time() - adapter.SESSION_UNOBSERVABLE_WAIT_SECONDS - 5}
            with self.assertLogs(adapter.logger, level="WARNING"):
                self.assertFalse(instance._room_session_busy("room-1"))
            instance._end_session_wait("room-1")
            self.assertEqual(local_status.read(path)["rooms"]["room-1"]["lastGateWaitReason"],
                             "host_session_unobservable")

    def test_p7a_09_web_reading_reported_from_the_host_tool_selection(self):
        with tempfile.TemporaryDirectory() as scratch:
            path = Path(scratch) / "status.json"
            for selection, expected in (
                (({"read_file", "web_search", "web_extract", "browser_navigate"}, []),
                 ("available", ["browser_navigate", "web_extract", "web_search"])),
                (({"read_file", "terminal"}, []), ("unavailable", [])),
                ((None, []), ("unknown", [])),  # core cannot report its selection
            ):
                with self.subTest(selection=selection[0]):
                    instance = object.__new__(adapter.SyntheticSocialityAdapter)
                    instance._status = local_status.LocalStatus(path, plugin="hermes",
                                                                version=adapter.CONNECTOR_VERSION, host_label="h")
                    instance._state = types.SimpleNamespace(bindings=[])
                    instance._state_file = Path(scratch) / "state.json"
                    instance._stop = asyncio.Event()
                    instance._tasks = {}
                    instance._renewal_task = None
                    instance.config = types.SimpleNamespace(extra={})
                    with mock.patch.object(adapter, "_core_tool_selection", lambda: selection), \
                            self.assertLogs(adapter.logger, level="DEBUG"):
                        adapter.logger.debug("connect")
                        asyncio.run(instance.connect())
                    written = local_status.read(path)
                    self.assertEqual((written["webRead"], written["webReadTools"]), expected)


if __name__ == "__main__":
    unittest.main()
