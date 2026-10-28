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
import http.server
import io
import json
import socketserver
import ssl
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


def edge_error(status, body=b"<html><body>edge error page</body></html>"):
    """The ProtocolError the real client raises for an edge or proxy page."""
    failure = protocol.urllib.error.HTTPError("https://room.example/api/x", status, "edge", {}, io.BytesIO(body))
    with mock.patch.object(protocol, "_urlopen", side_effect=failure):
        try:
            protocol.RoomProtocol("https://room.example/api", "credential").request("GET", "/rooms/room-1/x")
        except adapter.ProtocolError as error:
            return error
    raise AssertionError("no error raised")


def base(step, cap=CAP):
    return min(cap, 2.0 ** step)


def on_schedule(case, waits, cap=CAP):
    """Each wait in [0.8, 1.0] of its base 1, 2, 4, 8, 15, ... s."""
    for step, delay in enumerate(waits):
        case.assertTrue(0.8 * base(step, cap) - 1e-9 <= delay <= base(step, cap) + 1e-9, (step, waits))


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
        self.log: list[str] = []  # registration and heartbeat attempts, in order

    def status(self):
        return {"protocolCapabilities": []}

    def register_connector(self, _room, _installation, _metadata):
        with self.lock:
            if self.register_errors:
                self.log.append("register failed")
                failure = self.register_errors.pop(0)
                raise failure() if callable(failure) else failure  # a callable makes the real failing request
            self.log.append("register")
            self.registrations += 1
            return {"sessionId": f"session-{self.registrations}", "leaseExpiresAt": LEASE, "heartbeatIntervalSeconds": 15}

    def heartbeat(self, _room, _session):
        with self.lock:
            if self.heartbeat_errors:
                self.log.append("heartbeat failed")
                raise self.heartbeat_errors.pop(0)
            self.log.append("heartbeat")
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
            self.log.append("stream")
            if self.stream_errors:
                failure = self.stream_errors.pop(0)
                raise failure() if callable(failure) else failure
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
    instance._call = lambda _binding, operation, **_request_options: asyncio.to_thread(operation, room)
    instance._raw_call = instance._call  # the roster probe bypasses the participation gate
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
        self.assertEqual(len(cases), 20)
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
            self.assertEqual(heartbeats, 0)  # P15: every watcher pauses, including heartbeat
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
        on_schedule(self, failures)  # the jitter bound: [0.8, 1.0] of each base
        # The factor is drawn, not fixed: twelve waits never all sit on their base.
        self.assertTrue(any(delay < base(step) - 1e-9 for step, delay in enumerate(failures)), failures)
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
                    with mock.patch.object(adapter, "_core_tool_selection", lambda: (set(), [])), \
                            mock.patch.object(adapter, "_room_turn_tool_selection", lambda: selection[0]), \
                            self.assertLogs(adapter.logger, level="DEBUG"):
                        adapter.logger.debug("connect")
                        asyncio.run(instance.connect())
                    written = local_status.read(path)
                    self.assertEqual((written["webRead"], written["webReadTools"]), expected)



class Endless(list):
    """A scripted failure that never runs out (``FakeRoom`` pops its errors)."""

    def __init__(self, make):
        super().__init__([None])
        self.make = make

    def pop(self, _index=-1):
        return self.make()


def html_page_room():
    room = FakeRoom()
    room.stream_errors = [edge_error(403), edge_error(404)]
    room.pending_events = [{"id": "evt-after-edge", "type": "message.posted"}]
    return room


class P7aRound2Tests(unittest.TestCase):
    def test_the_failed_contacts_are_the_listed_ones(self):
        # Contract P2c-3a: an edge page with any status is a failed contact;
        # the Room's own JSON decides everything else.
        for status in (401, 403, 404, 413, 502):
            with self.subTest(status=status):
                error = edge_error(status)
                self.assertTrue(error.non_json and error.retryable and not error.code, status)
                self.assertFalse(error.revoked or error.expired or error.session_lost)
                self.assertFalse(error.hard_sse_unavailable)
                self.assertTrue(adapter._uncoded_contact_failure(error))
                self.assertEqual(adapter._status_error_code(error), "invalid_json_response")
        room_404 = edge_error(404, json.dumps({"code": "not_found", "message": "room or object not found"}).encode())
        self.assertTrue(room_404.session_lost and not room_404.retryable)
        revoked = edge_error(401, json.dumps({"code": "credential_revoked"}).encode())
        self.assertTrue(revoked.revoked and not revoked.retryable)
        marked = edge_error(409, json.dumps({"code": "stale_context", "retryable": True}).encode())
        self.assertTrue(marked.retryable)
        self.assertFalse(adapter._uncoded_contact_failure(marked))  # an answer, never retried as a contact
        # Hermes r2 N1 (round 3): a 5xx or 429 with a Room code is retried too.
        internal = edge_error(500, json.dumps({"code": "internal", "retryable": True}).encode())
        self.assertTrue(adapter._uncoded_contact_failure(internal))
        network = protocol.urllib.error.URLError(ConnectionRefusedError(111, "Connection refused"))
        with mock.patch.object(protocol, "_urlopen", side_effect=network):
            with self.assertRaises(adapter.ProtocolError) as raised:
                protocol.RoomProtocol("https://room.example/api", "credential").request("GET", "/status")
        self.assertTrue(raised.exception.retryable and not raised.exception.code)

    def test_p7a_10_html_error_pages_are_failed_contacts(self):
        # The event read: the loop continues, the SSE route stays in use.
        room = html_page_room()
        instance, binding = watched(room)
        waits = Waits()
        with mock.patch.object(asyncio, "sleep", waits), self.assertLogs(adapter.logger, level="INFO"):
            outcome = run_watch(instance, binding, lambda: instance.consumed == ["evt-after-edge"])
        self.assertTrue(outcome["reached"], instance.consumed)
        self.assertFalse(outcome["exited"])
        self.assertNotEqual(binding.transport, "long_poll_fallback")
        self.assertEqual(len(waits.of("_watch")), 2)
        self.assertEqual(room.registrations, 1)

        # The heartbeat: the loop and the session stay; no registration.
        room = FakeRoom()
        room.heartbeat_errors = [edge_error(403), edge_error(404)]
        instance, binding = watched(room)
        instance._registered_rooms.add(binding.room_id)

        async def heartbeat():
            task = asyncio.create_task(instance._heartbeat_loop(binding, 0.01))
            while room.heartbeats < 2:
                await REAL_SLEEP(0.01)
            alive = not task.done()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            return alive

        with mock.patch.object(asyncio, "sleep", Waits()), self.assertLogs(adapter.logger, level="INFO"):
            alive = asyncio.run(asyncio.wait_for(heartbeat(), 20))
        self.assertTrue(alive)
        self.assertEqual(room.registrations, 0)
        self.assertIn(binding.room_id, instance._registered_rooms)

        # The post: an edge page leaves the reply retryable, never quarantined.
        for status in (403, 404, 413):
            error = edge_error(status)
            self.assertEqual(adapter.post_refusal_outcome(error.code, error.retryable), "retryable", status)

    def test_p7a_11_an_invalid_context_page_is_read_again(self):
        from test_parity_p1a import HUMAN, PEER, START

        invalid = {"events": [{**START, "id": "never", "seq": 40, "type": "message.posted",
                               "payload": {"body": "Never processed."}}, "garbage"]}
        reads = {"context": 0}
        event = {**HUMAN, "id": "m-44", "seq": 44}

        def events(_room, after, _wait):
            reads["context"] += 1
            if reads["context"] == 1:
                return invalid
            return {"events": [item for item in (START, HUMAN, PEER) if item["seq"] > after]}

        captured = []
        instance = object.__new__(adapter.SyntheticSocialityAdapter)
        instance.platform = adapter.Platform(adapter.NAME)
        instance._inflight_events, instance._event_seq, instance._event_epoch = set(), {}, {"m-44": "epoch-2"}
        instance._latest_source, instance._run_for_event, instance._epoch_handovers = {}, {}, {}
        instance._cycle_attempts = {}
        state = {"title": "Room", "headSeq": 60, "activeEpoch": {"id": "epoch-2", "startsAtSeq": 41}, "rules": [],
                 "roster": [{"membershipId": "owner-1", "displayName": "Owner"},
                            {"membershipId": "member-1", "displayName": "Agent"}]}
        api = types.SimpleNamespace(room_state=lambda _room: state, room_policy=lambda _room: {"policy": {}},
                                    events=events, artifacts=lambda _room: {"items": []}, artifact=lambda _r, _a: {})
        instance._call = lambda _binding, operation, **_request_options: asyncio.sleep(0, result=operation(api))
        instance._publish = lambda *_args, **_kwargs: asyncio.sleep(0)
        instance.build_source = types.MethodType(
            lambda self, **kwargs: types.SimpleNamespace(platform=self.platform, **kwargs), instance)
        instance.handle_message = lambda message: asyncio.sleep(0, result=captured.append(message))
        binding = adapter.RoomBinding("https://room.example/api", "room-1", "member-1", "credential",
                                      installation_id="inst-1", display_name="Agent")
        binding.epoch_session_routing_initialized = True
        binding.legacy_session_epoch_id = "epoch-1"
        # The invalid page is never processed: the dispatch stops before the model.
        with self.assertRaises(adapter.ProtocolError) as raised:
            asyncio.run(instance._dispatch_room_event(binding, event, "generation-1"))
        self.assertEqual(adapter._status_error_code(raised.exception), "invalid_event_page")
        self.assertEqual(captured, [])
        self.assertNotIn("m-44", instance._inflight_events)
        # Read again (as _watch does from the durable frontier): the turn is not lost.
        self.assertTrue(asyncio.run(instance._dispatch_room_event(binding, event, "generation-2")))
        self.assertEqual(len(captured), 1)
        self.assertNotIn("Never processed.", captured[0].text)

        # The loop does not end: the failure is retried on the schedule.
        room = FakeRoom()
        room.pending_events = [{"id": "evt-1", "type": "message.posted"}]
        instance, binding = watched(room)
        attempts = []

        async def consume(_binding, item):
            attempts.append(item["id"])
            if len(attempts) == 1:
                raise raised.exception
            instance.consumed.append(item["id"])

        instance._consume = consume
        original = room.stream_events

        def stream(room_id, after, on_event):
            if not room.pending_events and not instance.consumed:
                room.pending_events = [{"id": "evt-1", "type": "message.posted"}]
            return original(room_id, after, on_event)

        room.stream_events = stream
        waits = Waits()
        with mock.patch.object(asyncio, "sleep", waits), self.assertLogs(adapter.logger, level="INFO"):
            outcome = run_watch(instance, binding, lambda: instance.consumed == ["evt-1"])
        self.assertTrue(outcome["reached"], attempts)
        self.assertFalse(outcome["exited"])
        self.assertLessEqual(waits.of("_watch")[0], 1.0)

    def test_p7a_12_a_partial_outage_backs_off_without_registering(self):
        # The event read succeeds and the processing of its event fails every time.
        room = FakeRoom()
        instance, binding = watched(room)

        async def consume(_binding, _event):
            raise http_error(503)

        instance._consume = consume
        original = room.stream_events

        def stream(room_id, after, on_event):
            room.pending_events = [{"id": "evt-1", "type": "message.posted"}]
            return original(room_id, after, on_event)

        room.stream_events = stream
        waits = Waits()
        with mock.patch.object(asyncio, "sleep", waits), self.assertLogs(adapter.logger, level="INFO"):
            outcome = run_watch(instance, binding, lambda: len(waits.of("_watch")) >= 7)
        self.assertTrue(outcome["reached"])
        on_schedule(self, waits.of("_watch")[:7])
        self.assertEqual(room.registrations, 1)

        # OP-12 Hermes N4: every heartbeat fails and the stream fails while the
        # Room would accept a registration.
        room = FakeRoom()
        room.heartbeat_errors = Endless(lambda: http_error(502))
        room.stream_errors = Endless(lambda: http_error(502))
        instance, binding = watched(room)
        waits = Waits()
        with mock.patch.object(adapter, "HEARTBEAT_MIN_INTERVAL_SECONDS", 3600), \
                mock.patch.object(asyncio, "sleep", waits), self.assertLogs(adapter.logger, level="INFO"):
            outcome = run_watch(instance, binding, lambda: len(waits.of("_watch")) >= 7)
        self.assertTrue(outcome["reached"])
        on_schedule(self, waits.of("_watch")[:7])
        self.assertEqual(room.registrations, 1)

    def test_p7a_13_settle_and_acknowledgement_retry_inside_the_connector(self):
        attempt = cycle_attempt()
        instance, binding, api, _published, _dispatched = room_instance(attempt)
        settles, acks = [], []
        complete = api.complete_discussion_attempt
        acknowledge = api.acknowledge

        def flaky_complete(*args):
            settles.append(True)
            if len(settles) <= 2:
                raise http_error(502)
            return complete(*args)

        def flaky_acknowledge(room_id, seq):
            acks.append(seq)
            if len(acks) == 1:
                raise edge_error(503)
            if len(acks) == 2:
                raise adapter.ProtocolError("Room API unavailable: [Errno 104] Connection reset by peer", retryable=True)
            return acknowledge(room_id, seq)

        api.complete_discussion_attempt = flaky_complete
        api.acknowledge = flaky_acknowledge
        waits = Waits()

        async def run():
            await instance._complete_cycle_attempt(binding, attempt, "pass")
            await instance._complete_event_durably(binding, 5, terminal_status="skipped", source_id="evt-5",
                                                   reason="model_no_visible_reply")
            deadline = time.monotonic() + 10
            while binding.acknowledged_cursor < 5 and time.monotonic() < deadline:
                await REAL_SLEEP(0.01)

        with mock.patch.object(asyncio, "sleep", waits), self.assertLogs(adapter.logger, level="INFO"):
            asyncio.run(run())
        self.assertEqual(len(settles), 3)
        self.assertEqual(api.completions[-1][0], "attempt-1")
        self.assertEqual(binding.acknowledged_cursor, 5)
        self.assertEqual(api.acks, [5])
        self.assertLessEqual(waits.of("_complete_cycle_attempt")[0], 1.0)
        self.assertLessEqual(waits.of("retry")[0], 1.0)
        self.assertEqual(len(waits.of("_complete_cycle_attempt")) + len(waits.of("retry")), 4)

    def test_p7a_14_status_connection_precedence(self):
        with tempfile.TemporaryDirectory() as scratch:
            path = Path(scratch) / "status.json"
            status = local_status.LocalStatus(path, plugin="hermes", version=adapter.CONNECTOR_VERSION, host_label="h")
            seen = []

            def note():
                entry = local_status.read(path)["rooms"]["room-1"]
                seen.append((entry["connection"], entry["lastErrorCode"]))

            status.room("room-1", "member-1", connected=True, registered=True); note()
            status.room("room-1", connection="inactive", inactive=True, error_code="connector_not_active"); note()
            status.room("room-1", connected=True); note()  # a heartbeat while muted
            status.room("room-1", connection="reconnecting", error_code="network"); note()
            status.room("room-1", connected=True); note()  # up again, still muted
            status.room("room-1", error_code="credential_revoked")
            status.room("room-1", connection="stopped"); note()
            status.room("room-1", connected=True, registered=True); note()  # a new runtime registers
            status.room("room-1", connection="inactive", inactive=True, error_code="connector_not_active")
            status.room("room-1", inactive=False, connection="connected", error_code=""); note()  # unmuted
            self.assertEqual(seen, [
                ("connected", ""), ("inactive", "connector_not_active"), ("inactive", "connector_not_active"),
                ("reconnecting", "network"), ("inactive", "network"), ("stopped", "credential_revoked"),
                ("connected", "credential_revoked"), ("connected", ""),
            ])

    def test_p7a_15_heartbeat_retry_schedule_and_reset(self):
        room = FakeRoom()
        script = [False, False, True, False, True]
        heartbeat = room.heartbeat

        def scripted(room_id, session):
            with room.lock:
                calls = getattr(room, "calls", 0) + 1
                room.calls = calls
            if calls <= len(script) and not script[calls - 1]:
                raise http_error(503)
            return heartbeat(room_id, session)

        room.heartbeat = scripted
        instance, binding = watched(room)
        instance._registered_rooms.add(binding.room_id)
        waits = Waits()

        async def run():
            task = asyncio.create_task(instance._heartbeat_loop(binding, 60))
            while getattr(room, "calls", 0) < len(script) + 1:
                await REAL_SLEEP(0.01)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        with mock.patch.object(asyncio, "sleep", waits), self.assertLogs(adapter.logger, level="INFO"):
            asyncio.run(asyncio.wait_for(run(), 20))
        delays = waits.of("_heartbeat_loop")[:6]
        # interval, fail -> step 0, fail -> step 1, success -> interval, fail -> step 0 again, success -> interval
        self.assertEqual(delays[0], 60, delays)
        on_schedule(self, delays[1:3])
        self.assertEqual(delays[3], 60, delays)
        on_schedule(self, delays[4:5])
        self.assertEqual(delays[5], 60, delays)

    def test_p7a_16_renewal_and_reclaim_capped_at_5_s(self):
        # One function serves the renewal and the re-claim before a post.
        def lease_in(seconds):
            at = datetime.now(timezone.utc) + timedelta(seconds=seconds)
            return at.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        attempt = cycle_attempt(lease=lease_in(600))
        instance, binding, _api, _published, _dispatched = room_instance(attempt)
        outcomes = [http_error(503) for _ in range(6)] + [attempt]

        async def claim(_binding, _cycle, _bound=None):
            outcome = outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        instance._claim_discussion_attempt = claim
        waits = Waits()
        with mock.patch.object(asyncio, "sleep", waits), self.assertLogs(adapter.logger, level="WARNING"):
            self.assertIs(asyncio.run(instance._claim_held_attempt(binding, "evt-5", attempt)), attempt)
        on_schedule(self, waits.of("_claim_held_attempt")[:6], cap=5.0)
        self.assertEqual(len(waits.of("_claim_held_attempt")), 6)

        # The re-claim before a post goes through the same call.
        attempt = cycle_attempt(lease=lease_in(600))
        instance, binding, api, _published, _dispatched = room_instance(attempt)
        api.claim = lambda number: (_ for _ in ()).throw(http_error(503)) if number <= 6 else attempt
        waits = Waits()
        with mock.patch.object(asyncio, "sleep", waits), self.assertLogs(adapter.logger, level="WARNING"):
            result = asyncio.run(instance._send_final("room-1", adapter._dispatch_source_ref("evt-5", "g5"), "An answer."))
        self.assertTrue(result.success)
        self.assertEqual(len(api.posts), 1)
        on_schedule(self, waits.of("_claim_held_attempt")[:6], cap=5.0)

    def test_the_stream_loop_resets_after_a_full_cycle_never_after_a_registration(self):
        # A full cycle resets the schedule.
        room = FakeRoom()
        instance, binding = watched(room)
        script = [False, False, False, True, False, False]
        calls = []

        async def consume(_binding, event):
            calls.append(event["id"])
            if not script[min(len(calls), len(script)) - 1]:
                raise http_error(503)
            instance.consumed.append(event["id"])

        instance._consume = consume
        original = room.stream_events

        def stream(room_id, after, on_event):
            room.pending_events = [{"id": f"evt-{len(calls) + 1}", "type": "message.posted"}]
            return original(room_id, after, on_event)

        room.stream_events = stream
        waits = Waits()
        with mock.patch.object(asyncio, "sleep", waits), self.assertLogs(adapter.logger, level="INFO"):
            outcome = run_watch(instance, binding, lambda: len(waits.of("_watch")) >= 5)
        self.assertTrue(outcome["reached"])
        delays = waits.of("_watch")[:5]
        on_schedule(self, delays[:3])
        on_schedule(self, delays[3:5])  # the first step again after the full cycle

        # The Room says the session is lost at every heartbeat and accepts each
        # registration while the stream fails: the registrations reset nothing.
        room = FakeRoom()
        room.heartbeat_errors = Endless(lambda: http_error(404, code="not_found"))
        room.stream_errors = Endless(lambda: http_error(502))
        instance, binding = watched(room)
        waits = Waits()
        with mock.patch.object(adapter, "HEARTBEAT_MIN_INTERVAL_SECONDS", 3600), \
                mock.patch.object(asyncio, "sleep", waits), self.assertLogs(adapter.logger, level="INFO"):
            outcome = run_watch(instance, binding, lambda: len(waits.of("_watch")) >= 6)
        self.assertTrue(outcome["reached"])
        on_schedule(self, waits.of("_watch")[:6])
        self.assertGreaterEqual(room.registrations, 5)

    def test_web_read_uses_the_gateway_turn_selection(self):
        # OP-12 Hermes N2: a saved platform list without web tools gives a Room
        # turn no web tool, whatever the profile-wide selection has.
        try:
            from hermes_cli.tools_config import _get_platform_tools  # noqa: F401 - Hermes core
            import model_tools  # Hermes core
        except Exception as error:  # noqa: BLE001
            self.skipTest(f"Hermes core not importable: {type(error).__name__}")
        names = {"web": ["web_search", "web_extract"], "terminal": ["terminal"], "file": ["read_file"]}

        def definitions(enabled_toolsets=None, disabled_toolsets=None, quiet_mode=False, **_kwargs):
            chosen = [name for toolset in (enabled_toolsets or []) for name in names.get(toolset, [])
                      if toolset not in (disabled_toolsets or [])]
            return [{"function": {"name": name}} for name in chosen]

        def resolve(config):
            run = types.ModuleType("gateway.run")
            run._load_gateway_config = lambda: config
            with mock.patch.dict(sys.modules, {"gateway.run": run}), \
                    mock.patch.object(model_tools, "get_tool_definitions", definitions):
                return adapter._room_turn_tool_selection()

        without_web = resolve({"platform_toolsets": {adapter.NAME: ["terminal", "file"]}})
        self.assertIsNotNone(without_web)
        self.assertFalse(without_web & set(local_status.WEB_READ_TOOL_NAMES), without_web)
        with_web = resolve({"platform_toolsets": {adapter.NAME: ["terminal", "web"]}})
        self.assertIn("web_search", with_web)
        disabled = resolve({"platform_toolsets": {adapter.NAME: ["terminal", "web"]}, "agent": {"disabled_toolsets": ["web"]}})
        self.assertNotIn("web_search", disabled)
        # No gateway loader offered to the plugin: unknown, never the wider list.
        with mock.patch.dict(sys.modules, {"gateway.run": types.ModuleType("gateway.run")}):
            self.assertIsNone(adapter._room_turn_tool_selection())


if __name__ == "__main__":
    unittest.main()


class _Elsewhere(http.server.BaseHTTPRequestHandler):
    """Another host: a followed redirect would reach it."""

    def do_GET(self):  # noqa: N802 - http.server API
        self.server.hits += 1
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"sessionId": "elsewhere"}')

    do_POST = do_GET

    def log_message(self, *_args):
        pass


class _Faulty(http.server.BaseHTTPRequestHandler):
    """A redirect to another host, or a body that cannot be decoded."""

    def do_GET(self):  # noqa: N802 - http.server API
        length = int(self.headers.get("Content-Length") or 0)
        self.rfile.read(length)
        if self.server.kind == "redirect":
            self.send_response(302)
            self.send_header("Location", self.server.location)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        stream = "/events/stream" in self.path
        body = b"event: ready\ndata: \xff\xfe\xfd\n\n" if stream else b"\xff\xfe\xfd not text"
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream" if stream else "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_POST = do_GET

    def log_message(self, *_args):
        pass


class _NotHttp(socketserver.BaseRequestHandler):
    def handle(self):
        received = b""
        while b"\r\n\r\n" not in received:
            chunk = self.request.recv(4096)
            if not chunk:
                return
            received += chunk
        self.request.sendall(b"SSH-2.0-OpenSSH_9.6\r\n")


class TransportFaults:
    """Round 3 (OP-12 r2 B1): real urllib against local servers for a redirect
    to another host, an answer that is not HTTP and a body that cannot be
    decoded; a certificate failure as urllib raises it (a URLError whose reason
    is ssl.SSLCertVerificationError)."""

    KINDS = ("certificate", "redirect", "non-HTTP", "corrupt")

    def __enter__(self):
        self.elsewhere = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Elsewhere)
        self.elsewhere.hits = 0
        self.servers = {"non-HTTP": socketserver.ThreadingTCPServer(("127.0.0.1", 0), _NotHttp)}
        for kind in ("redirect", "corrupt"):
            server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Faulty)
            server.kind, server.location = kind, f"http://127.0.0.1:{self.elsewhere.server_port}/api/x"
            self.servers[kind] = server
        for server in (self.elsewhere, *self.servers.values()):
            server.daemon_threads = True
            threading.Thread(target=server.serve_forever, daemon=True).start()
        return self

    def __exit__(self, *_exc):
        for server in (self.elsewhere, *self.servers.values()):
            server.shutdown()
            server.server_close()

    def fail(self, kind, contact):
        """Make the real request of ``contact`` fail with ``kind``; raise what the client raises."""
        port = self.servers[kind].server_address[1] if kind in self.servers else 9
        api = protocol.RoomProtocol(f"http://127.0.0.1:{port}/api", "credential")
        request = (lambda: api.register_connector("room-1", "inst-1", {})) if contact == "register" else (
            lambda: api.stream_events("room-1", 0, lambda _event: True))
        if kind == "certificate":
            reason = ssl.SSLCertVerificationError(1, "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: "
                                                     "self-signed certificate (_ssl.c:1010)")
            with mock.patch.object(protocol, "_urlopen", side_effect=protocol.urllib.error.URLError(reason)):
                request()
        else:
            request()
        raise AssertionError(f"{kind}: the {contact} did not fail")


class RecordingTasks(dict):
    """A task map that remembers every task put in it (P7a-20 waits on them)."""

    def __init__(self):
        super().__init__()
        self.started = []

    def __setitem__(self, key, task):
        self.started.append(task)
        super().__setitem__(key, task)


def lost_session():
    """The Room's answer to a heartbeat on a session it does not hold (server
    c1f5c4a, deployed 2026-10-04T22:19Z: api/connector_sessions.go:218-219,
    ErrSessionNotFound): HTTP 404 with its JSON code not_found."""
    return edge_error(404, json.dumps({"code": "not_found", "message": "room or object not found",
                                       "requestId": "request-1"}).encode())


class P7aRound3Tests(unittest.TestCase):
    def test_p7a_17_transport_failures_are_failed_contacts(self):
        with TransportFaults() as faults:
            room = FakeRoom()
            room.register_errors = [lambda kind=kind: faults.fail(kind, "register") for kind in faults.KINDS]
            room.stream_errors = [lambda kind=kind: faults.fail(kind, "stream") for kind in faults.KINDS]
            room.pending_events = [{"id": "evt-after-transport", "type": "message.posted"}]
            instance, binding = watched(room)
            waits = Waits()
            with mock.patch.object(asyncio, "sleep", waits), self.assertLogs(adapter.logger, level="INFO") as logs:
                outcome = run_watch(instance, binding, lambda: instance.consumed == ["evt-after-transport"])
            self.assertTrue(outcome["reached"], instance.consumed)
            self.assertFalse(outcome["exited"])
            self.assertEqual(instance.connects, [])
            self.assertEqual(room.registrations, 1)
            self.assertEqual(room.log[:5], ["register failed"] * 4 + ["register"])
            self.assertEqual(faults.elsewhere.hits, 0)  # the redirect was never followed
            failures = waits.of("_watch")[:8]
            self.assertEqual(len(failures), 8, failures)
            on_schedule(self, failures)
            # Each was a failed contact (ProtocolError), none an unexpected error.
            self.assertFalse([line for line in logs.output if "connector loop failed" in line], logs.output)
            self.assertFalse([line for line in logs.output if "quarantin" in line.lower()], logs.output)
            for kind in faults.KINDS:  # what the client raises: a failed contact, retryable, no Room code
                for contact in ("register", "stream"):
                    with self.assertRaises(adapter.ProtocolError, msg=(kind, contact)) as raised:
                        faults.fail(kind, contact)
                    self.assertTrue(raised.exception.retryable and not raised.exception.code, (kind, contact))

    def test_p7a_18_a_lost_session_is_registered_again(self):
        # The heartbeat loop: the Room no longer holds the session; the
        # enrollment listing answers [] and does not ask for a registration.
        room = FakeRoom()
        room.heartbeat_errors = [lost_session()]
        room.register_errors = [http_error(502), http_error(502)]
        instance, binding = watched(room)
        instance._registered_rooms.add(binding.room_id)
        waits = Waits()

        async def heartbeat():
            task = asyncio.create_task(instance._heartbeat_loop(binding, 15))
            while "heartbeat" not in room.log:
                await REAL_SLEEP(0.01)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        with mock.patch.object(asyncio, "sleep", waits), self.assertLogs(adapter.logger, level="INFO"):
            asyncio.run(asyncio.wait_for(heartbeat(), 20))
        self.assertEqual(room.log[:5], ["heartbeat failed", "register failed", "register failed", "register", "heartbeat"])
        self.assertEqual(room.registrations, 1)
        retries = waits.of("_heartbeat_loop")[1:4]  # after the interval: the first step, then the schedule
        on_schedule(self, retries)
        self.assertEqual(binding.connector_session_id, "session-1")

        # The event-read path (the stream loop's own heartbeat, no heartbeat loop yet).
        room = FakeRoom()
        room.heartbeat_errors = [lost_session()]
        room.register_errors = [http_error(502), http_error(502)]
        room.pending_events = [{"id": "evt-after-restart", "type": "message.posted"}]
        instance, binding = watched(room)
        instance._registered_rooms.add(binding.room_id)
        waits = Waits()
        with mock.patch.object(asyncio, "sleep", waits), self.assertLogs(adapter.logger, level="INFO"):
            outcome = run_watch(instance, binding, lambda: instance.consumed == ["evt-after-restart"])
        self.assertTrue(outcome["reached"], instance.consumed)
        self.assertEqual(room.log[:4], ["heartbeat failed", "register failed", "register failed", "register"])
        self.assertEqual(room.registrations, 1)
        on_schedule(self, waits.of("_watch")[:2])

    def test_every_retry_loop_ends_on_the_stop_without_a_further_contact(self):
        # OP-12 r2 s4: the stop (disconnect sets it, then cancels) arrives
        # during a loop's retry wait; by its own check the loop ends without
        # contacting the Room again. A cancellation ends it at once.
        for mode in ("stop", "cancel"):
            with self.subTest(mode=mode):
                contacts = {}
                # The start and stream loop.
                room = FakeRoom()
                room.register_errors = Endless(lambda: http_error(502))
                instance, binding = watched(room)
                contacts["_watch"] = self._run_until_wait(instance, instance._watch(binding, is_reconnect=False),
                                                          "_watch", mode, lambda: room.log.count("register failed"))
                # The heartbeat loop.
                room = FakeRoom()
                room.heartbeat_errors = Endless(lambda: http_error(502))
                instance, binding = watched(room)
                instance._registered_rooms.add(binding.room_id)
                contacts["_heartbeat_loop"] = self._run_until_wait(
                    instance, instance._heartbeat_loop(binding, 15), "_heartbeat_loop", mode,
                    lambda: room.log.count("heartbeat failed"))
                # The settle retry.
                attempt = cycle_attempt()
                instance, binding, api, _published, _dispatched = room_instance(attempt)
                settles = []

                def failing_complete(*_args):
                    settles.append(True)
                    raise http_error(502)

                api.complete_discussion_attempt = failing_complete
                contacts["_complete_cycle_attempt"] = self._run_until_wait(
                    instance, instance._complete_cycle_attempt(binding, attempt, "pass"), "_complete_cycle_attempt",
                    mode, lambda: len(settles))
                # The acknowledgement retry of a durable skip.
                instance, binding, api, _published, _dispatched = room_instance(cycle_attempt())
                acks = []

                def failing_acknowledge(_room_id, seq):
                    acks.append(seq)
                    raise http_error(502)

                api.acknowledge = failing_acknowledge

                async def acknowledgement():
                    await instance._complete_event_durably(binding, 5, terminal_status="skipped", source_id="evt-5",
                                                           reason="model_no_visible_reply")
                    await instance._ack_retry_tasks[binding.room_id]

                contacts["retry"] = self._run_until_wait(instance, acknowledgement(), "retry", mode, lambda: len(acks))
                self.assertEqual(contacts, {"_watch": 1, "_heartbeat_loop": 1, "_complete_cycle_attempt": 1, "retry": 1})

    def _run_until_wait(self, instance, coroutine, caller, mode, count):
        """Run ``coroutine`` until ``caller``'s first retry wait; stop or cancel it there; return the contacts made."""
        waiting = asyncio.Event()

        def sleep(delay, result=None):
            if sys._getframe(1).f_code.co_name == caller and delay < 15:  # a retry wait, not the heartbeat interval
                if mode == "stop":
                    instance._stop.set()
                waiting.set()
                return REAL_SLEEP(0.002 if mode == "stop" else 30, result)
            return REAL_SLEEP(0.002 if delay > 0 else 0, result)

        async def run():
            task = asyncio.ensure_future(coroutine)
            await asyncio.wait_for(waiting.wait(), 10)
            if mode == "cancel":
                task.cancel()
            await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 10)
            await REAL_SLEEP(0.05)
            self.assertTrue(task.done())

        with mock.patch.object(asyncio, "sleep", sleep), mock.patch.object(adapter, "logger", mock.Mock()):
            asyncio.run(run())
        return count()


class P7aRound3AddendumTests(unittest.TestCase):
    def test_p7a_19_a_retried_settle_does_not_renew(self):
        # Hermes r2 B1: the settle after a post ran with the renewal alive.
        attempt = cycle_attempt()
        instance, binding, api, _published, _dispatched = room_instance(attempt)
        settles = []
        complete = api.complete_discussion_attempt

        def flaky_complete(*args):
            settles.append(api.claims)  # the claims made before each settle try
            if len(settles) <= 3:
                raise edge_error(502)  # no Room answer
            return complete(*args)

        api.complete_discussion_attempt = flaky_complete
        calls = []

        def sleep(delay, result=None):
            caller = sys._getframe(1).f_code.co_name
            calls.append((caller, float(delay)))
            # A settle wait is long enough for a live renewal to claim again.
            return REAL_SLEEP(0.05 if caller == "_complete_cycle_attempt" else 0.002, result)

        async def run():
            instance._start_attempt_renewal(binding, "evt-5", attempt)
            deadline = time.monotonic() + 10
            while api.claims < 2 and time.monotonic() < deadline:  # the renewal is alive and the claim succeeds
                await REAL_SLEEP(0.005)
            return await instance._send_final("room-1", adapter._dispatch_source_ref("evt-5", "g5"), "An answer.")

        with mock.patch.object(asyncio, "sleep", sleep), self.assertLogs(adapter.logger, level="INFO"):
            result = asyncio.run(run())
        self.assertTrue(result.success)
        self.assertEqual(len(api.posts), 1)
        self.assertEqual(len(settles), 4)
        self.assertGreaterEqual(settles[0], 2)
        self.assertEqual({*settles, api.claims}, {settles[0]})  # no renewal or re-claim once the settle started
        on_schedule(self, [delay for caller, delay in calls if caller == "_complete_cycle_attempt"])

    def test_p7a_20_a_revoked_or_expired_credential_ends_both_loops(self):
        # Hermes r2 B2: both loops end on the Room's JSON credential answer.
        self.maxDiff = None
        outcomes, expected = {}, {}
        for code in ("credential_revoked", "credential_expired"):
            for where in ("event read", "heartbeat"):
                with tempfile.TemporaryDirectory() as scratch:
                    path = Path(scratch) / "status.json"
                    room = FakeRoom()
                    refusal = edge_error(401, json.dumps({"code": code, "message": "refused"}).encode())
                    instance, binding = watched(room, path)
                    # P7c item 8 (r3 N8): no real-time deadline. Every loop task
                    # is recorded, and the stream double waits on an event set
                    # when the binding ends.
                    instance._tasks, instance._heartbeat_tasks = RecordingTasks(), RecordingTasks()
                    binding_ended = threading.Event()
                    if where == "event read":
                        instance._registered_rooms.add(binding.room_id)
                        room.stream_errors = [refusal]
                    else:
                        room.heartbeat_errors = [refusal]

                        def blocking_stream(_room_id, after, _on_event):
                            # The stream stays open until the binding ends.
                            binding_ended.wait(60)
                            return {"cursor": after, "ready": {"ok": True}}

                        room.stream_events = blocking_stream
                    ended, started = {}, []  # contacts counted where a loop starts them
                    for name in ("_call", "_stream_call"):
                        def contact(target, operation, original=getattr(instance, name), **request_options):
                            started.append(True)
                            return original(target, operation, **request_options)
                        setattr(instance, name, contact)
                    for name in ("_revoke", "_expire"):
                        def end(target, original=getattr(instance, name)):
                            ended.setdefault("contacts", len(started))
                            original(target)
                            binding_ended.set()
                        setattr(instance, name, end)
                    with mock.patch.object(adapter, "update", lambda change: change(types.SimpleNamespace(binding=lambda _room: None))), \
                            mock.patch.object(asyncio, "sleep", Waits()), mock.patch.object(adapter, "logger", mock.Mock()):
                        async def heartbeat_ended():
                            # Both loops end by themselves (run_watch would cancel
                            # them): wait for the tasks, not for a clock.
                            tasks = [*instance._tasks.started, *instance._heartbeat_tasks.started]
                            _done, pending = await asyncio.wait(tasks, timeout=60)
                            ended["loops"] = not pending and not instance._heartbeat_tasks and not instance._tasks
                            await REAL_SLEEP(0.05)

                        outcome = run_watch(instance, binding, lambda: bool(ended), timeout=60.0, after=heartbeat_ended)
                    entry = local_status.read(path)["rooms"]["room-1"]
                    key = f"{code} on the {where}"
                    outcomes[key] = (ended.get("loops", False), outcome["exited"], len(started) - ended.get("contacts", -1),
                                     entry["connection"], entry["lastErrorCode"])
                    expected[key] = (True, True, 0, "stopped", code)
        self.assertEqual(outcomes, expected)

    def test_the_sse_half_of_the_edge_page_rule(self):
        # Hermes r2 j2: an edge page on the stream route is a failed contact.
        for status in (401, 404):
            page = protocol.urllib.error.HTTPError("https://room.example/api/x", status, "edge", {},
                                                   io.BytesIO(b"<html><body>edge</body></html>"))
            with self.subTest(status=status), mock.patch.object(protocol, "_urlopen", side_effect=page):
                with self.assertRaises(adapter.ProtocolError) as raised:
                    protocol.RoomProtocol("https://room.example/api", "credential").stream_events("room-1", 0, lambda _e: True)
                error = raised.exception
                self.assertTrue(error.non_json and error.retryable and not error.code)
                self.assertFalse(error.hard_sse_unavailable or error.revoked or error.session_lost)

    def test_the_long_poll_page_is_validated_before_any_event(self):
        # Hermes r2 k3: an invalid page is never processed, not even its first event.
        room = FakeRoom()
        room.events = lambda *_args, **_kwargs: {"events": [{"id": "evt-valid", "seq": 6}, "garbage"]}
        instance, binding = watched(room)
        with self.assertRaises(protocol.InvalidEventPage):
            asyncio.run(instance._long_poll_once(binding))
        self.assertEqual(instance.consumed, [])

    def test_a_lease_run_out_registers_again(self):
        # Hermes r2 n5: no answer said "lost", but the lease ran out locally.
        room = FakeRoom()
        room.heartbeat_errors = [http_error(502)]
        instance, binding = watched(room)
        instance._registered_rooms.add(binding.room_id)
        instance._lease_deadline[binding.room_id] = 1.0  # long past on the loop's clock

        async def heartbeat():
            task = asyncio.create_task(instance._heartbeat_loop(binding, 15))
            deadline = time.monotonic() + 10
            while room.registrations < 1 and time.monotonic() < deadline:
                await REAL_SLEEP(0.01)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        with mock.patch.object(asyncio, "sleep", Waits()), self.assertLogs(adapter.logger, level="INFO"):
            asyncio.run(heartbeat())
        self.assertEqual(room.log[:2], ["heartbeat failed", "register"])

    def test_the_stream_loop_resets_when_the_failed_contact_succeeds(self):
        # Hermes r2 s: the contact that last failed succeeds, then another
        # fails in the same pass: the schedule is at its first step again.
        class Room(FakeRoom):
            def register_connector(self, *args):
                result = super().register_connector(*args)
                protocol._contact_succeeded("contact-register")
                return result

        room = Room()
        room.register_errors = [adapter.ProtocolError("down", retryable=True, contact="contact-register") for _ in range(3)]
        room.stream_errors = [adapter.ProtocolError("down", retryable=True, contact="contact-stream")]
        room.pending_events = [{"id": "evt-after", "type": "message.posted"}]
        instance, binding = watched(room)
        waits = Waits()
        with mock.patch.object(asyncio, "sleep", waits), self.assertLogs(adapter.logger, level="INFO"):
            outcome = run_watch(instance, binding, lambda: instance.consumed == ["evt-after"])
        self.assertTrue(outcome["reached"])
        failures = waits.of("_watch")[:4]
        on_schedule(self, failures[:3])
        self.assertLessEqual(failures[3], 1.0, failures)
