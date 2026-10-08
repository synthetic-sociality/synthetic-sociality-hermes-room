"""Parity P8 "Idle cadence": contract section P8 (conformance ``parityP8Cases``;
OpenClaw implements the same names).

An idle installation with several Rooms in two join lineages runs for simulated
minutes on a fake clock: ``asyncio.sleep``, the adapter's P8 clock, the status
file's timer and the status time all follow it, and no test waits in real time.
The Room is a double that records each call with its simulated time. No test
uses a real credential or talks to a live Room.
"""

from __future__ import annotations

import asyncio
import contextlib
import heapq
import itertools
import json
import os
import shutil
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from test_delivery_lifecycle import ROOT, adapter

CONFORMANCE = json.loads((ROOT / "conformance.json").read_text())
PACKAGE = adapter.__name__.rsplit(".", 1)[0]
state_module = sys.modules[PACKAGE + ".state"]
local_status = adapter.local_status
renewal = adapter.renewal
REAL_SLEEP = asyncio.sleep
REAL_TO_THREAD = asyncio.to_thread
REAL_REPLACE = os.replace
REAL_READ_TEXT = Path.read_text
START = 1000.0
LEASE = "2099-01-01T00:00:00Z"
BASE = "https://room.example/api"
STREAM_PASS = 20.0  # an event-stream pass of an idle Room (the long poll's 20 s)


class VirtualClock:
    """Simulated time: sleeps and timers wake in time order, none in real time."""

    def __init__(self, start: float = START) -> None:
        self.now = start
        self._queue: list[list] = []
        self._order = itertools.count()

    def __call__(self) -> float:
        return self.now

    def iso(self) -> str:
        return datetime.fromtimestamp(1_790_000_000 + self.now, timezone.utc).isoformat(
            timespec="seconds").replace("+00:00", "Z")

    def schedule(self, delay: float, callback):
        entry = [self.now + max(0.0, delay), next(self._order), callback, False]
        heapq.heappush(self._queue, entry)
        return lambda: entry.__setitem__(3, True)

    async def sleep(self, delay, result=None):
        if delay <= 0:
            await REAL_SLEEP(0)
            return result
        future = asyncio.get_running_loop().create_future()
        self.schedule(delay, lambda: future.done() or future.set_result(None))
        await future
        return result

    async def settle(self) -> None:
        for _ in range(80):
            await REAL_SLEEP(0)

    async def run_until(self, end: float) -> None:
        while True:
            await self.settle()
            while self._queue and self._queue[0][3]:
                heapq.heappop(self._queue)
            if not self._queue or self._queue[0][0] > end:
                self.now = max(self.now, end)
                await self.settle()
                return
            when, _order, callback, _cancelled = heapq.heappop(self._queue)
            self.now = max(self.now, when)
            callback()


async def immediate(function, *args, **kwargs):
    return function(*args, **kwargs)


@contextlib.contextmanager
def simulated(clock: VirtualClock):
    with mock.patch.object(asyncio, "sleep", clock.sleep), \
            mock.patch.object(asyncio, "to_thread", immediate), \
            mock.patch.object(adapter, "_clock", clock, create=True), \
            mock.patch.object(local_status, "_now", clock.iso):
        yield


class IdleRoom:
    """The Room as an idle connector sees it; every call recorded with its time."""

    def __init__(self, clock: VirtualClock, interval: int = 15) -> None:
        self.clock = clock
        self.interval = interval
        self.calls: list[tuple[float, str, str]] = []  # (time, route, room of the credential)
        self.heartbeat_answers: dict[str, list] = {}
        self.pending: dict[str, list] = {}  # installation -> enrollments
        self.muted: set[str] = set()
        self.claims: dict[str, dict] = {}
        self.sessions = itertools.count(1)

    def api(self, binding):
        room = self
        room_id = binding.room_id

        class API:
            def status(self):
                return {"protocolCapabilities": []}

            def register_connector(self, _room, _installation, _metadata):
                room.calls.append((room.clock.now, "register", room_id))
                return {"sessionId": f"session-{next(room.sessions)}", "leaseExpiresAt": LEASE,
                        "heartbeatIntervalSeconds": room.interval}

            def heartbeat(self, _room, _session):
                room.calls.append((room.clock.now, "heartbeat", room_id))
                script = room.heartbeat_answers.get(room_id)
                if script:
                    answer = script.pop(0)
                    if isinstance(answer, Exception):
                        raise answer
                    return answer
                return {"leaseExpiresAt": LEASE}

            def connector_enrollments(self, installation):
                room.calls.append((room.clock.now, "enrollments", room_id))
                if room_id in room.muted:
                    raise adapter.ProtocolError("an active connector session is required", status=401,
                                                code="connector_not_active", retryable=True)
                pending, room.pending[installation] = room.pending.get(installation, []), []
                return pending

            def claim_connector_enrollment(self, invitation, _installation):
                room.calls.append((room.clock.now, "claim", room_id))
                return room.claims[invitation]

            def room_state(self, _room):
                room.calls.append((room.clock.now, "state", room_id))
                status = "muted" if room_id in room.muted else "active"
                return {"roster": [{"membershipId": binding.membership_id, "status": status}],
                        "activeEpoch": {"id": "epoch-1", "startsAtSeq": 1}}

            def activity(self, _room, payload):
                room.calls.append((room.clock.now, "activity:" + payload["kind"], room_id))
                return {"acceptedStreamSeq": payload["streamSeq"]}

            def disconnect(self, _room, _session, _will_reconnect):
                room.calls.append((room.clock.now, "disconnect", room_id))
                return {}

        return API()

    def times(self, route, room_id=None, since=float("-inf"), until=float("inf")):
        return [at for at, name, room in self.calls
                if name == route and (room_id is None or room == room_id) and since <= at < until]

    def rooms_of(self, route, since=float("-inf"), until=float("inf")):
        return [room for at, name, room in self.calls if name == route and since <= at < until]


def binding_for(room_id: str, installation: str) -> "adapter.RoomBinding":
    return adapter.RoomBinding(BASE, room_id, "member-" + room_id, "credential-" + room_id,
                               installation_id=installation, connector_session_id="session-stored",
                               credential_expires_at="2099-01-01T00:00:00Z")


def idle_instance(room: IdleRoom, bindings, *, status=None, real_state: bool = False):
    """A connector instance whose Room calls go to ``room``; stream passes take STREAM_PASS."""
    instance = object.__new__(adapter.SyntheticSocialityAdapter)
    for name, value in {
        "_stop": asyncio.Event(), "_tasks": {}, "_heartbeat_tasks": {}, "_lease_deadline": {},
        "_registered_rooms": set(), "_message_contract_checked": set(), "_effective_transports": {},
        "_stream_generations": {}, "_receive_locks": {}, "_renewing_rooms": set(), "_host_label": "host-p8",
        "_status": status, "_state": state_module.PluginState(bindings=list(bindings)),
        "_active_dispatch_rooms": set(), "_renewal_retry_after": {},
    }.items():
        setattr(instance, name, value)

    async def noop(*_args, **_kwargs):
        return None

    async def stream_pass(_binding):
        await asyncio.sleep(STREAM_PASS)

    async def refreshed(_binding):
        return True

    instance._expire_stale_pending = noop
    instance._ensure_epoch_session_routing = noop
    instance._repair_pending_lifecycles = noop
    instance._transport_cycle = stream_pass
    if not real_state:
        instance._refresh_binding = refreshed
        instance._persist_binding = lambda _binding: True
    instance._mark_connected = lambda: None
    instance._mark_disconnected = lambda: None
    instance._call = lambda binding, operation: REAL_SLEEP(0, result=operation(room.api(binding)))
    instance.connect = lambda **_kwargs: None
    return instance


async def start(instance, bindings):
    for binding in bindings:
        instance._tasks[binding.room_id] = asyncio.create_task(instance._watch(binding, is_reconnect=False))


async def stop(instance):
    instance._stop.set()
    tasks = [*instance._tasks.values(), *instance._heartbeat_tasks.values()]
    renewal_task = getattr(instance, "_renewal_task_p8", None)
    if renewal_task is not None:
        tasks.append(renewal_task)
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def run(coroutine_function):
    return asyncio.run(coroutine_function())


def lineages():
    return ([binding_for(room, "inst-A") for room in ("a1", "a2", "a3")],
            [binding_for(room, "inst-B") for room in ("b1", "b2")])


class P8ConformanceTests(unittest.TestCase):
    def test_p8_00_cases_and_constants_are_pinned(self):
        cases = CONFORMANCE.get("parityP8Cases", {})
        self.assertEqual(len(cases), 7)
        for index, (name, method) in enumerate(cases.items(), start=1):
            self.assertTrue(name.startswith(f"P8-{index:02d} "), name)
            self.assertTrue(hasattr(P8IdleCadenceTests, method), method)
        self.assertEqual(CONFORMANCE.get("parityP8Contract"),
                         "docs/delivery-lifecycle-contract-v1.md#p8--idle-cadence-2026-10-08")
        self.assertEqual(CONFORMANCE.get("parityP8Constants"), {
            "heartbeatMaximumSeconds": getattr(adapter, "HEARTBEAT_MAX_INTERVAL_SECONDS", None),
            "enrollmentListingIntervalSeconds": getattr(adapter, "ENROLLMENT_LISTING_INTERVAL_SECONDS", None),
            "enrollmentPollAfterBoundsSeconds": [getattr(adapter, "ENROLLMENT_POLL_AFTER_MIN_SECONDS", None),
                                                 getattr(adapter, "ENROLLMENT_POLL_AFTER_MAX_SECONDS", None)],
            "renewalIntentHealthySeconds": getattr(adapter, "RENEWAL_INTENT_HEALTHY_SECONDS", None),
            "renewalExpiryHorizonSeconds": getattr(adapter, "RENEWAL_EXPIRY_HORIZON_SECONDS", None),
            "idleActivityFrameSeconds": getattr(adapter, "IDLE_ACTIVITY_FRAME_SECONDS", None),
            "connectedAtWriteSeconds": getattr(local_status, "CONNECTED_AT_WRITE_SECONDS", None),
        })


class P8IdleCadenceTests(unittest.TestCase):
    def idle_run(self, minutes: float = 11, *, status=None, before=None, during=None):
        """Two lineages (3 + 2 Rooms) idle for ``minutes``; returns the Room double."""
        clock = VirtualClock()
        room = IdleRoom(clock)
        group_a, group_b = lineages()
        bindings = group_a + group_b

        async def body():
            instance = idle_instance(room, bindings, status=status(clock) if status else None)
            if before is not None:
                before(instance, room, clock)
            await start(instance, bindings)
            if during is None:
                await clock.run_until(START + minutes * 60)
            else:
                await during(instance, room, clock, bindings)
            await stop(instance)
            return instance

        with simulated(clock):
            instance = run(body)
        return room, clock, instance

    def test_p8_01_an_idle_session_sends_exactly_one_heartbeat_per_interval_capped_at_60_s(self):
        room, _clock, _instance = self.idle_run()
        window = dict(since=START + 60, until=START + 660)
        for room_id in ("a1", "a2", "a3", "b1", "b2"):
            beats = room.times("heartbeat", room_id, **window)
            # 15 s from the session answer: 40 heartbeats in ten minutes, none
            # from a stream pass (a pass ends every 20 s).
            self.assertEqual(len(beats), 40, (room_id, beats[:8]))
            self.assertEqual({round(later - earlier, 6) for earlier, later in zip(beats, beats[1:])}, {15.0})

        # An instructed 120 s is used as 60 s; a later answer's 20 s takes effect.
        clock = VirtualClock()
        room = IdleRoom(clock, interval=120)
        binding = binding_for("c1", "inst-C")
        room.heartbeat_answers["c1"] = [{"leaseExpiresAt": LEASE}] * 3 + [
            {"leaseExpiresAt": LEASE, "heartbeatIntervalSeconds": 20}]

        async def body():
            instance = idle_instance(room, [binding])
            await start(instance, [binding])
            await clock.run_until(START + 400)
            await stop(instance)

        with simulated(clock):
            run(body)
        registered = room.times("register", "c1")[0]
        beats = [registered] + room.times("heartbeat", "c1")
        gaps = [round(later - earlier, 6) for earlier, later in zip(beats, beats[1:])]
        self.assertEqual(gaps[:4], [60.0, 60.0, 60.0, 60.0], gaps)
        self.assertEqual(set(gaps[4:8]), {20.0}, gaps)
        self.assertLessEqual(max(gaps), 60.0)

    def test_p8_02_n_rooms_of_one_join_lineage_make_one_listing_per_interval_two_lineages_in_one_process_make_two(self):
        room, _clock, _instance = self.idle_run()
        window = dict(since=START + 60, until=START + 660)
        listed = room.rooms_of("enrollments", **window)
        self.assertEqual(len([name for name in listed if name.startswith("a")]), 10, listed)
        self.assertEqual(len([name for name in listed if name.startswith("b")]), 10, listed)
        for lineage in ("a", "b"):
            times = sorted(at for at, route, name in room.calls
                           if route == "enrollments" and name.startswith(lineage) and window["since"] <= at < window["until"])
            self.assertTrue(all(later - earlier >= 60.0 - 1e-9 for earlier, later in zip(times, times[1:])), times)
        # The listing comes round every binding of the lineage.
        self.assertEqual({name for name in listed if name.startswith("a")}, {"a1", "a2", "a3"})
        self.assertEqual({name for name in listed if name.startswith("b")}, {"b1", "b2"})
        # Directly after every accepted registration: each Room at its start ...
        for room_id in ("a1", "a2", "a3", "b1", "b2"):
            self.assertIn(room.times("register", room_id)[0], room.times("enrollments", room_id))

        # ... and after a registration again (a heartbeat 404), with that binding.
        def lost(instance, room, clock):
            room.heartbeat_answers["a2"] = [{"leaseExpiresAt": LEASE}] * 20 + [
                adapter.ProtocolError("room or object not found", status=404, code="not_found")]

        room, _clock, _instance = self.idle_run(8, before=lost)
        again = room.times("register", "a2")[1]
        self.assertIn(again, room.times("enrollments", "a2"))
        # The listing answer as an object: pollAfterSeconds is the interval, bounded.
        self.assertEqual(adapter.enrollment_listing({"enrollments": [], "rooms": [], "pollAfterSeconds": 120}), ([], 120.0))
        self.assertEqual(adapter.enrollment_listing({"enrollments": [], "pollAfterSeconds": 1}), ([], 15.0))
        self.assertEqual(adapter.enrollment_listing({"enrollments": [], "pollAfterSeconds": 9999}), ([], 300.0))
        self.assertEqual(adapter.enrollment_listing([{"invitationId": "i"}]), ([{"invitationId": "i"}], None))
        for malformed in ({"rooms": []}, {"enrollments": {}}, None, "x"):
            with self.assertRaises(adapter.ProtocolError):
                adapter.enrollment_listing(malformed)

    def test_p8_03_a_new_room_is_claimed_at_the_next_listing_a_mute_at_the_listing_with_its_credential_revoked_and_lost_sessions_at_the_heartbeat(self):
        statuses = {}

        def status(clock):
            directory = tempfile.mkdtemp(prefix="p8-03-")
            self.addCleanup(shutil.rmtree, directory, True)
            statuses["path"] = Path(directory) / "status.json"
            return local_status.LocalStatus(statuses["path"], plugin="hermes", version=adapter.CONNECTOR_VERSION,
                                            host_label="h", clock=clock, schedule=clock.schedule)

        observed = {}

        async def during(instance, room, clock, bindings):
            await clock.run_until(START + 200)
            # The owner approves a new Room for lineage A, and mutes a2.
            room.pending["inst-A"] = [{"invitationId": "invite-new"}]
            room.claims["invite-new"] = {"roomId": "a4", "membershipId": "member-a4", "credential": "credential-a4",
                                         "headSeq": 0}
            room.muted.add("a2")
            # A lost session (b1) and a revoked credential (b2) at the next heartbeat.
            room.heartbeat_answers["b1"] = [adapter.ProtocolError("room or object not found", status=404, code="not_found")]
            room.heartbeat_answers["b2"] = [adapter.ProtocolError("revoked", status=401, code="credential_revoked")]
            with mock.patch.object(adapter, "update", lambda mutator: mutator(state_module.PluginState())):
                await clock.run_until(START + 600)
            observed["status"] = local_status.read(statuses["path"])

        room, _clock, _instance = self.idle_run(status=status, during=during)
        lineage_a = [at for at, route, name in room.calls if route == "enrollments" and name[0] == "a"
                     and START + 200 <= at < START + 600]
        claim = room.times("claim", since=START + 200)[0]
        # Claimed at the lineage's next listing, within one interval; no listing in between.
        self.assertEqual(claim, lineage_a[0])
        self.assertLessEqual(claim - (START + 200), 60.0)
        self.assertEqual(len([at for at in lineage_a if at < claim]), 0)
        before = max(at for at, route, name in room.calls if route == "enrollments" and name[0] == "a" and at < claim)
        self.assertGreaterEqual(claim - before, 60.0 - 1e-9, "the lineage's listing, once per interval")
        self.assertTrue(room.times("register", "a4"), "the claimed Room starts")
        # The mute is seen at the listing made with a2's credential (one of the
        # lineage's turns), and the other bindings keep the lineage's listing.
        muted_listing = room.times("enrollments", "a2", since=START + 200)[0]
        self.assertLessEqual(muted_listing - (START + 200), 4 * 60.0)
        self.assertEqual(observed["status"]["rooms"]["a2"]["connection"], "inactive")
        self.assertEqual(room.times("enrollments", "a2", since=muted_listing + 1), [])
        self.assertTrue(room.times("enrollments", "a1", since=muted_listing + 1)
                        or room.times("enrollments", "a3", since=muted_listing + 1))
        # The lost session registers again at its next heartbeat (P2c-3a: within 1 s).
        lost_at = room.times("heartbeat", "b1", since=START + 200)[0]
        self.assertLessEqual(lost_at - (START + 200), 15.0)
        self.assertLessEqual(room.times("register", "b1", since=lost_at)[0] - lost_at, 1.0)
        # The revoked credential stops b2 at its heartbeat.
        revoked_at = room.times("heartbeat", "b2", since=START + 200)[0]
        self.assertLessEqual(revoked_at - (START + 200), 15.0)
        self.assertEqual(room.times("heartbeat", "b2", since=revoked_at + 1), [])
        self.assertEqual((observed["status"]["rooms"]["b2"]["connection"],
                          observed["status"]["rooms"]["b2"]["lastErrorCode"]), ("stopped", "credential_revoked"))

    def test_p8_04_a_healthy_binding_asks_for_the_renewal_intent_once_per_300_s_otherwise_at_the_present_cadence(self):
        clock = VirtualClock()
        soon = datetime.fromtimestamp(datetime.now(timezone.utc).timestamp() + 3600, timezone.utc)
        bindings = {
            "healthy": binding_for("healthy", "inst-R"),
            "no-expiry": binding_for("no-expiry", "inst-R"),
            "unparsable": binding_for("unparsable", "inst-R"),
            "soon": binding_for("soon", "inst-R"),
            "expired": binding_for("expired", "inst-R"),
            "rotation": binding_for("rotation", "inst-R"),
        }
        bindings["no-expiry"].credential_expires_at = ""
        bindings["unparsable"].credential_expires_at = "next spring"
        bindings["soon"].credential_expires_at = soon.isoformat().replace("+00:00", "Z")
        bindings["expired"].enabled, bindings["expired"].disabled_reason = False, "credential_expired"
        bindings["rotation"].credential_rotation = {"owner_initiated": True}
        asked, resumed = [], []

        def discover(binding):
            asked.append((clock.now, binding.room_id))
            return None

        def resume(_path, room_id, _intent):
            resumed.append((clock.now, room_id))

        with tempfile.TemporaryDirectory() as scratch:
            path = Path(scratch) / "synthetic-sociality-room" / "state.json"
            state_module.save(state_module.PluginState(bindings=list(bindings.values())), path)

            async def body():
                instance = idle_instance(IdleRoom(clock), [])
                instance._state_file = path
                instance._renewal_task_p8 = asyncio.create_task(instance._renewal_loop())
                await clock.run_until(START + 660)
                await stop(instance)

            with simulated(clock), mock.patch.object(renewal, "discover", discover), \
                    mock.patch.object(renewal, "resume", resume):
                run(body)
        per_room = {name: [at for at, room_id in asked + resumed if room_id == name] for name in bindings}
        ticks = len(per_room["soon"])
        self.assertGreaterEqual(ticks, 660 // 15)  # the present cadence: every 15 s tick
        for name in ("healthy", "no-expiry", "unparsable"):
            # Healthy (an absent or unparsable expiry too): at most once per 300 s.
            self.assertEqual(per_room[name], [START, START + 300, START + 600], name)
        for name in ("expired", "rotation"):
            self.assertEqual(len(per_room[name]), ticks, name)

    def test_p8_05_an_idle_session_posts_an_activity_frame_no_more_often_than_half_the_announced_freshness(self):
        room, _clock, instance = self.idle_run()
        interval = getattr(adapter, "IDLE_ACTIVITY_FRAME_SECONDS", 20.0)
        self.assertEqual(interval, 20.0)
        self.assertEqual(adapter.idle_activity_frame_interval(), 20.0)
        for room_id in ("a1", "a2", "a3", "b1", "b2"):
            frames = room.times("activity:heartbeat", room_id)
            # The registration frame, with the registration.
            self.assertEqual(frames[0], room.times("register", room_id)[0])
            gaps = [later - earlier for earlier, later in zip(frames, frames[1:])]
            self.assertTrue(gaps)
            # Never more often than the half (20 s), never further apart than
            # the half plus one heartbeat interval (15 s).
            self.assertGreaterEqual(min(gaps), interval - 1e-9, gaps)
            self.assertLessEqual(max(gaps), interval + 15 + 1e-9, gaps)
            self.assertLessEqual(len(room.times("activity:heartbeat", room_id, since=START + 60, until=START + 660)),
                                 600 / interval)

        # Turn frames are posted at once, whatever the idle schedule.
        clock = VirtualClock()
        room = IdleRoom(clock)
        binding = binding_for("t1", "inst-T")

        async def body():
            instance = idle_instance(room, [binding])
            await start(instance, [binding])
            await clock.run_until(START + 5)
            await instance._publish(binding, "evt-1", "context_acknowledged", source_seq=7)
            await instance._publish(binding, "evt-1", "lifecycle", status="preparing_response")
            await instance._publish(binding, "evt-1", "terminal", status="completed")
            await stop(instance)

        with simulated(clock):
            run(body)
        self.assertEqual([route for at, route, _room in room.calls if route.startswith("activity:") and at == START + 5],
                         ["activity:context_acknowledged", "activity:lifecycle", "activity:terminal"])

    def test_p8_06_status_json_is_not_rewritten_for_heartbeats_alone_within_60_s_any_other_change_at_once(self):
        writes: list[float] = []
        holder = {}

        def status(clock):
            directory = tempfile.mkdtemp(prefix="p8-06-")
            self.addCleanup(shutil.rmtree, directory, True)
            holder["path"] = path = Path(directory) / "status.json"
            holder["clock"] = clock
            return local_status.LocalStatus(path, plugin="hermes", version=adapter.CONNECTOR_VERSION,
                                            host_label="h", clock=clock, schedule=clock.schedule)

        def replace(source, target, *args, **kwargs):
            if str(target) == str(holder.get("path")):
                writes.append(holder["clock"].now)
            return REAL_REPLACE(source, target, *args, **kwargs)

        observed = {}

        async def during(instance, room, clock, bindings):
            await clock.run_until(START + 600)
            before = len(writes)
            # Every other change at once: a Room's connection and its error code.
            instance._status_room(bindings[0], connection="reconnecting", error_code="network")
            observed["at_once"] = (len(writes) - before, local_status.read(holder["path"])["rooms"]["a1"])
            instance._status_room(bindings[0], connection="connected", error_code="")
            observed["back"] = local_status.read(holder["path"])["rooms"]["a1"]
            await clock.run_until(START + 660)
            # A clean stop writes the newest values.
            instance._status.set_loaded(False)
            observed["stopped"] = (local_status.read(holder["path"]), instance._status.snapshot())

        with mock.patch.object(os, "replace", replace):
            room, _clock, instance = self.idle_run(status=status, during=during)
        window = [at for at in writes if START + 60 <= at < START + 600]
        # Heartbeats alone (5 Rooms, 4 a minute): at most one write a minute.
        self.assertLessEqual(len(window), 9, window)
        self.assertGreaterEqual(len(window), 8, window)
        gaps = [later - earlier for earlier, later in zip(window, window[1:])]
        self.assertTrue(all(59.999 <= gap <= 60.001 for gap in gaps), gaps)  # updatedAt at most 60 s old
        count, entry = observed["at_once"]
        self.assertEqual(count, 1)
        self.assertEqual((entry["connection"], entry["lastErrorCode"]), ("reconnecting", "network"))
        self.assertEqual((observed["back"]["connection"], observed["back"]["lastErrorCode"]), ("connected", ""))
        written, memory = observed["stopped"]
        self.assertFalse(written["pluginLoaded"])
        self.assertEqual({room_id: entry["lastConnectedAt"] for room_id, entry in written["rooms"].items()},
                         {room_id: entry["lastConnectedAt"] for room_id, entry in memory["rooms"].items()})

    def test_p8_07_the_state_file_is_neither_parsed_nor_written_while_unchanged_and_an_outside_change_is_picked_up(self):
        clock = VirtualClock()
        room = IdleRoom(clock)
        bindings = [binding_for("s1", "inst-S"), binding_for("s2", "inst-S")]
        reads: list[float] = []
        writes: list[float] = []
        observed = {}
        with tempfile.TemporaryDirectory() as scratch:
            path = Path(scratch) / "synthetic-sociality-room" / "state.json"
            state_module.save(state_module.PluginState(bindings=bindings), path)

            def read_text(self, *args, **kwargs):
                if str(self) == str(path):
                    reads.append(clock.now)
                return REAL_READ_TEXT(self, *args, **kwargs)

            def replace(source, target, *args, **kwargs):
                if str(target) == str(path):
                    writes.append(clock.now)
                return REAL_REPLACE(source, target, *args, **kwargs)

            async def body():
                running = state_module.load(path).bindings
                instance = idle_instance(room, running, real_state=True)
                instance._state_file = path
                await start(instance, running)
                await clock.run_until(START + 600)
                observed["idle"] = ([at for at in reads if at >= START + 120], [at for at in writes if at >= START + 120])
                # Another process (the CLI: hermes room disable) changes the file.
                observed["outside_at"] = clock.now

                def disable(state):
                    state.binding("s1").enabled = False
                    return True

                state_module.update(disable, path)
                reads.clear()
                writes.clear()
                await clock.run_until(START + 600 + 25)
                observed["after"] = (list(reads), list(writes))
                observed["s1_done"] = instance._tasks["s1"].done() and "s1" not in instance._heartbeat_tasks
                observed["s2_running"] = not instance._tasks["s2"].done()
                await stop(instance)

            with simulated(clock), mock.patch.dict(os.environ, {"HERMES_HOME": scratch}), \
                    mock.patch.object(Path, "read_text", read_text), mock.patch.object(os, "replace", replace):
                run(body)
        idle_reads, idle_writes = observed["idle"]
        # Eight minutes of heartbeats and stream passes: no parse, no write.
        self.assertEqual(idle_reads, [])
        self.assertEqual(idle_writes, [])
        after_reads, after_writes = observed["after"]
        # The outside change is read at the next tick, once per change, and ends s1.
        self.assertTrue(after_reads)
        self.assertLessEqual(after_reads[0] - observed["outside_at"], 15.0)
        self.assertLessEqual(len(after_reads), 4)
        self.assertEqual(after_writes, [])
        self.assertTrue(observed["s1_done"])
        self.assertTrue(observed["s2_running"])
        self.assertTrue(room.times("disconnect", "s1"))


if __name__ == "__main__":
    unittest.main()
