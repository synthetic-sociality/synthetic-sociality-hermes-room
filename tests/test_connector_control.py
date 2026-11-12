"""P17: owner connector control (Reconnect agent / Restart agent), Hermes 1.0.104.

Plugin B (card t_1a1a6733). The Room server delivers an owner command in an
authenticated register or heartbeat answer (``connectorControl``). The
connector claims it once per process, fences new work, refuses while any work
of the installation is in progress, and then either reconnects every Room
listener from one reload of the state file or asks its own supervisor for the
drain-aware restart (SIGUSR1 to the gateway process).

These tests drive the real adapter from a real three-binding state file (two
enabled Rooms, one stopped with ``credential_expired``) behind a loopback fake
of the Room's connector routes. Restart requests go to a simulated supervisor:
a child Python process that records SIGUSR1 to a temp file. Nothing here
signals the test process, a gateway or any Hermes process.
"""
from __future__ import annotations

import asyncio
import copy
import json
import os
import signal
import stat
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest
import uuid
from pathlib import Path
from unittest import mock

from test_delivery_lifecycle import adapter, room_protocol, state_store
from test_binding_split import ROOM_A, ROOM_B, SOURCE, _binding


ROOM_C = "room-c"
CAPABILITY = "connector.control.v1"
ANSWER = {"leaseExpiresAt": "2099-01-01T00:00:00Z", "heartbeatIntervalSeconds": 10}
COUNTS = {"bindings": 3, "enabled": 2, "disabled": 1}
SECRETS = ("credential-room-a", "credential-room-b", "credential-room-c")

SUPERVISOR = textwrap.dedent("""
    import pathlib, signal, sys, time
    record, ready = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
    def restart(_signum, _frame):
        record.write_text("SIGUSR1\\n")
        sys.exit(0)
    signal.signal(signal.SIGUSR1, restart)
    ready.write_text("ready")
    time.sleep(60)
""")


def _command(action: str, *, state: str = "requested", command_id: str = "") -> dict:
    return {
        "commandId": command_id or f"cmd-{uuid.uuid4().hex}", "action": action, "state": state,
        "requestedAt": "2026-11-12T08:00:00Z", "expiresAt": "2099-11-12T08:02:00Z",
    }


def _conflict(code: str):
    return adapter.ProtocolError(code, status=409, code=code)


class FakeRoom:
    """Loopback fake of the Room connector routes this lane uses."""

    def __init__(self) -> None:
        self.registrations: list[tuple[str, dict]] = []
        self.claims: list[tuple[str, str, str]] = []
        self.reports: list[tuple[str, str, dict]] = []
        self.disconnects: list[str] = []
        self.attempt_claims: list[str] = []
        self.claim_errors: dict[str, BaseException] = {}
        self.posted: list[tuple[str, str]] = []
        self.during_report = None

    def phases(self, command_id: str) -> list[tuple[str, str]]:
        return [(report["phase"], report.get("code", "")) for _room, command, report in self.reports
                if command == command_id]

    def api(self, binding):
        room = self
        room_id = binding.room_id

        class API:
            def status(self):
                return {"protocolCapabilities": []}

            def register_connector(self, registered_room, _client_instance, metadata):
                room.registrations.append((registered_room, dict(metadata)))
                return {"sessionId": f"session-{registered_room}", **ANSWER}

            def heartbeat(self, _room, _session):
                return dict(ANSWER)

            def disconnect(self, disconnected_room, _session, _will_reconnect):
                room.disconnects.append(disconnected_room)
                return {}

            def claim_connector_control(self, claimed_room, session_id, command_id):
                room.claims.append((claimed_room, session_id, command_id))
                error = room.claim_errors.get(command_id)
                if error is not None:
                    raise error
                return {"command": {"commandId": command_id, "state": "claimed"}}

            def report_connector_control(self, reported_room, _session_id, command_id, report):
                room.reports.append((reported_room, command_id, copy.deepcopy(report)))
                return {"command": {"commandId": command_id, "state": report.get("phase")}}

            # The send path of tests/test_binding_split.py.
            def claim_discussion_attempt(self, _room, cycle_id):
                room.attempt_claims.append(cycle_id)
                return None

            def room_state(self, _room):
                return {"headSeq": 5, "activeEpoch": {"id": "epoch-1", "startsAtSeq": 1}}

            def post_message(self, *args, **_kwargs):
                room.posted.append((room_id, args[4]))
                return {"id": "posted-6", "seq": 6, "ts": "2026-11-11T00:00:00Z"}

            def complete_discussion_attempt(self, *_args):
                return {"state": "completed"}

            def acknowledge(self, _room, seq):
                return {"acknowledgedSeq": seq}

        return API()


class ConnectorControlFixture(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._directory = tempfile.TemporaryDirectory()
        self.addCleanup(self._directory.cleanup)
        self.path = Path(self._directory.name) / "profile-state" / "state.json"
        self.path.parent.mkdir(mode=0o700)
        bindings = [_binding(ROOM_A, "member-a", "installation-a"),
                    _binding(ROOM_B, "member-b", "installation-b"),
                    _binding(ROOM_C, "member-c", "installation-c")]
        bindings[0].connector_session_id = f"session-{ROOM_A}"
        bindings[1].connector_session_id = f"session-{ROOM_B}"
        bindings[2].enabled = False
        bindings[2].disabled_reason = "credential_expired"
        state_store.save(adapter.PluginState(bindings=bindings), self.path)
        scope = state_store.state_path_scope(self.path)
        scope.__enter__()
        self.addCleanup(scope.__exit__, None, None, None)
        patcher = mock.patch.object(adapter, "CONTROL_RECONNECT_WAIT_SECONDS", 3.0, create=True)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.room = FakeRoom()
        self.requests: list[str] = []
        self.watched: dict[str, list] = {}
        self.instance = self._adapter()
        self.listeners = {binding.room_id: binding for binding in self.instance._state.bindings}
        self.old_tasks = await self._start_listeners(self.instance, [ROOM_A, ROOM_B])

    def _adapter(self):
        instance = adapter.SyntheticSocialityAdapter(adapter.PlatformConfig())
        self.assertEqual(instance._state_file, self.path)
        room = self.room

        async def call(binding, operation, **_options):
            result = operation(room.api(binding))
            if room.during_report is not None and room.reports and room.reports[-1][2].get("_seen") is None:
                room.reports[-1][2]["_seen"] = True
                await room.during_report()
            return result

        async def watch(binding, *, is_reconnect):
            # A listener: it registers its session (forced after a reconnect),
            # then idles; it leaves _tasks on exit like the real _watch.
            self.watched.setdefault(binding.room_id, []).append(binding)
            try:
                await instance._ensure_connector(binding)
                await asyncio.Event().wait()
            finally:
                instance._tasks.pop(binding.room_id, None)

        def requester():
            self.requests.append("restart")
            raise RuntimeError("no supervisor in this test")

        instance._mark_connected = lambda: None
        instance._mark_disconnected = lambda: None
        instance._call = call
        instance._watch = watch
        instance._restart_requester = requester
        instance._message_contract_checked.update({ROOM_A, ROOM_B, ROOM_C})
        self.addAsyncCleanup(self._stop_adapter, instance)
        return instance

    async def _start_listeners(self, instance, rooms):
        """The adapter as connect() leaves it: one listener and heartbeat per enabled Room."""
        async def idle(room_id, registry):
            try:
                await asyncio.Event().wait()
            finally:
                registry.pop(room_id, None)

        tasks = {}
        for room_id in rooms:
            instance._tasks[room_id] = asyncio.create_task(idle(room_id, instance._tasks))
            instance._heartbeat_tasks[room_id] = asyncio.create_task(idle(room_id, instance._heartbeat_tasks))
            instance._registered_rooms.add(room_id)
            instance._room_map("_registrations")[room_id] = 1
            tasks[room_id] = (instance._tasks[room_id], instance._heartbeat_tasks[room_id])
        await asyncio.sleep(0)
        return tasks

    @staticmethod
    async def _stop_adapter(instance):
        tasks = [*instance._tasks.values(), *instance._heartbeat_tasks.values(),
                 *getattr(instance, "_control_tasks", set())]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def deliver(self, room_id, command, *, instance=None, registered=False):
        instance = instance or self.instance
        instance._note_connected(instance._binding(room_id), {**ANSWER, "connectorControl": command},
                                 registered=registered)

    async def settle(self, instance=None):
        instance = instance or self.instance
        for _ in range(200):
            await asyncio.sleep(0)
            pending = [task for task in getattr(instance, "_control_tasks", set()) if not task.done()]
            if not pending:
                break
            await asyncio.wait(pending, timeout=0.05)
        for _ in range(5):
            await asyncio.sleep(0)

    def assert_listeners_untouched(self):
        for room_id, (listener, heartbeat) in self.old_tasks.items():
            with self.subTest(untouched=room_id):
                self.assertIs(self.instance._tasks.get(room_id), listener)
                self.assertIs(self.instance._heartbeat_tasks.get(room_id), heartbeat)
                self.assertFalse(listener.done())
        self.assertEqual(self.watched, {})

    def receipt(self):
        status = self.path.parent / "status.json"
        return json.loads(status.read_text()).get("connectorControl") if status.exists() else None

    def assert_no_secret(self, value):
        text = json.dumps(value, sort_keys=True)
        for secret in SECRETS:
            self.assertNotIn(secret, text)

        def keys(node):
            if isinstance(node, dict):
                for key, child in node.items():
                    yield key
                    yield from keys(child)
            elif isinstance(node, list):
                for child in node:
                    yield from keys(child)

        for key in keys(value):
            self.assertNotIn(key, {"credential", "credential_rotation", "credentialRotation", "secret"})


class ConnectorControlTests(ConnectorControlFixture):
    # P17-01 ---------------------------------------------------------------
    async def test_registration_reports_installation_identity_version_and_control_capability(self):
        instance = self.instance
        for room_id in (ROOM_A, ROOM_B):
            instance._registered_rooms.discard(room_id)
            await instance._ensure_connector(instance._binding(room_id))
        record_path = self.path.parent / "installation.json"
        self.assertTrue(record_path.is_file(), "installation.json is created at the first registration")
        record = json.loads(record_path.read_text())
        self.assertRegex(record["installationId"], r"^hermes-inst-[0-9a-f]{32}$")
        self.assertEqual(stat.S_IMODE(record_path.stat().st_mode), 0o600)
        first = record_path.read_bytes()
        for room_id, metadata in self.room.registrations:
            with self.subTest(room=room_id):
                self.assertEqual(metadata["installationId"], record["installationId"])
                self.assertEqual(metadata["connectorVersion"], adapter.CONNECTOR_VERSION)
                # Existing metadata stays as it was.
                self.assertEqual(metadata["runtimeName"], "Hermes Agent")
        # One identity per installation: a new process reports the same id and
        # never rotates the file.
        again = self._adapter()
        again._registered_rooms.discard(ROOM_A)
        await again._ensure_connector(again._binding(ROOM_A))
        self.assertEqual(self.room.registrations[-1][1]["installationId"], record["installationId"])
        self.assertEqual(record_path.read_bytes(), first)
        # The registration requests the control capability next to the existing ones.
        with mock.patch.object(room_protocol.RoomProtocol, "request", autospec=True,
                               return_value={"sessionId": "s"}) as request:
            room_protocol.RoomProtocol("https://room.example/api", "c").register_connector(
                ROOM_A, "installation-a", {"runtimeName": "Hermes Agent"})
        capabilities = request.call_args.args[3]["capabilities"]
        self.assertIn(CAPABILITY, capabilities)
        self.assertIn("events.sse", capabilities)
        self.assertIn(room_protocol.MESSAGE_LOGICAL_CONTRIBUTION_CAPABILITY, capabilities)

    # P17-02 ---------------------------------------------------------------
    async def test_malformed_or_absent_connector_control_is_ignored_and_the_loop_continues(self):
        binding = self.instance._binding(ROOM_A)
        malformed = [
            None, "reconnect", 7, [], {}, {"action": "reconnect", "state": "requested"},
            {"commandId": 5, "action": "reconnect", "state": "requested"},
            {"commandId": "../../etc", "action": "reconnect", "state": "requested"},
            {"commandId": "cmd-shell", "action": "shutdown", "state": "requested"},
            {"commandId": "cmd-shell", "action": "restart; rm -rf /", "state": "requested"},
            {"commandId": "cmd-shell", "action": ["restart"], "state": "requested"},
            {"commandId": "cmd-shell", "action": "restart", "state": 3},
        ]
        self.instance._note_connected(binding, dict(ANSWER))
        for value in malformed:
            with self.subTest(value=value):
                self.instance._note_connected(binding, {**ANSWER, "connectorControl": value})
        await self.settle()
        self.assertEqual(self.room.claims, [])
        self.assertEqual(self.room.reports, [])
        self.assertEqual(self.requests, [])
        self.assert_listeners_untouched()
        # The loop keeps working: the next well-formed command is claimed.
        command = _command("reconnect")
        self.room.claim_errors[command["commandId"]] = _conflict("control_claimed")
        self.deliver(ROOM_B, command)
        await self.settle()
        self.assertEqual(self.room.claims, [(ROOM_B, f"session-{ROOM_B}", command["commandId"])])

    # P17-03 ---------------------------------------------------------------
    async def test_one_claim_per_command_across_bindings_and_409_answers_are_ignored(self):
        for code in ("control_claimed", "control_closed"):
            with self.subTest(code=code):
                self.room.claims.clear()
                command = _command("restart")
                self.room.claim_errors[command["commandId"]] = _conflict(code)
                self.deliver(ROOM_A, command)
                self.deliver(ROOM_B, command)
                await self.settle()
                self.deliver(ROOM_A, command)
                await self.settle()
                self.assertEqual(self.room.claims, [(ROOM_A, f"session-{ROOM_A}", command["commandId"])])
                self.assertEqual(self.room.reports, [])
                self.assertEqual(self.requests, [])
                self.assertIsNone(self.receipt())
                self.assert_listeners_untouched()

    async def test_protocol_claim_and_report_routes(self):
        with mock.patch.object(room_protocol.RoomProtocol, "request", autospec=True,
                               return_value={"command": {}}) as request:
            api = room_protocol.RoomProtocol("https://room.example/api", "c")
            api.claim_connector_control("room a", "session/1", "cmd-1")
            api.report_connector_control("room a", "session/1", "cmd-1", {"phase": "reconnecting"})
        claim, report = request.call_args_list
        self.assertEqual(claim.args[1:3], ("POST", "/rooms/room%20a/connector/sessions/session%2F1/control/cmd-1/claim"))
        self.assertEqual(report.args[1:4], ("POST", "/rooms/room%20a/connector/sessions/session%2F1/control/cmd-1/report",
                                            {"phase": "reconnecting"}))

    # P17-04 ---------------------------------------------------------------
    async def test_work_in_progress_anywhere_in_the_installation_refuses_at_once(self):
        instance = self.instance
        blocker = asyncio.create_task(asyncio.Event().wait())
        self.addCleanup(blocker.cancel)

        def active_dispatch():
            instance._active_dispatch_rooms[ROOM_B] = "dispatch-generation"
            return lambda: instance._active_dispatch_rooms.clear()

        def inflight_event():
            instance._inflight_events.add("evt-in-flight")
            return lambda: instance._inflight_events.clear()

        def submission():
            instance._submission_tasks["evt-submitting"] = blocker
            return lambda: instance._submission_tasks.clear()

        for label, arrange in (("active dispatch", active_dispatch), ("in-flight event", inflight_event),
                               ("submission task", submission)):
            for action in ("reconnect", "restart"):
                with self.subTest(work=label, action=action):
                    undo = arrange()
                    command = _command(action)
                    started = time.monotonic()
                    self.deliver(ROOM_A, command)
                    await self.settle()
                    undo()
                    self.assertLess(time.monotonic() - started, 2.0, "refusal never waits for the work")
                    self.assertEqual(self.room.phases(command["commandId"]), [("refused", "work_in_progress")])
                    self.assertEqual(self.requests, [])
                    self.assert_listeners_untouched()

    # P17-05 ---------------------------------------------------------------
    async def test_pending_renewal_or_credential_rotation_refuses(self):
        instance = self.instance

        def renewing():
            instance._renewing_rooms.add(ROOM_B)
            return lambda: instance._renewing_rooms.clear()

        def rotation():
            state_store.update(lambda current: setattr(
                current.binding(ROOM_B), "credential_rotation", {"owner_initiated": True, "phase": "redeemed"}))

            def undo():
                state_store.update(lambda current: setattr(current.binding(ROOM_B), "credential_rotation", {}))
            return undo

        for label, arrange in (("renewal pending", renewing), ("credential_rotation", rotation)):
            with self.subTest(renewal=label):
                undo = arrange()
                command = _command("reconnect")
                self.deliver(ROOM_A, command)
                await self.settle()
                undo()
                self.assertEqual(self.room.phases(command["commandId"]), [("refused", "work_in_progress")])
                self.assert_listeners_untouched()

    # P17-06 ---------------------------------------------------------------
    async def test_unfinished_delivery_intent_in_another_room_refuses_but_plain_quarantine_does_not(self):
        intent = {"state": "prepared", "delivery_state": "not_started", "lifecycle_state": "not_started"}
        state_store.update(lambda current: current.binding(ROOM_B).delivery_intents.update({"evt-9": intent}))
        command = _command("restart")
        self.deliver(ROOM_A, command)
        await self.settle()
        self.assertEqual(self.room.phases(command["commandId"]), [("refused", "work_in_progress")])
        self.assertEqual(self.requests, [])
        self.assert_listeners_untouched()
        # Quarantined evidence is inert: it never blocks control (and is kept).
        quarantined = {"state": "quarantined", "delivery_state": "quarantined", "lifecycle_state": "not_started"}
        state_store.update(lambda current: current.binding(ROOM_B).delivery_intents.update({"evt-9": quarantined}))
        state_store.update(lambda current: current.binding(ROOM_B).inbox.update({"9": "quarantined"}))
        before = self.path.read_bytes()
        command = _command("reconnect")
        self.deliver(ROOM_A, command)
        await self.settle()
        self.assertEqual(self.room.phases(command["commandId"])[0], ("reconnecting", ""))
        self.assertEqual(state_store.load(self.path).binding(ROOM_B).delivery_intents["evt-9"], quarantined)
        self.assertEqual(json.loads(self.path.read_text())["bindings"][1]["inbox"],
                         json.loads(before)["bindings"][1]["inbox"])

    # P17-07 ---------------------------------------------------------------
    async def test_fence_blocks_new_dispatch_attempt_claims_and_renewal_during_the_guard(self):
        instance = self.instance
        binding = instance._binding(ROOM_A)
        dispatched: list[int] = []

        async def dispatch(_binding, event, _generation):
            dispatched.append(event["seq"])
            return True

        async def active(_binding, **_kwargs):
            return True

        instance._dispatch_room_event = dispatch
        instance._room_session_busy = lambda _room: False
        instance._membership_active = active
        instance._queued_events = {ROOM_A: {7: {"id": "evt-7", "seq": 7}}}
        instance._active_dispatch_rooms[ROOM_B] = "dispatch-generation"
        observed = {}

        async def during_report():
            await instance._dispatch_next_queued(binding)
            claim = asyncio.create_task(instance._claim_discussion_attempt(binding, "cycle-7"))
            for _ in range(10):
                await asyncio.sleep(0)
            with mock.patch.object(adapter.renewal, "discover", return_value=None) as discover:
                await instance._maintain_renewal(binding)
            observed.update(dispatched=list(dispatched), attempt_claims=list(self.room.attempt_claims),
                            renewal=discover.call_count, claim=claim)

        self.room.during_report = during_report
        command = _command("restart")
        self.deliver(ROOM_A, command)
        await self.settle()
        self.assertEqual(self.room.phases(command["commandId"]), [("refused", "work_in_progress")])
        self.assertIn("claim", observed, "the refusal is reported while the fence is held")
        self.assertEqual(observed["dispatched"], [], "no new dispatch starts while fenced")
        self.assertEqual(observed["attempt_claims"], [], "no new turn attempt is claimed while fenced")
        self.assertEqual(observed["renewal"], 0, "no renewal starts while fenced")
        # The fence lifts with the refusal; the durable work then proceeds.
        await asyncio.wait_for(observed["claim"], 2)
        await self.settle()
        self.assertEqual(dispatched, [7])
        self.assertEqual(self.room.attempt_claims, ["cycle-7"])

    # P17-08 ---------------------------------------------------------------
    async def test_reconnect_reloads_once_and_starts_one_listener_per_enabled_binding(self):
        instance = self.instance
        stopped = instance._binding(ROOM_C)
        old = [task for pair in self.old_tasks.values() for task in pair]
        real_load = adapter.load
        loads: list[bool] = []

        def counting_load(path=None):
            loads.append(all(task.done() for task in old) and not self.watched)
            return real_load(path)

        command = _command("reconnect")
        with mock.patch.object(adapter, "load", counting_load):
            self.deliver(ROOM_A, command)
            self.deliver(ROOM_B, command)
            await self.settle()
        self.assertEqual(len(self.room.claims), 1)
        self.assertEqual(loads.count(True), 1, "the state file is reloaded once, after every listener stopped")
        self.assertTrue(all(task.done() for task in old), "every Room listener and heartbeat was stopped")
        self.assertEqual(sorted(self.watched), [ROOM_A, ROOM_B])
        for room_id in (ROOM_A, ROOM_B):
            with self.subTest(room=room_id):
                self.assertEqual(len(self.watched[room_id]), 1, "exactly one listener per enabled binding")
                self.assertIs(instance._binding(room_id), self.watched[room_id][0])
                self.assertIs(instance._binding(room_id), self.listeners[room_id], "adopted into the live object")
                self.assertIn(room_id, instance._tasks)
        # Disabled stays disabled, untouched and without a listener.
        self.assertIs(instance._binding(ROOM_C), stopped)
        self.assertFalse(stopped.enabled)
        self.assertEqual(stopped.disabled_reason, "credential_expired")
        self.assertNotIn(ROOM_C, instance._tasks)
        # Re-registration was forced for every enabled binding.
        self.assertEqual(sorted(room for room, _metadata in self.room.registrations), [ROOM_A, ROOM_B])
        reports = [report for _room, cid, report in self.room.reports if cid == command["commandId"]]
        self.assertEqual([report["phase"] for report in reports], ["reconnecting", "completed"])
        self.assertEqual(reports[-1]["inventory"], {"before": COUNTS, "after": COUNTS, "match": True})
        receipt = self.receipt()
        self.assertEqual((receipt["commandId"], receipt["action"], receipt["phase"]),
                         (command["commandId"], "reconnect", "reported"))
        self.assert_no_secret(receipt)

    async def test_reconnect_reports_incomplete_when_a_binding_does_not_register_again(self):
        instance = self.instance

        async def silent(binding, *, is_reconnect):
            self.watched.setdefault(binding.room_id, []).append(binding)
            try:
                await asyncio.Event().wait()
            finally:
                instance._tasks.pop(binding.room_id, None)

        instance._watch = silent
        command = _command("reconnect")
        with mock.patch.object(adapter, "CONTROL_RECONNECT_WAIT_SECONDS", 0.2, create=True):
            self.deliver(ROOM_A, command)
            await self.settle()
        self.assertEqual(self.room.phases(command["commandId"]),
                         [("reconnecting", ""), ("failed", "reconnect_incomplete")])
        self.assertEqual(sorted(self.watched), [ROOM_A, ROOM_B])

    # P17-09 ---------------------------------------------------------------
    async def test_reconnect_cures_the_split_and_a_sibling_turn_is_accepted(self):
        instance = self.instance
        # 1.0.101: the plugin's state was replaced, so each listener kept an
        # object the send path no longer read.
        instance._state = state_store.load(self.path)
        self.assertIsNot(instance._binding(ROOM_B), self.listeners[ROOM_B])
        command = _command("reconnect")
        self.deliver(ROOM_A, command)
        await self.settle()
        self.assertEqual([phase for phase, _code in self.room.phases(command["commandId"])],
                         ["reconnecting", "completed"])
        listeners = {room_id: self.watched[room_id][0] for room_id in (ROOM_A, ROOM_B)}
        for room_id, listener in listeners.items():
            with self.subTest(room=room_id):
                self.assertIs(instance._binding(room_id), listener)
        # The send path of tests/test_binding_split.py on the listener's object.
        instance._binding_generation_active = lambda _binding: True
        instance._persist_binding = lambda _binding: True
        instance._event_seq = {ROOM_B: {SOURCE: 5}}
        instance._event_epoch = {SOURCE: "epoch-1"}
        instance._source_coordination_modes = {SOURCE: "open"}
        instance._cycle_response_sources = {SOURCE: "evt-human"}
        instance._run_for_event = {SOURCE: "run-1"}
        instance._publish = lambda *_args, **_kwargs: asyncio.sleep(0)
        listener = listeners[ROOM_B]
        attempt = {"cycle": {"id": "cycle-b", "generation": 3},
                   "attempt": {"id": "attempt-b", "membershipId": listener.membership_id,
                               "leaseExpiresAt": "2099-08-29T00:00:00Z"}}
        listener.delivery_authority[adapter._attempt_authority_key(listener, SOURCE, attempt)] = \
            adapter._attempt_authority_record(listener, SOURCE, attempt)
        instance._cycle_attempts = {SOURCE: attempt}
        result = await instance.send(ROOM_B, "reply in room-b",
                                     reply_to=adapter._dispatch_source_ref(SOURCE, "g-b"), metadata={"notify": True})
        self.assertTrue(result.success, getattr(result, "error", None))
        self.assertEqual(self.room.posted, [(ROOM_B, "reply in room-b")])
        intent = listener.delivery_intents.get(SOURCE, {})
        self.assertNotEqual(intent.get("last_error_code"), "attempt_authority_missing")

    # P17-10 ---------------------------------------------------------------
    def _supervisor(self):
        """A simulated supervisor: a child process that records SIGUSR1."""
        directory = Path(self._directory.name)
        record, ready = directory / "supervisor-record.txt", directory / "supervisor-ready.txt"
        child = subprocess.Popen([sys.executable, "-I", "-c", SUPERVISOR, str(record), str(ready)])

        def stop():
            if child.poll() is None:
                child.kill()
            child.wait(10)

        self.addCleanup(stop)
        deadline = time.monotonic() + 15
        while not ready.exists():
            self.assertLess(time.monotonic(), deadline, "simulated supervisor did not start")
            self.assertIsNone(child.poll())
            time.sleep(0.02)
        self.assertNotEqual(child.pid, os.getpid())
        return child, record

    @staticmethod
    def _wait_for_record(child, record):
        deadline = time.monotonic() + 15
        while not record.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        child.wait(15)
        return record.read_text() if record.exists() else ""

    async def test_restart_persists_the_receipt_before_the_simulated_supervisor_is_asked(self):
        instance = self.instance
        child, record = self._supervisor()
        seen: list[dict] = []
        state_before = self.path.read_bytes()

        def requester():
            seen.append({"receipt": self.receipt(), "listeners": dict(instance._tasks)})
            adapter.signal_supervisor_restart(child.pid)

        instance._restart_requester = requester
        command = _command("restart")
        self.deliver(ROOM_A, command)
        await self.settle()
        self.assertEqual(self.room.phases(command["commandId"]), [("restarting", "")],
                         "the old process never reports completion")
        self.assertEqual(len(seen), 1)
        receipt = seen[0]["receipt"]
        self.assertIsNotNone(receipt, "the receipt is in status.json before the supervisor is asked")
        self.assertEqual((receipt["commandId"], receipt["action"], receipt["phase"]),
                         (command["commandId"], "restart", "restarting"))
        self.assertEqual(receipt["pid"], os.getpid())
        self.assertEqual(receipt["requestedAt"], command["requestedAt"])
        self.assertTrue(receipt["startedAt"])
        self.assertEqual(receipt["before"]["counts"], COUNTS)
        self.assertEqual(sorted(item["roomId"] for item in receipt["before"]["bindings"]), [ROOM_A, ROOM_B, ROOM_C])
        self.assert_no_secret(receipt)
        self.assertEqual(seen[0]["listeners"], {}, "a clean drain precedes the supervisor request")
        self.assertEqual(sorted(self.room.disconnects), [ROOM_A, ROOM_B])
        self.assertEqual(stat.S_IMODE((self.path.parent / "status.json").stat().st_mode), 0o600)
        self.assertEqual(self._wait_for_record(child, record), "SIGUSR1\n")
        self.assertEqual(self.watched, {}, "no listener restarts while the supervisor restarts the gateway")
        self.assertEqual(self.path.read_bytes(), state_before)

    async def test_default_requester_signals_only_its_own_gateway_process(self):
        # The default asks the gateway it runs in (SIGUSR1 to os.getpid()). Here
        # os.getpid() names the simulated supervisor, never the test process.
        child, record = self._supervisor()
        with mock.patch("os.getpid", return_value=child.pid), \
                mock.patch("signal.getsignal", return_value=lambda *_args: None):
            adapter.request_gateway_restart()
        self.assertEqual(self._wait_for_record(child, record), "SIGUSR1\n")
        # Without the gateway's SIGUSR1 handler (the default action would end
        # the process), it refuses and sends nothing.
        with mock.patch("os.kill") as kill, mock.patch("signal.getsignal", return_value=signal.SIG_DFL):
            with self.assertRaises(Exception):
                adapter.request_gateway_restart()
        kill.assert_not_called()

    # P17-11 ---------------------------------------------------------------
    async def test_requester_failure_reports_supervisor_unavailable_and_restores_listeners(self):
        instance = self.instance
        command = _command("restart")
        self.deliver(ROOM_A, command)
        await self.settle()
        self.assertEqual(self.requests, ["restart"])
        self.assertEqual(self.room.phases(command["commandId"]),
                         [("restarting", ""), ("failed", "supervisor_unavailable")])
        self.assertFalse(instance._stop.is_set())
        self.assertEqual(sorted(self.watched), [ROOM_A, ROOM_B])
        for room_id in (ROOM_A, ROOM_B):
            with self.subTest(room=room_id):
                self.assertEqual(len(self.watched[room_id]), 1)
                self.assertIs(instance._binding(room_id), self.watched[room_id][0])
                self.assertIn(room_id, instance._tasks)
        self.assertNotIn(ROOM_C, instance._tasks)
        receipt = self.receipt()
        self.assertEqual((receipt["phase"], receipt.get("code")), ("reported", "supervisor_unavailable"))

    # P17-12 ---------------------------------------------------------------
    async def _restart_then_new_process(self, between=None):
        command = _command("restart")
        self.instance._restart_requester = lambda: self.requests.append("restart")
        state_before = self.path.read_bytes()
        self.deliver(ROOM_A, command)
        await self.settle()
        self.assertEqual(self.requests, ["restart"])
        self.assertEqual(self.path.read_bytes(), state_before, "control never writes the state file")
        if between is not None:
            between()
        getattr(adapter, "_CONTROL_COMMANDS_SEEN", set()).clear()  # a new process
        self.room.reports.clear()
        self.watched.clear()
        successor = self._adapter()
        return command, successor

    async def test_post_restart_reports_completion_once_and_never_restarts_again(self):
        command, successor = await self._restart_then_new_process()
        await successor._ensure_connector(successor._binding(ROOM_A))
        await self.settle(successor)
        reports = [report for _room, cid, report in self.room.reports if cid == command["commandId"]]
        self.assertEqual([(report["phase"], report.get("code", "")) for report in reports], [("completed", "")])
        self.assertEqual(reports[0]["inventory"], {"before": COUNTS, "after": COUNTS, "match": True})
        # Further registrations and a stale answer neither report again nor
        # restart again.
        await successor._ensure_connector(successor._binding(ROOM_B))
        self.deliver(ROOM_B, {**command, "state": "restarting"}, instance=successor)
        self.deliver(ROOM_B, {**command, "state": "requested"}, instance=successor)
        await self.settle(successor)
        self.assertEqual(len(self.room.reports), 1)
        self.assertEqual(self.room.claims, [(ROOM_A, f"session-{ROOM_A}", command["commandId"])])
        self.assertEqual(self.requests, ["restart"])
        receipt = self.receipt()
        self.assertEqual((receipt["commandId"], receipt["phase"]), (command["commandId"], "reported"))
        self.assert_no_secret(receipt)
        # A third process finds the receipt reported: nothing more.
        getattr(adapter, "_CONTROL_COMMANDS_SEEN", set()).clear()
        third = self._adapter()
        await third._ensure_connector(third._binding(ROOM_A))
        await self.settle(third)
        self.assertEqual(len(self.room.reports), 1)

    async def test_post_restart_reports_an_inventory_mismatch(self):
        def cursor_moved():
            state_store.update(lambda current: setattr(current.binding(ROOM_B), "cursor", 9))

        command, successor = await self._restart_then_new_process(cursor_moved)
        await successor._ensure_connector(successor._binding(ROOM_B))
        await self.settle(successor)
        reports = [report for _room, cid, report in self.room.reports if cid == command["commandId"]]
        self.assertEqual([(report["phase"], report.get("code", "")) for report in reports],
                         [("failed", "inventory_mismatch")])
        self.assertEqual(reports[0]["inventory"], {"before": COUNTS, "after": COUNTS, "match": False})
        self.assertEqual(self.receipt()["phase"], "reported")


if __name__ == "__main__":
    unittest.main()
