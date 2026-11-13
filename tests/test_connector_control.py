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

import ast
import asyncio
import copy
import importlib.util
import logging
import json
import os
import signal
import stat
import subprocess
import sys
import tempfile
import types
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

# The exact Hermes core this plugin runs in (~/.hermes/hermes-agent). Importing
# gateway.run would run hermes_bootstrap (a dependency sync for the data root),
# so the two signal functions are taken verbatim from their files and executed
# alone; gateway/restart.py is loaded from its file.
CORE = Path(importlib.util.find_spec("hermes_cli").origin).resolve().parents[1]


def core_function(relative: str, name: str) -> str:
    source = (CORE / relative).read_text(encoding="utf-8")
    node = next(item for item in ast.parse(source).body if isinstance(item, ast.FunctionDef) and item.name == name)
    return ast.get_source_segment(source, node)


def core_restart_module():
    spec = importlib.util.spec_from_file_location("p17_core_gateway_restart", CORE / "gateway" / "restart.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def core_restart_handler_factory():
    namespace = {"logger": logging.getLogger("p17.core.gateway.run")}
    exec(core_function("gateway/run.py", "_start_gateway_make_restart_signal_handler"), namespace)  # noqa: S102
    return namespace["_start_gateway_make_restart_signal_handler"]


# A simulated supervised gateway: a child process installs its signal handlers
# with the core's own installer (gateway/run_bootstrap.py) and restart handler
# (gateway/run.py); a fake runner records what request_restart was asked.
SUPERVISOR = textwrap.dedent("""
    import asyncio, json, logging, pathlib, sys, threading
    record, ready = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
    namespace = {"logger": logging.getLogger("core"), "threading": threading}
    for source in json.loads(sys.argv[3]):
        exec(source, namespace)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    ignore = len(sys.argv) > 4 and sys.argv[4] == "ignore"

    class Runner:
        def request_restart(self, **kwargs):
            record.write_text(json.dumps(kwargs, sort_keys=True))
            if not ignore:  # "ignore": a core that never exits (a restart already under way)
                loop.stop()

    namespace["_start_gateway_install_signal_handlers"](
        loop, lambda *_args: loop.stop(), namespace["_start_gateway_make_restart_signal_handler"](Runner()))
    ready.write_text("ready")
    loop.call_later(60, loop.stop)
    loop.run_forever()
""")
RESTART_REQUEST = {"detached": False, "via_service": True}
# R2-1: the process-global holder every plugin copy of one gateway shares.
PROCESS_HOLDER = "_synthetic_sociality_room_connector_control"
ROOM_D, ROOM_E = "room-d", "room-e"
INCARNATION = r"^[A-Za-z0-9._:-]{8,128}$"


def load_second_copy(name: str = "p17_second_profile_copy"):
    """A second copy of the plugin, as Hermes imports one per served profile
    under ``gateway.multiplex_profiles`` (the gateway stubs are shared)."""
    if f"{name}.adapter" not in sys.modules:
        root = Path(adapter.__file__).resolve().parent
        spec = importlib.util.spec_from_file_location(name, root / "__init__.py", submodule_search_locations=[str(root)])
        package = importlib.util.module_from_spec(spec)
        sys.modules[name] = package
        package.__path__ = [str(root)]
        for child_name in ("state", "protocol", "cli", "adapter"):
            child_spec = importlib.util.spec_from_file_location(f"{name}.{child_name}", root / f"{child_name}.py")
            child = importlib.util.module_from_spec(child_spec)
            sys.modules[f"{name}.{child_name}"] = child
            child_spec.loader.exec_module(child)
    return sys.modules[f"{name}.adapter"], sys.modules[f"{name}.state"]


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
        self.attempt_answers: dict[str, dict] = {}
        self.claim_errors: dict[str, BaseException] = {}
        self.posted: list[tuple[str, str]] = []
        self.during_report = None
        self.declarations: list[tuple[str, str, dict]] = []
        self.declare_error: BaseException | None = None
        self.granted = ["events.long_poll", "events.sse", CAPABILITY]

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
                return {"sessionId": f"session-{registered_room}", "capabilities": list(room.granted), **ANSWER}

            def declare_installation(self, declared_room, session_id, declaration):
                room.declarations.append((declared_room, session_id, copy.deepcopy(declaration)))
                if room.declare_error is not None:
                    raise room.declare_error
                return {"accepted": True}

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
                return room.attempt_answers.get(cycle_id)

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
        # R2-1: the process host home (HERMES_HOME of the process environment).
        self.host_home = Path(self._directory.name) / "host-home"
        environment = mock.patch.dict(os.environ, {"HERMES_HOME": str(self.host_home)})
        environment.start()
        self.addCleanup(environment.stop)
        self.new_process()
        self.addCleanup(self.new_process)
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

    @staticmethod
    def new_process():
        """A new gateway process: no process-global control state survives."""
        sys.modules.pop(PROCESS_HOLDER, None)
        getattr(adapter, "_CONTROL_COMMANDS_SEEN", set()).clear()

    def installation_record(self):
        return self.host_home / "synthetic-sociality-room" / "installation.json"

    def _adapter(self, module=None, path=None):
        module = module or adapter
        instance = module.SyntheticSocialityAdapter(module.PlatformConfig())
        self.assertEqual(instance._state_file, path or self.path)
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
        # A supervised gateway (systemd/launchd) unless a test says otherwise.
        instance._restart_supervised = lambda: True
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
        watchdog = getattr(instance, "_control_watchdog", None)
        if watchdog is not None:
            tasks.append(watchdog)
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

    def receipt(self, path=None):
        status = (path or self.path).parent / "status.json"
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


    def _supervisor(self, *, ignore=False):
        """A simulated supervisor: a child process that records SIGUSR1."""
        directory = Path(self._directory.name)
        record, ready = directory / "supervisor-record.txt", directory / "supervisor-ready.txt"
        sources = json.dumps([
            core_function("gateway/run.py", "_start_gateway_make_restart_signal_handler"),
            core_function("gateway/run_bootstrap.py", "_start_gateway_install_signal_handlers"),
        ])
        child = subprocess.Popen([sys.executable, "-I", "-c", SUPERVISOR, str(record), str(ready), sources,
                                  "ignore" if ignore else "stop"],
                                 cwd=self._directory.name, env={"PATH": os.environ.get("PATH", "")})

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
        return json.loads(record.read_text()) if record.exists() else None


class ConnectorControlTests(ConnectorControlFixture):
    # P17-01 ---------------------------------------------------------------
    async def test_registration_reports_installation_identity_version_and_control_capability(self):
        instance = self.instance
        for room_id in (ROOM_A, ROOM_B):
            instance._registered_rooms.discard(room_id)
            await instance._ensure_connector(instance._binding(room_id))
        record_path = self.installation_record()
        self.assertTrue(record_path.is_file(), "installation.json is created at the first registration")
        self.assertFalse((self.path.parent / "installation.json").exists(), "never per profile (R2-1)")
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
        # B-F002 (R2-2): a disabled binding's unfinished intent is work in
        # progress too: every stored binding is guarded, and it stays untouched.
        def disable_with_debt(current):
            stopped = current.binding(ROOM_B)
            stopped.enabled = False
            stopped.disabled_reason = "credential_expired"
            stopped.delivery_intents["evt-9"] = intent
        state_store.update(disable_with_debt)
        for action in ("reconnect", "restart"):
            with self.subTest(disabled_debt=action):
                command = _command(action)
                self.deliver(ROOM_A, command)
                await self.settle()
                self.assertEqual(self.room.phases(command["commandId"]), [("refused", "work_in_progress")])
        stopped = state_store.load(self.path).binding(ROOM_B)
        self.assertFalse(stopped.enabled)
        self.assertEqual(stopped.delivery_intents["evt-9"], intent)

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
        self.room.attempt_answers["cycle-b"] = attempt
        result = await instance.send(ROOM_B, "reply in room-b",
                                     reply_to=adapter._dispatch_source_ref(SOURCE, "g-b"), metadata={"notify": True})
        self.assertTrue(result.success, getattr(result, "error", None))
        self.assertEqual(self.room.posted, [(ROOM_B, "reply in room-b")])
        intent = listener.delivery_intents.get(SOURCE, {})
        self.assertNotEqual(intent.get("last_error_code"), "attempt_authority_missing")

    # P17-10 ---------------------------------------------------------------
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
        self.assertEqual(self._wait_for_record(child, record), RESTART_REQUEST)
        self.assertEqual(self.watched, {}, "no listener restarts while the supervisor restarts the gateway")
        self.assertEqual(self.path.read_bytes(), state_before)

    async def test_core_maps_sigusr1_to_the_supervised_drain_aware_gateway_restart(self):
        # Pins the exact Hermes core: SIGUSR1 is its `hermes gateway restart`
        # path, request_restart(detached=False, via_service=True), which exits
        # 75 after the drain so the supervisor relaunches the gateway.
        gateway_restart = core_restart_module()
        _start_gateway_make_restart_signal_handler = core_restart_handler_factory()

        class Runner:
            def __init__(self):
                self.calls = []

            def request_restart(self, **kwargs):
                self.calls.append(kwargs)

        runner = Runner()
        _start_gateway_make_restart_signal_handler(runner)()
        self.assertEqual(runner.calls, [RESTART_REQUEST])
        self.assertEqual(gateway_restart.GATEWAY_SERVICE_RESTART_EXIT_CODE, 75)
        supervised = gateway_restart.is_gateway_supervisor_process
        self.assertTrue(supervised({"XPC_SERVICE_NAME": "ai.hermes.gateway"}))
        self.assertTrue(supervised({"HERMES_LAUNCHD_LABEL": "ai.hermes.gateway"}))
        self.assertTrue(supervised({"INVOCATION_ID": "0123456789abcdef"}))
        self.assertFalse(supervised({"PATH": "/usr/bin", "HOME": "/tmp/home", "XPC_SERVICE_NAME": "0"}))
        # The plugin's pre-check is that same core decision.
        modules = mock.patch.dict(sys.modules, {"gateway.restart": gateway_restart})
        modules.start()
        self.addCleanup(modules.stop)
        with mock.patch.object(gateway_restart, "is_gateway_supervisor_process", return_value=True), \
                mock.patch.object(gateway_restart, "is_container_restart_context", return_value=False):
            self.assertTrue(adapter.gateway_restart_supervised())
        with mock.patch.object(gateway_restart, "is_gateway_supervisor_process", return_value=False), \
                mock.patch.object(gateway_restart, "is_container_restart_context", return_value=True):
            self.assertTrue(adapter.gateway_restart_supervised())
        with mock.patch.object(gateway_restart, "is_gateway_supervisor_process", return_value=False), \
                mock.patch.object(gateway_restart, "is_container_restart_context", return_value=False):
            self.assertFalse(adapter.gateway_restart_supervised())

    async def test_default_requester_signals_only_its_own_gateway_process(self):
        # The default asks the gateway it runs in (SIGUSR1 to os.getpid()). Here
        # os.getpid() names the simulated supervisor, never the test process.
        child, record = self._supervisor()
        with mock.patch("os.getpid", return_value=child.pid), \
                mock.patch("signal.getsignal", return_value=lambda *_args: None):
            adapter.request_gateway_restart()
        self.assertEqual(self._wait_for_record(child, record), RESTART_REQUEST)
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

    async def test_unsupervised_gateway_reports_supervisor_unavailable_without_drain_or_signal(self):
        instance = self.instance
        instance._restart_supervised = adapter.gateway_restart_supervised
        gateway_restart = core_restart_module()
        modules = mock.patch.dict(sys.modules, {"gateway.restart": gateway_restart})
        modules.start()
        self.addCleanup(modules.stop)

        command = _command("restart")
        with mock.patch.object(gateway_restart, "is_gateway_supervisor_process", return_value=False), \
                mock.patch.object(gateway_restart, "is_container_restart_context", return_value=False), \
                mock.patch("os.kill") as kill:
            self.deliver(ROOM_A, command)
            await self.settle()
        kill.assert_not_called()
        self.assertEqual(self.room.phases(command["commandId"]), [("failed", "supervisor_unavailable")])
        self.assertEqual(self.requests, [])
        self.assertEqual(self.room.disconnects, [])
        self.assertIsNone(self.receipt())
        self.assert_listeners_untouched()
        self.assertFalse(instance._stop.is_set())

    async def test_unwritten_receipt_reports_receipt_unavailable_without_drain_or_signal(self):
        """PR80-H-004 (contract P17-6, both connectors): one restarting report, then failed / receipt_unavailable;
        nothing is drained and no supervisor is asked."""
        instance = self.instance
        command = _command("restart")
        with mock.patch.object(adapter.SyntheticSocialityAdapter, "_write_receipts", return_value=False), \
                mock.patch("os.kill") as kill:
            self.deliver(ROOM_A, command)
            await self.settle()
        kill.assert_not_called()
        self.assertEqual(self.room.phases(command["commandId"]),
                         [("restarting", ""), ("failed", "receipt_unavailable")])
        self.assertEqual(self.requests, [], "no supervisor call")
        self.assertEqual(self.room.disconnects, [], "no disconnect")
        self.assert_listeners_untouched()
        self.assertFalse(instance._stop.is_set())

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
        self.new_process()
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
        self.new_process()
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


class ConnectorControlR2Tests(ConnectorControlFixture):
    """OP-12 R2 remedies (CONTRACT-R2.md), each test named after its finding."""

    async def second_profile(self, *, register=True):
        """Profile P2 served by the same gateway process through its own plugin copy."""
        second, second_state = load_second_copy()
        path = Path(self._directory.name) / "profile-two" / "synthetic-sociality-room" / "state.json"
        path.parent.mkdir(parents=True, mode=0o700)
        served = second.RoomBinding("https://room.example/api", ROOM_D, "member-d", "credential-room-d",
                                    installation_id="installation-d", cursor=3, acknowledged_cursor=3,
                                    credential_expires_at="2099-01-01T00:00:00Z", connector_session_id="session-room-d")
        elsewhere = second.RoomBinding("https://other.example:8443/api", ROOM_E, "member-e", "credential-room-e",
                                       installation_id="installation-e", enabled=False,
                                       disabled_reason="credential_expired")
        second_state.save(second.PluginState(bindings=[served, elsewhere]), path)
        with second_state.state_path_scope(path):
            instance = self._adapter(second, path)
        instance._persist_binding = lambda _binding: True
        await self._start_listeners(instance, [ROOM_D])
        if register:
            instance._note_connected(instance._binding(ROOM_D), dict(ANSWER))
        return instance, path

    # B-F001 / R2-1 ---------------------------------------------------------
    async def test_bf001_two_profiles_in_one_process_report_one_installation_id(self):
        other, other_path = await self.second_profile()
        self.instance._registered_rooms.discard(ROOM_A)
        await self.instance._ensure_connector(self.instance._binding(ROOM_A))
        other._registered_rooms.discard(ROOM_D)
        await other._ensure_connector(other._binding(ROOM_D))
        ids = {metadata.get("installationId") for _room, metadata in self.room.registrations}
        record = json.loads(self.installation_record().read_text())
        self.assertEqual(ids, {record["installationId"]})
        for profile in (self.path, other_path):
            self.assertFalse((profile.parent / "installation.json").exists())
        self.assertIn(PROCESS_HOLDER, sys.modules, "process-global holder, shared by every plugin copy")

    async def test_bf001_restart_from_one_profile_refuses_while_another_profile_dispatches(self):
        other, _other_path = await self.second_profile()
        other._active_dispatch_rooms[ROOM_D] = "dispatch-generation"
        for action in ("restart", "reconnect"):
            with self.subTest(action=action):
                command = _command(action)
                self.deliver(ROOM_A, command)
                await self.settle()
                self.assertEqual(self.room.phases(command["commandId"]), [("refused", "work_in_progress")])
        self.assertEqual(self.requests, [])
        self.assertEqual(self.room.disconnects, [])
        self.assertIn(ROOM_D, other._tasks)
        self.assert_listeners_untouched()

    async def test_bf001_inventory_and_receipt_cover_every_profile_of_the_process(self):
        other, other_path = await self.second_profile()
        seen = []
        self.instance._restart_requester = lambda: seen.append((self.receipt(), self.receipt(other_path)))
        command = _command("restart")
        self.deliver(ROOM_A, command)
        await self.settle()
        self.assertEqual(self.room.phases(command["commandId"]), [("restarting", "")])
        self.assertEqual(len(seen), 1)
        mine, theirs = seen[0]
        for receipt in (mine, theirs):
            self.assertIsNotNone(receipt, "the receipt is in every profile's status.json before the request")
            self.assertEqual(receipt["commandId"], command["commandId"])
            self.assertEqual(receipt["before"]["counts"], {"bindings": 5, "enabled": 3, "disabled": 2})
            self.assertEqual(sorted(item["membershipId"] for item in receipt["before"]["bindings"]),
                             ["member-a", "member-b", "member-c", "member-d", "member-e"])
            self.assert_no_secret(receipt)
        self.assertNotIn("credential-room-d", json.dumps(theirs))
        # The whole gateway drains: every profile's listeners stop.
        self.assertEqual(sorted(self.room.disconnects), [ROOM_A, ROOM_B, ROOM_D])
        self.assertEqual((self.instance._tasks, other._tasks), ({}, {}))

    async def test_bf001_unreadable_profile_state_refuses_with_connector_scope_unknown(self):
        other, other_path = await self.second_profile()
        other_path.write_text("{not json")
        for action in ("reconnect", "restart"):
            with self.subTest(action=action):
                command = _command(action)
                self.deliver(ROOM_A, command)
                await self.settle()
                self.assertEqual(self.room.phases(command["commandId"]), [("refused", "connector_scope_unknown")])
        self.assertEqual(self.requests, [])
        self.assert_listeners_untouched()

    # B-F002 / R2-2 ---------------------------------------------------------
    async def test_bf002_disabled_binding_with_unfinished_intent_refuses_reconnect_and_restart(self):
        intent = {"state": "prepared", "delivery_state": "not_started", "lifecycle_state": "not_started"}
        state_store.update(lambda current: current.binding(ROOM_C).delivery_intents.update({"evt-12": intent}))
        before = self.path.read_bytes()
        for action in ("reconnect", "restart"):
            with self.subTest(action=action):
                command = _command(action)
                self.deliver(ROOM_A, command)
                await self.settle()
                self.assertEqual(self.room.phases(command["commandId"]), [("refused", "work_in_progress")])
        self.assertEqual(self.requests, [])
        self.assertEqual(self.path.read_bytes(), before)
        self.assert_listeners_untouched()

    # B-F003 / R2-3 ---------------------------------------------------------
    async def test_bf003_registration_declares_the_installation_with_every_profiles_memberships(self):
        other, _other_path = await self.second_profile()
        binding = self.instance._binding(ROOM_A)
        self.instance._note_connected(binding, {**ANSWER, "sessionId": f"session-{ROOM_A}"}, registered=True)
        await self.settle()
        self.assertEqual(self.room.declarations, [], "no declaration unless the Room granted control")
        granted = {**ANSWER, "sessionId": f"session-{ROOM_A}", "capabilities": [CAPABILITY]}
        self.instance._note_connected(binding, granted, registered=True)
        await self.settle()
        self.assertEqual(len(self.room.declarations), 1)
        room_id, session_id, declaration = self.room.declarations[0]
        self.assertEqual((room_id, session_id), (ROOM_A, f"session-{ROOM_A}"))
        self.assertEqual(sorted(declaration), ["incarnation", "installationId", "memberships", "origins"])
        self.assertEqual(declaration["installationId"],
                         json.loads(self.installation_record().read_text())["installationId"])
        self.assertRegex(declaration["incarnation"], INCARNATION)
        # Every stored binding of the process at this binding's origin, both profiles, disabled included.
        self.assertEqual(sorted(declaration["memberships"]), ["member-a", "member-b", "member-c", "member-d"])
        self.assertEqual(sorted(declaration["origins"], key=lambda item: item["origin"]), [
            {"origin": "https://other.example:8443", "bindings": 1, "enabled": 0},
            {"origin": "https://room.example", "bindings": 4, "enabled": 3},
        ])
        self.assert_no_secret(declaration)
        # Every registration declares again; a 404/409 answer never stops the loop or a claim.
        self.room.declare_error = adapter.ProtocolError("not found", status=404, code="not_found")
        command = _command("reconnect")
        self.room.claim_errors[command["commandId"]] = _conflict("control_claimed")
        self.instance._note_connected(binding, {**granted, "connectorControl": command}, registered=True)
        await self.settle()
        self.assertEqual(len(self.room.declarations), 2)
        self.assertEqual(self.room.claims, [(ROOM_A, f"session-{ROOM_A}", command["commandId"])])
        self.assertIn(ROOM_D, other._tasks)

    async def test_bf003_protocol_declaration_route(self):
        with mock.patch.object(room_protocol.RoomProtocol, "request", autospec=True,
                               return_value={"accepted": True}) as request:
            room_protocol.RoomProtocol("https://room.example/api", "c").declare_installation(
                "room a", "session/1", {"installationId": "hermes-inst-x"})
        self.assertEqual(request.call_args.args[1:4], (
            "PUT", "/rooms/room%20a/connector/sessions/session%2F1/installation", {"installationId": "hermes-inst-x"}))

    # B-F005 / R2-7 ---------------------------------------------------------
    async def test_bf005_only_a_new_incarnation_completes_a_restart(self):
        self.instance._restart_requester = lambda: self.requests.append("restart")
        command = _command("restart")
        self.deliver(ROOM_A, command)
        await self.settle()
        incarnation = sys.modules[PROCESS_HOLDER].incarnation
        self.assertRegex(incarnation, INCARNATION)
        self.assertEqual(self.receipt()["incarnation"], incarnation, "the receipt stores the claim incarnation")
        self.room.reports.clear()
        # A re-created adapter of the same gateway process (a reconnect
        # watcher, a re-attached profile) never completes and never resumes.
        same = self._adapter()
        await same._ensure_connector(same._binding(ROOM_A))
        await self.settle(same)
        self.assertEqual(self.room.phases(command["commandId"]), [])
        self.assertEqual(self.receipt()["phase"], "restarting")
        # The new gateway process completes it, once.
        self.new_process()
        successor = self._adapter()
        self.assertNotEqual(sys.modules[PROCESS_HOLDER].incarnation, incarnation)
        await successor._ensure_connector(successor._binding(ROOM_A))
        await self.settle(successor)
        self.assertEqual(self.room.phases(command["commandId"]), [("completed", "")])
        self.assertEqual(self.receipt()["phase"], "reported")

    # B-F008 / R2-8 ---------------------------------------------------------
    async def test_bf008_an_accepted_signal_the_core_ignores_stays_fenced_and_marks_restart_overdue(self):
        instance = self.instance
        child, record = self._supervisor(ignore=True)
        instance._restart_requester = lambda: adapter.signal_supervisor_restart(child.pid)
        command = _command("restart")
        with mock.patch.object(adapter, "CONTROL_RESTART_OVERDUE_SECONDS", 0.3, create=True):
            self.deliver(ROOM_A, command)
            await self.settle()
            deadline = time.monotonic() + 5
            while (self.receipt() or {}).get("phase") != "restart_overdue" and time.monotonic() < deadline:
                await asyncio.sleep(0.05)
        self.assertEqual((self.receipt() or {}).get("phase"), "restart_overdue")
        deadline = time.monotonic() + 10
        while not record.exists() and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        self.assertEqual(json.loads(record.read_text()), RESTART_REQUEST)
        self.assertIsNone(child.poll(), "the simulated core ignored the signal and still runs")
        self.assertEqual(self.room.phases(command["commandId"]), [("restarting", "")], "nothing more is reported")
        self.assertEqual(self.watched, {}, "no listener is restored")
        self.assertEqual(instance._tasks, {})
        self.assertTrue(instance._control_fenced(), "the fence stays held")

    async def test_bf008_restart_window_follows_the_core_after_turn_timeout(self):
        # R2-8b: core restart_after_turn_timeout (default 1800 s) + 300 s, read
        # read-only from the host home's config; the overdue mark uses it too.
        config = self.host_home / "config.yaml"
        config.parent.mkdir(parents=True, exist_ok=True)
        cases = (("configured", "agent:\n  restart_after_turn_timeout: 600\n", 900),
                 ("unreadable", "agent: [unclosed\n  : :\n", 2100),
                 ("absent", None, 2100))
        for label, text, expected in cases:
            with self.subTest(config=label):
                self.new_process()
                if text is None:
                    config.unlink(missing_ok=True)
                else:
                    config.write_text(text)
                self.room.reports.clear()
                instance = self._adapter()
                instance._restart_requester = lambda: None
                slept: list[float] = []

                async def overdue_sleep(seconds, slept=slept):
                    slept.append(seconds)

                instance._restart_overdue_sleep = overdue_sleep
                command = _command("restart")
                environment = {key: value for key, value in os.environ.items()
                               if key != "HERMES_RESTART_AFTER_TURN_TIMEOUT"}
                with mock.patch.dict(os.environ, environment, clear=True), \
                        mock.patch.object(adapter, "CONTROL_RESTART_OVERDUE_SECONDS", None, create=True):
                    self.deliver(ROOM_A, command, instance=instance)
                    await self.settle(instance)
                    deadline = time.monotonic() + 5
                    while (self.receipt() or {}).get("phase") != "restart_overdue" and time.monotonic() < deadline:
                        await asyncio.sleep(0.02)
                reports = [report for _room, cid, report in self.room.reports if cid == command["commandId"]]
                self.assertEqual([report["phase"] for report in reports], ["restarting"])
                self.assertEqual(reports[0].get("restartWithinSeconds"), expected)
                self.assertEqual(slept, [expected])
                self.assertEqual(self.receipt()["phase"], "restart_overdue")


if __name__ == "__main__":
    unittest.main()


class ConnectorControlR3Tests(ConnectorControlFixture):
    """OP-12 R2 residual of B-F001: a served profile whose adapter exists in the
    gateway process but is disconnected or never joined is restarted with the
    process, so it belongs to the guard, the inventory, the receipts and the
    declaration. Committed equivalents of review-r2/probe-hermes/tests/
    test_zz_op12_r2_probe.py (reviewer evidence, not edited)."""

    second_profile = ConnectorControlR2Tests.second_profile
    DEBT = {"state": "prepared", "delivery_state": "not_started", "lifecycle_state": "not_started"}

    async def _p2_with_debt(self, *, register: bool, leave: bool, debt: bool = True):
        other, other_path = await self.second_profile(register=register)
        if debt:
            data = json.loads(other_path.read_text())
            for binding in data["bindings"]:
                if binding.get("room_id") == ROOM_E:
                    binding.setdefault("delivery_intents", {})["evt-probe"] = dict(self.DEBT)
            other_path.write_text(json.dumps(data))
        if leave:
            other._control_leave()
        return other, other_path

    async def _assert_refused(self, actions=("restart", "reconnect")):
        for action in actions:
            with self.subTest(action=action):
                command = _command(action)
                self.deliver(ROOM_A, command)
                await self.settle()
                self.assertEqual(self.room.phases(command["commandId"]), [("refused", "work_in_progress")])
        self.assertEqual(self.requests, [])
        self.assert_listeners_untouched()

    # Reviewer probe cases (a), (b), (c), restart and reconnect -------------
    async def test_bf001_r2_probe_a_joined_profile_with_disabled_debt_refuses(self):
        await self._p2_with_debt(register=True, leave=False)
        await self._assert_refused()

    async def test_bf001_r2_probe_b_disconnected_profile_with_disabled_debt_refuses(self):
        await self._p2_with_debt(register=True, leave=True)
        await self._assert_refused()

    async def test_bf001_r2_probe_c_never_joined_profile_with_disabled_debt_refuses(self):
        await self._p2_with_debt(register=False, leave=False)
        await self._assert_refused()

    async def test_bf001_profile_disconnected_by_the_core_with_disabled_debt_refuses_both_actions(self):
        other, other_path = await self._p2_with_debt(register=True, leave=False)
        await other.disconnect()  # the real path: the adapter leaves the live registry
        before = other_path.read_bytes()
        await self._assert_refused()
        self.assertEqual(other_path.read_bytes(), before, "control never writes the member's state")
        self.assertEqual(other._tasks, {}, "a disconnected profile is never started by control")

    # Inventory, receipts and declaration include non-joined profiles -------
    async def test_bf001_restart_inventory_and_receipts_include_a_disconnected_profile(self):
        other, other_path = await self._p2_with_debt(register=True, leave=False, debt=False)
        await other.disconnect()
        drained_before = list(self.room.disconnects)
        seen = []
        self.instance._restart_requester = lambda: seen.append((self.receipt(), self.receipt(other_path)))
        command = _command("restart")
        self.deliver(ROOM_A, command)
        await self.settle()
        self.assertEqual(self.room.phases(command["commandId"]), [("restarting", "")])
        self.assertEqual(len(seen), 1)
        for receipt in seen[0]:
            self.assertIsNotNone(receipt, "every member profile's status.json holds the receipt before the request")
            self.assertEqual(receipt["before"]["counts"], {"bindings": 5, "enabled": 3, "disabled": 2})
            self.assertEqual(sorted(item["membershipId"] for item in receipt["before"]["bindings"]),
                             ["member-a", "member-b", "member-c", "member-d", "member-e"])
            self.assertIn(str(other_path), receipt["stateFiles"])
            self.assert_no_secret(receipt)
        # Only joined adapters are drained; the disconnected profile is not touched again.
        self.assertEqual(sorted(self.room.disconnects[len(drained_before):]), [ROOM_A, ROOM_B])

    async def test_bf001_never_joined_profile_is_in_inventory_receipt_and_declaration(self):
        other, other_path = await self._p2_with_debt(register=False, leave=False, debt=False)
        binding = self.instance._binding(ROOM_A)
        granted = {**ANSWER, "sessionId": f"session-{ROOM_A}", "capabilities": [CAPABILITY]}
        self.instance._note_connected(binding, granted, registered=True)
        await self.settle()
        _room, _session, declaration = self.room.declarations[-1]
        self.assertEqual(sorted(declaration["memberships"]), ["member-a", "member-b", "member-c", "member-d"])
        self.assertEqual(sorted(declaration["origins"], key=lambda item: item["origin"]), [
            {"origin": "https://other.example:8443", "bindings": 1, "enabled": 0},
            {"origin": "https://room.example", "bindings": 4, "enabled": 3},
        ])
        seen = []
        self.instance._restart_requester = lambda: seen.append(self.receipt(other_path))
        command = _command("restart")
        self.deliver(ROOM_A, command)
        await self.settle()
        self.assertEqual(self.room.phases(command["commandId"]), [("restarting", "")])
        self.assertEqual(seen[0]["before"]["counts"], {"bindings": 5, "enabled": 3, "disabled": 2})

    async def test_bf001_reconnect_keeps_joined_listeners_and_never_starts_a_disconnected_profile(self):
        other, other_path = await self._p2_with_debt(register=True, leave=False, debt=False)
        await other.disconnect()
        command = _command("reconnect")
        self.deliver(ROOM_A, command)
        await self.settle()
        reports = [report for _room, cid, report in self.room.reports if cid == command["commandId"]]
        self.assertEqual([report["phase"] for report in reports], ["reconnecting", "completed"])
        member_counts = {"bindings": 5, "enabled": 3, "disabled": 2}
        self.assertEqual(reports[-1]["inventory"], {"before": member_counts, "after": member_counts, "match": True})
        self.assertEqual(sorted(self.watched), [ROOM_A, ROOM_B], "joined listeners started again, once each")
        self.assertEqual(other._tasks, {}, "the disconnected profile stays as it was")
        self.assertNotIn(ROOM_D, self.watched)
        self.assertEqual(self.receipt(other_path)["phase"], "reported")

    async def test_bf001_unreadable_never_joined_profile_state_refuses_with_connector_scope_unknown(self):
        _other, other_path = await self._p2_with_debt(register=False, leave=False, debt=False)
        other_path.write_text("{not json")
        for action in ("reconnect", "restart"):
            with self.subTest(action=action):
                command = _command(action)
                self.deliver(ROOM_A, command)
                await self.settle()
                self.assertEqual(self.room.phases(command["commandId"]), [("refused", "connector_scope_unknown")])
        self.assertEqual(self.requests, [])
        self.assert_listeners_untouched()


class RestartWindowParserTests(unittest.TestCase):
    """OP-12 105 PM addendum H105-F006: the restart window reads config.yaml with core's shared hermes_yaml
    parser (PM generations ship no PyYAML), falls back to yaml.safe_load on an older core, and otherwise keeps
    the 1800 s default; env precedence, the file checks and the value checks are unchanged."""

    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.home = Path(directory.name) / "host-home"
        self.home.mkdir()
        environ = mock.patch.dict(os.environ, {"HERMES_HOME": str(self.home)})
        environ.start()
        self.addCleanup(environ.stop)
        os.environ.pop("HERMES_RESTART_AFTER_TURN_TIMEOUT", None)

    def window(self, text, *, modules=None):
        if text is not None:
            (self.home / "config.yaml").write_text(text)
        with mock.patch.dict(sys.modules, modules or {}):
            return adapter.restart_within_seconds()

    def test_h105_f006_a_configured_value_is_read_without_pyyaml(self):
        self.assertEqual(self.window("agent:\n  restart_after_turn_timeout: 600\n", modules={"yaml": None}), 900)

    def test_h105_f006_an_older_core_without_hermes_yaml_falls_back_to_yaml_safe_load(self):
        calls = []
        legacy = types.ModuleType("yaml")
        legacy.safe_load = lambda text: (calls.append(text), {"agent": {"restart_after_turn_timeout": 600}})[1]
        self.assertEqual(self.window("agent:\n  restart_after_turn_timeout: 600\n",
                                     modules={"hermes_yaml": None, "yaml": legacy}), 900)
        self.assertEqual(len(calls), 1, "the legacy parser is used only when hermes_yaml is unavailable")

    def test_h105_f006_with_neither_parser_the_default_applies(self):
        self.assertEqual(self.window("agent:\n  restart_after_turn_timeout: 600\n",
                                     modules={"hermes_yaml": None, "yaml": None}), 2100)

    def test_h105_f006_the_environment_value_wins(self):
        with mock.patch.dict(os.environ, {"HERMES_RESTART_AFTER_TURN_TIMEOUT": "120"}):
            self.assertEqual(self.window("agent:\n  restart_after_turn_timeout: 600\n", modules={"yaml": None}), 420)

    def test_h105_f006_invalid_configs_and_values_fail_closed_to_the_default(self):
        cases = {
            "unreadable": "agent: [unclosed\n  : :\n",
            "duplicate key": "agent:\n  restart_after_turn_timeout: 600\n  restart_after_turn_timeout: 900\n",
            "bool": "agent:\n  restart_after_turn_timeout: true\n",
            "negative": "agent:\n  restart_after_turn_timeout: -5\n",
            "not a number": "agent:\n  restart_after_turn_timeout: soon\n",
            "list": "agent:\n  restart_after_turn_timeout: [600]\n",
            "agent not a mapping": "agent: 600\n",
            "top level not a mapping": "- 600\n",
        }
        for label, text in cases.items():
            with self.subTest(config=label):
                self.assertEqual(self.window(text, modules={"yaml": None}), 2100)

    def test_h105_f006_a_symlinked_config_is_not_read(self):
        real = self.home.parent / "elsewhere.yaml"
        real.write_text("agent:\n  restart_after_turn_timeout: 600\n")
        (self.home / "config.yaml").symlink_to(real)
        self.assertEqual(self.window(None, modules={"yaml": None}), 2100)
