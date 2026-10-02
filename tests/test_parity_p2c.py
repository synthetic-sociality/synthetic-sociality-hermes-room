"""Shared conformance cases for parity release P2c.

Delivery contract section "P2c — Secrets, late replies, self-recovery"; case
names are the keys of ``conformance.json`` ``parityP2cCases``; OpenClaw
implements the same names. No test uses or prints a real credential.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import stat
import tempfile
import threading
import time
import types
import unittest
from pathlib import Path
from unittest import mock

from test_delivery_lifecycle import ROOT, adapter
from test_parity_p1 import cycle_attempt, refusal, room_instance, source_ref
from test_parity_p1a import DENY_PATTERN, HUMAN, PEER, START, attempt_ready, build_message, cycle

CONFORMANCE = json.loads((ROOT / "conformance.json").read_text())
CONTRACT = (ROOT / "docs" / "delivery-lifecycle-contract-v1.md").read_text()
secret_isolation = adapter.secret_isolation
local_status = adapter.local_status
state_module = __import__(adapter.__name__.rsplit(".", 1)[0] + ".state", fromlist=["state"])

# Test-only values; never a real credential.
FAKE_CREDENTIAL = "room-cred-TEST-" + "0f1e2d3c" * 4
FAKE_REPLACEMENT = "room-cred-TEST-replacement-" + "9a8b" * 6
HUMAN_AT = {**HUMAN, "ts": "2026-10-02T09:49:12Z"}
KEPT_TEXT = "The dosage question needs the lab values first; I would wait for them."


def appendix_keys() -> set[str]:
    section = CONTRACT[CONTRACT.index("## Appendix — Local status file (P2c-4)"):]
    return set(re.findall(r"^\| `([A-Za-z.<>]+)` \|", section, re.MULTILINE))


def flatten_keys(value: dict, prefix: str = "") -> set[str]:
    keys = set()
    for key, item in value.items():
        name = f"{prefix}{key}"
        keys.add(name)
        if key == "rooms":
            for entry in item.values():
                keys |= {f"rooms.<id>.{inner}" for inner in entry}
    return keys


def hermes_plugins():
    from hermes_cli import plugins
    return plugins, plugins._delivery_manager()


REAL_DISPATCH = r"""
import json, sys, types
sys.path.insert(0, sys.argv[1])
from support import load_module
adapter = load_module("adapter")
from hermes_cli import plugins
import model_tools
state_file, notes, credential = sys.argv[2], sys.argv[3], sys.argv[4]
adapter.secret_isolation.remember_secrets([types.SimpleNamespace(credential=credential, credential_rotation={})])
manager = plugins._delivery_manager()
manager._hooks.setdefault("pre_tool_call", []).append(adapter._on_pre_tool_call)
manager._hooks.setdefault("transform_tool_result", []).append(adapter._on_transform_tool_result)
out = {"blocked": model_tools.handle_function_call("read_file", {"path": state_file}, task_id="p2c-01"),
       "allowed": model_tools.handle_function_call("read_file", {"path": notes}, task_id="p2c-01")}
open(notes, "w").write("pasted: " + credential)
out["scrubbed"] = model_tools.handle_function_call("read_file", {"path": notes}, task_id="p2c-01")
print("RESULT " + json.dumps(out))
"""


def real_dispatch(home, state_file, notes):
    """Hermes' own read_file tool through its real dispatch, in a clean interpreter."""
    import subprocess
    import sys
    environ = {**os.environ, "HERMES_HOME": str(home)}
    completed = subprocess.run(
        [sys.executable, "-c", REAL_DISPATCH, str(ROOT / "tests"), str(state_file), str(notes), FAKE_CREDENTIAL],
        capture_output=True, text=True, timeout=240, env=environ, cwd=str(ROOT / "tests"),
    )
    line = next((item for item in completed.stdout.splitlines() if item.startswith("RESULT ")), "")
    if not line:
        raise AssertionError(f"real dispatch failed: rc={completed.returncode} {completed.stderr[-800:]!r}")
    return json.loads(line[len("RESULT "):])


class P2cConformanceTests(unittest.TestCase):
    def test_case_names_resolve_to_tests(self):
        cases = CONFORMANCE["parityP2cCases"]
        self.assertEqual(len(cases), 5)
        sources = "\n".join(path.read_text() for path in (ROOT / "tests").glob("test_*.py"))
        for name, method in cases.items():
            self.assertTrue(name.startswith("P2c-0"), name)
            self.assertEqual(sources.count(f"def {method}(self"), 1, method)

    def test_status_schema_is_the_contract_appendix(self):
        self.assertEqual(CONFORMANCE["parityP2cStatusSchema"], local_status.SCHEMA)
        self.assertIn(f"`{local_status.SCHEMA}`", CONTRACT)


class P2cTests(unittest.TestCase):
    def setUp(self):
        secret_isolation.forget_secrets()
        adapter._SECRET_REFUSAL_LOGGED.clear()

    def tearDown(self):
        secret_isolation.forget_secrets()
        adapter._SECRET_REFUSAL_LOGGED.clear()

    def test_p2c_01_the_credential_is_not_readable_through_the_agent_s_tools_and_never_appears_in_prompt_tool_result_or_log(self):
        with tempfile.TemporaryDirectory() as scratch:
            home = Path(scratch) / ".hermes"
            store = home / "synthetic-sociality-room"
            store.mkdir(parents=True)
            state_file = store / "state.json"
            state_file.write_text(json.dumps({"credential": FAKE_CREDENTIAL}))
            alias = Path(scratch) / "innocent.json"
            alias.symlink_to(state_file)
            notes = Path(scratch) / "notes.txt"
            notes.write_text("an ordinary file")
            with mock.patch.dict(os.environ, {"HERMES_HOME": str(home)}):
                # 1. The plugin's own fence, in every session (no Room context).
                refused = {
                    "read_file": {"path": str(state_file)},
                    "read_file (symlink)": {"path": str(alias)},
                    "read_file (relative)": {"path": "../.hermes/synthetic-sociality-room/state.json"},
                    "write_file": {"path": str(store / "status.json"), "content": "{}"},
                    "search_files": {"path": scratch, "pattern": "credential"},
                    "patch": {"mode": "patch", "patch": f"*** Begin Patch\n*** Update File: {state_file}\n@@\n-a\n+b\n*** End Patch"},
                    "terminal": {"command": "cd ~/.hermes && cat synthetic-sociality-room/state.json"},
                    "process": {"action": "spawn", "command": f"tail -f {store}/state.json"},
                    "execute_code": {"code": "print(open('" + str(state_file) + "').read())"},
                }
                with mock.patch.dict(os.environ, {"TERMINAL_CWD": str(Path(scratch) / "work")}), \
                        self.assertLogs(adapter.logger, level="WARNING"):
                    for label, args in refused.items():
                        with self.subTest(tool=label):
                            verdict = adapter._on_pre_tool_call(tool_name=label.split(" ")[0], args=args)
                            self.assertEqual(verdict["action"], "block", label)
                            self.assertNotIn(FAKE_CREDENTIAL, verdict["message"])
                self.assertIsNone(adapter._on_pre_tool_call(tool_name="read_file", args={"path": str(notes)}))
                self.assertIsNone(adapter._on_pre_tool_call(tool_name="search_files", args={"path": str(store.parent / "skills")}))
                # A fence error refuses (older cores let a raising hook through).
                with mock.patch.object(secret_isolation, "tool_call_block", side_effect=RuntimeError("boom")), \
                        self.assertLogs(adapter.logger, level="WARNING"):
                    self.assertEqual(adapter._on_pre_tool_call(tool_name="read_file", args={"path": str(notes)})["action"], "block")

                # 2. The agent's own read_file tool, through Hermes' real
                # dispatch with the plugin's hooks (a clean interpreter: this
                # suite stubs parts of the gateway): refused, credential
                # absent; an ordinary file still reads; a credential that
                # reached another file is scrubbed from the result.
                dispatched = real_dispatch(home, state_file, notes)
                self.assertNotIn(FAKE_CREDENTIAL, dispatched["blocked"])
                self.assertIn("not available to agent tools", dispatched["blocked"])
                self.assertIn("an ordinary file", dispatched["allowed"])
                self.assertNotIn(FAKE_CREDENTIAL, dispatched["scrubbed"])
                self.assertIn(secret_isolation.REDACTED, dispatched["scrubbed"])

                # 3. The start gate: a host shell backend, or a sandbox mount
                # over the store, refuses to start, logged once.
                cases = [
                    ({}, ["host_shell_backend"]),
                    ({"TERMINAL_ENV": "local"}, ["host_shell_backend"]),
                    ({"TERMINAL_ENV": "ssh"}, ["host_shell_backend"]),
                    ({"TERMINAL_ENV": "docker", "TERMINAL_DOCKER_VOLUMES": json.dumps([f"{home}:/root/.hermes"])},
                     ["sandbox_volume_covers_store"]),
                    ({"TERMINAL_ENV": "docker", "TERMINAL_DOCKER_VOLUMES": json.dumps([f"{store}/state.json:/s:ro"])},
                     ["sandbox_volume_covers_store"]),
                    ({"TERMINAL_ENV": "docker", "TERMINAL_DOCKER_MOUNT_CWD_TO_WORKSPACE": "true", "TERMINAL_CWD": scratch},
                     ["sandbox_cwd_covers_store"]),
                    ({"TERMINAL_ENV": "docker", "TERMINAL_DOCKER_VOLUMES": "not json"}, ["sandbox_volumes_unreadable"]),
                ]
                for environ, expected in cases:
                    with self.subTest(environ=environ):
                        self.assertEqual(secret_isolation.store_reachability(store, environ, mount_sources=[]), expected)
                self.assertEqual(secret_isolation.store_reachability(store, {"TERMINAL_ENV": "docker"}, mount_sources=[str(home)]),
                                 ["sandbox_mount_covers_store"])
                isolated = {"TERMINAL_ENV": "docker", "TERMINAL_DOCKER_VOLUMES": json.dumps(["/srv/data:/data"]),
                            "TERMINAL_DOCKER_MOUNT_CWD_TO_WORKSPACE": "true", "TERMINAL_CWD": str(Path(scratch) / "work")}
                self.assertEqual(secret_isolation.store_reachability(store, isolated, mount_sources=[str(home / "skills")]), [])

                binding = adapter.RoomBinding("https://room.example/api", "room-1", "member-1", FAKE_CREDENTIAL,
                                              installation_id="inst-1", enabled=False, disabled_reason="credential_expired",
                                              credential_rotation={"replacement_credential": FAKE_REPLACEMENT})
                instance = object.__new__(adapter.SyntheticSocialityAdapter)
                instance._state_file = state_file
                instance._state = adapter.PluginState(bindings=[binding])
                instance._status = local_status.LocalStatus(store / "status.json", plugin="hermes",
                                                            version=adapter.CONNECTOR_VERSION, host_label="host-a")
                instance._stop = asyncio.Event()
                instance._tasks, instance._renewal_task = {}, None
                with mock.patch.dict(os.environ, {"TERMINAL_ENV": "local"}), self.assertLogs(adapter.logger, level="ERROR") as logs:
                    self.assertFalse(asyncio.run(instance.connect()))
                    self.assertFalse(asyncio.run(instance.connect()))
                refusals = [line for line in logs.output if "refused to start" in line]
                self.assertEqual(len(refusals), 1, logs.output)
                self.assertIn("secret_store_reachable (host_shell_backend)", refusals[0])
                self.assertEqual(instance._tasks, {})
                written = local_status.read(store / "status.json")
                self.assertEqual(written["secretStore"], "reachable")
                self.assertEqual(written["rooms"]["room-1"]["lastErrorCode"], "secret_store_reachable")
                with mock.patch.object(secret_isolation, "_sandbox_mount_sources", lambda: []), \
                        mock.patch.dict(os.environ, isolated):
                    asyncio.run(instance.connect())
                self.assertEqual(local_status.read(store / "status.json")["secretStore"], "isolated")
                self.assertNotIn(FAKE_CREDENTIAL, (store / "status.json").read_text())

                # 4. Never in a tool result, a log line, or the model prompt.
                self.assertIsNone(adapter._on_transform_tool_result(tool_name="terminal", result="clean output"))
                for secret in (FAKE_CREDENTIAL, FAKE_REPLACEMENT):
                    result = adapter._on_transform_tool_result(tool_name="terminal", result=f"export X={secret}\n")
                    self.assertNotIn(secret, result)
                    with self.assertLogs(adapter.logger, level="WARNING") as logs:
                        adapter.logger.warning("Room diagnostic %s", secret)
                    self.assertNotIn(secret, "\n".join(logs.output))

                def prepare(_instance, prepared):
                    prepared.credential = FAKE_CREDENTIAL
                    prepared.credential_rotation = {"replacement_credential": FAKE_REPLACEMENT}

                prompt = build_message(attempt_ready(50, 1, source="m-42"), attempt=cycle(), prepare=prepare)
                self.assertNotIn(FAKE_CREDENTIAL, prompt.text)
                self.assertNotIn(FAKE_REPLACEMENT, prompt.text)
                self.assertNotIn(FAKE_CREDENTIAL, json.dumps(prompt.raw_message, default=str))

        # 5. No plugin module places anything in the environment tools inherit.
        for module in ROOT.glob("*.py"):
            source = module.read_text()
            self.assertNotRegex(source, r"os\.environ\[[^\]]+\]\s*=|os\.putenv|os\.environ\.(update|setdefault)\(", module.name)

    def test_p2c_02_a_late_reply_is_offered_once_as_a_fact_with_its_kept_text_and_then_cleared(self):
        # Kept: the model ran, the Room refused the post because the turn had ended.
        for code in ("turn_expired", "cycle_superseded", "turn_not_active"):
            with self.subTest(code=code):
                async def run(code=code):
                    instance, binding, api, _published, _dispatched = room_instance(cycle_attempt())
                    api.post_error = lambda _n: refusal(code)
                    result = await instance._send_final("room-1", source_ref(), KEPT_TEXT)
                    return binding, result

                binding, result = asyncio.run(run())
                self.assertTrue(result.success)
                kept = binding.kept_reply
                self.assertEqual((kept["source_event_id"], kept["source_seq"], kept["epoch_id"], kept["reason"], kept["text"]),
                                 ("evt-5", 5, "epoch-1", code, KEPT_TEXT))
                self.assertTrue(state_module._valid_canonical_timestamp(kept["produced_at"]))

        # Kept when the lease was lost while the model ran (re-claim refused).
        async def lease_lost():
            instance, binding, api, _published, _dispatched = room_instance(cycle_attempt())
            api.claim = lambda _n: None
            await instance._send_final("room-1", source_ref(), KEPT_TEXT)
            return binding, api

        binding, api = asyncio.run(lease_lost())
        self.assertEqual(api.posts, [])
        self.assertEqual(binding.kept_reply["text"], KEPT_TEXT)

        # Not kept: the discussion changed (never crosses discussions), or no text.
        async def stale():
            instance, binding, api, _published, _dispatched = room_instance(cycle_attempt())
            api.post_error = lambda _n: refusal("stale_epoch")
            await instance._send_final("room-1", source_ref(), KEPT_TEXT)
            return binding

        self.assertEqual(asyncio.run(stale()).kept_reply, {})

        # Bounded: one per Room, the latest wins.
        instance, binding, _api, _published, _dispatched = room_instance(cycle_attempt())
        instance._keep_late_reply(binding, "evt-5", "m-1", "epoch-1", "first", "turn_expired")
        instance._keep_late_reply(binding, "evt-5", "m-2", "epoch-1", "second", "cycle_superseded")
        self.assertEqual((binding.kept_reply["source_event_id"], binding.kept_reply["text"]), ("m-2", "second"))
        instance._keep_late_reply(binding, "evt-5", "m-3", "epoch-1", "x" * (state_module.KEPT_REPLY_CHARACTER_LIMIT + 1), "turn_expired")
        self.assertEqual(binding.kept_reply["source_event_id"], "m-2")

        # Durable: saved, reloaded and validated with the state.
        with tempfile.TemporaryDirectory() as scratch:
            path = Path(scratch) / "profile" / "state.json"
            stored = adapter.RoomBinding("https://room.example/api", "room-1", "member-1", "credential",
                                         kept_reply=dict(binding.kept_reply))
            state_module.save(adapter.PluginState(bindings=[stored]), path)
            self.assertEqual(state_module.load(path).bindings[0].kept_reply, binding.kept_reply)
            for broken in ({"text": "x"}, {**binding.kept_reply, "text": ""}, {**binding.kept_reply, "source_seq": "5"}):
                with self.assertRaises(ValueError):
                    adapter.RoomBinding.from_dict({**stored.__dict__, "kept_reply": broken})

        # Offered once on the next model turn in the same discussion.
        kept = {"source_event_id": "m-42", "source_seq": 42, "epoch_id": "epoch-2",
                "produced_at": "2026-10-02T09:52:30Z", "reason": "turn_expired", "text": KEPT_TEXT}
        saved, prepared = [], []

        def prepare(instance, binding):
            binding.kept_reply = dict(kept)
            instance._persist_binding = lambda current: saved.append(dict(current.kept_reply)) or True
            prepared.append(binding)

        text = build_message(attempt_ready(50, 2, source="m-43"), attempt=cycle(), context=(START, HUMAN_AT, PEER),
                             prepare=prepare).text
        block = "\n".join([
            "[Your own earlier reply, kept: it was produced but not posted]",
            "Answered: #42 Owner, 09:49 UTC",
            "Produced: 09:52 UTC",
            "Not posted: turn_expired",
            KEPT_TEXT,
            "[/Your own earlier reply]",
        ])
        self.assertEqual(text.count("[Your own earlier reply"), 1, text)
        self.assertIn(block, text)
        # Procedural facts only: the connector-authored lines pass the deny-list.
        self.assertEqual(DENY_PATTERN.findall(block.replace(KEPT_TEXT, "")), [])
        # Cleared durably before core runs the turn; the next turn has none.
        self.assertEqual(prepared[0].kept_reply, {})
        self.assertEqual(saved[-1], {})
        again = build_message(attempt_ready(51, 2, source="m-43"), attempt=cycle(), context=(START, HUMAN_AT, PEER)).text
        self.assertNotIn("[Your own earlier reply", again)

        # Never across discussions: kept in epoch-1, next turn in epoch-2.
        other_saved = []

        def other(instance, binding):
            binding.kept_reply = {**kept, "epoch_id": "epoch-1"}
            instance._persist_binding = lambda current: other_saved.append(dict(current.kept_reply)) or True

        other_text = build_message(attempt_ready(52, 2, source="m-43"), attempt=cycle(), context=(START, HUMAN_AT, PEER),
                                   prepare=other).text
        self.assertNotIn("[Your own earlier reply", other_text)
        self.assertEqual(other_saved[-1], {})

    def test_p2c_03_after_network_loss_and_session_expiry_the_connector_reconnects_without_a_restart(self):
        # Wu and Real, Mac128, 2026-10-02: after a network loss the Room had no
        # live session for the installation; the connector logged "Room
        # connector enrollment check failed: an active connector session is
        # required" every 70 s until a gateway restart. Here: the stream drops,
        # heartbeats fail, then the Room says no active session (with the
        # stored session's heartbeat accepted, and with it gone). The connector
        # registers again and resumes its stream without connect().
        for variant in ("not_active", "session_gone"):
            with self.subTest(variant=variant):
                registrations, consumed, enrollment_results = self.run_outage(variant)
                after = [item for item in registrations if item["after_outage"]]
                self.assertGreaterEqual(len(after), 1, registrations)
                self.assertTrue(all(item["hostLabel"] == "host-now" for item in registrations))
                self.assertIn("ok", enrollment_results)
                self.assertEqual(consumed, ["evt-after-outage"])

    def run_outage(self, variant):
        room = types.SimpleNamespace(down=False, outage_over=False, forgotten=set(), sessions=[],
                                     registrations=[], enrollments=[], streams=0, delivered=False)
        lock = threading.Lock()

        def network():
            return adapter.ProtocolError("Room API unavailable: [Errno 51] Network is unreachable", retryable=True)

        class API:
            def status(self):
                return {"protocolCapabilities": []}

            def register_connector(self, _room, _installation, metadata):
                with lock:
                    if room.down:
                        raise network()
                    session = f"session-{len(room.sessions) + 1}"
                    room.sessions.append(session)
                    room.registrations.append({**metadata, "after_outage": room.outage_over})
                return {"sessionId": session, "leaseExpiresAt": "2099-01-01T00:00:00Z", "heartbeatIntervalSeconds": 1}

            def heartbeat(self, _room, session):
                with lock:
                    if room.down:
                        raise network()
                    if session in room.forgotten and variant == "session_gone":
                        raise adapter.ProtocolError("room or object not found", status=404, code="not_found")
                return {"leaseExpiresAt": "2099-01-01T00:00:00Z"}

            def connector_enrollments(self, _installation):
                with lock:
                    if room.down:
                        raise network()
                    if room.sessions[-1] in room.forgotten:
                        room.enrollments.append("not_active")
                        raise adapter.ProtocolError("an active connector session is required", status=401,
                                                    code="connector_not_active", retryable=True)
                    room.enrollments.append("ok")
                return []

            def stream_events(self, _room, after, on_event):
                with lock:
                    room.streams += 1
                    down, over = room.down, room.outage_over
                    if room.streams == 1:
                        room.down = True  # the laptop loses its network mid-stream
                        down = True
                if down:
                    time.sleep(0.05)
                    raise adapter.ProtocolError("SSE unavailable: timed out", retryable=True)
                if over and not room.delivered:
                    room.delivered = True
                    on_event({"id": "evt-after-outage", "seq": after + 1, "type": "message.posted"})
                time.sleep(0.1)
                return {"cursor": after, "ready": {"ok": True}}

        api = API()
        binding = adapter.RoomBinding("https://room.example/api", "room-1", "member-1", "credential",
                                      installation_id="inst-1", connector_session_id="session-stored")
        instance = object.__new__(adapter.SyntheticSocialityAdapter)
        for name, value in {
            "_stop": asyncio.Event(), "_tasks": {}, "_heartbeat_tasks": {}, "_lease_deadline": {},
            "_registered_rooms": set(), "_message_contract_checked": set(), "_effective_transports": {},
            "_stream_generations": {}, "_receive_locks": {}, "_renewing_rooms": set(), "_host_label": "host-now",
            "_status": None,
        }.items():
            setattr(instance, name, value)
        consumed = []

        async def consume(_binding, event):
            consumed.append(event["id"])

        noop = lambda *_args, **_kwargs: asyncio.sleep(0)
        instance._refresh_binding = lambda _binding: asyncio.sleep(0, result=True)
        instance._expire_stale_pending = noop
        instance._ensure_epoch_session_routing = noop
        instance._repair_pending_lifecycles = noop
        instance._publish = noop
        instance._persist_binding = lambda _binding: True
        instance._consume = consume
        instance._mark_connected = lambda: None
        instance._mark_disconnected = lambda: None
        instance._call = lambda _binding, operation: asyncio.to_thread(operation, api)
        instance._stream_call = lambda _binding, operation: asyncio.to_thread(operation, api)
        connects = []
        instance.connect = lambda **_kwargs: connects.append(True)

        async def run():
            task = asyncio.create_task(instance._watch(binding, is_reconnect=False))
            instance._tasks[binding.room_id] = task
            await asyncio.sleep(0.6)
            with lock:
                # The network is back; the Room no longer holds a live session
                # for any session registered before the outage.
                room.forgotten.update(room.sessions + ["session-stored"])
                room.down, room.outage_over = False, True
            for _ in range(80):
                if consumed and "ok" in room.enrollments:
                    break
                await asyncio.sleep(0.1)
            instance._stop.set()
            for pending in (task, *instance._heartbeat_tasks.values()):
                pending.cancel()
            await asyncio.gather(task, *instance._heartbeat_tasks.values(), return_exceptions=True)

        with mock.patch.object(adapter, "HEARTBEAT_MIN_INTERVAL_SECONDS", 0.2), \
                self.assertLogs(adapter.logger, level="INFO") as logs:
            asyncio.run(run())
        self.assertEqual(connects, [])
        self.assertFalse(any("Traceback" in line for line in logs.output))
        return room.registrations, consumed, room.enrollments

    def test_p2c_04_local_status_reports_load_version_connection_and_last_error_without_secrets_or_bodies(self):
        with tempfile.TemporaryDirectory() as scratch:
            path = Path(scratch) / "status.json"
            instance, binding, api, _published, _dispatched = room_instance(cycle_attempt())
            instance._status = local_status.LocalStatus(path, plugin="hermes", version=adapter.CONNECTOR_VERSION,
                                                        host_label="host-a")
            binding.credential = FAKE_CREDENTIAL

            async def run():
                instance._lease_deadline = {}
                instance._mark_connected = lambda: None
                instance._note_connected(binding, {"leaseExpiresAt": "2099-01-01T00:00:00Z"})
                connected = local_status.read(path)
                instance._status_room(binding, connection="reconnecting",
                                      error_code=adapter._status_error_code(adapter.ProtocolError("x", retryable=True)))
                instance._session_wait_started = {"room-1": time.time() - 2.5}
                instance._end_session_wait("room-1")
                api.post_error = lambda _n: refusal("turn_expired")
                await instance._send_final("room-1", source_ref(), f"Body that must not be in status {KEPT_TEXT}")
                return connected

            connected = asyncio.run(run())
            self.assertEqual(connected["rooms"]["room-1"]["connection"], "connected")
            written = local_status.read(path)
            self.assertEqual(written["schema"], "synthetic-sociality-room-status-v1")
            self.assertEqual((written["plugin"], written["version"], written["pluginLoaded"]),
                             ("hermes", adapter.CONNECTOR_VERSION, True))
            entry = written["rooms"]["room-1"]
            self.assertEqual((entry["membershipId"], entry["connection"], entry["lastErrorCode"]),
                             ("member-1", "reconnecting", "network"))
            self.assertIsNotNone(entry["lastConnectedAt"])
            self.assertGreaterEqual(entry["lastGateWaitSeconds"], 2.4)
            # The schema is exactly the contract appendix, identical in OpenClaw.
            self.assertEqual(flatten_keys(written), appendix_keys())
            # 0600, atomic (no temporary file left), no secret and no body.
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            self.assertEqual(sorted(item.name for item in Path(scratch).iterdir()), ["status.json"])
            raw = path.read_text()
            for forbidden in (FAKE_CREDENTIAL, KEPT_TEXT, "Body that must"):
                self.assertNotIn(forbidden, raw)
            # Revocation stops the Room; disconnect marks the plugin unloaded.
            instance._state = types.SimpleNamespace(bindings=[binding], binding=lambda _room: binding)
            with mock.patch.object(adapter, "update", lambda _merge: instance._state):
                instance._revoke(binding)
            self.assertEqual(local_status.read(path)["rooms"]["room-1"]["lastErrorCode"], "credential_revoked")
            instance._status.set_loaded(False)
            self.assertFalse(local_status.read(path)["pluginLoaded"])
            # An error code is a code, never free text.
            instance._status_room(binding, error_code="Room said: secret body text")
            self.assertEqual(local_status.read(path)["rooms"]["room-1"]["lastErrorCode"], "unknown_error")

    def test_p2c_05_the_reported_host_label_comes_from_the_current_machine(self):
        # Wu moved from Mac128 to the Mac mini and still reported "Mac128": the
        # stored session was only heartbeated, so the Room kept the metadata of
        # the old machine. Now the first contact of a process registers with the
        # label read from this machine at start.
        with mock.patch.object(adapter.platform, "node", return_value="Mac-mini"), \
                mock.patch.object(adapter, "load", lambda *_: adapter.PluginState()):
            label = adapter.SyntheticSocialityAdapter(adapter.PlatformConfig())._host_label
        self.assertEqual(label, "Mac-mini")
        calls = []

        class API:
            def status(self):
                return {"protocolCapabilities": []}

            def heartbeat(self, _room, session):
                calls.append(("heartbeat", session))
                return {"leaseExpiresAt": "2099-01-01T00:00:00Z"}

            def register_connector(self, _room, _installation, metadata):
                calls.append(("register", metadata["hostLabel"]))
                return {"sessionId": "session-resumed", "leaseExpiresAt": "2099-01-01T00:00:00Z"}

        binding = adapter.RoomBinding("https://room.example/api", "room-1", "member-1", "credential",
                                      installation_id="inst-1", connector_session_id="session-from-mac128")
        instance = object.__new__(adapter.SyntheticSocialityAdapter)
        instance._host_label = label
        instance._registered_rooms = set()
        instance._message_contract_checked = set()
        instance._lease_deadline = {}
        instance._mark_connected = lambda: None
        instance._persist_binding = lambda _binding: True
        instance._call = lambda _binding, operation: asyncio.sleep(0, result=operation(API()))

        async def run():
            await instance._ensure_connector(binding)
            await instance._ensure_connector(binding)

        asyncio.run(run())
        self.assertEqual(calls, [("register", "Mac-mini"), ("heartbeat", "session-resumed")])
        self.assertNotIn("hostLabel", adapter.RoomBinding.__dataclass_fields__)


if __name__ == "__main__":
    unittest.main()
