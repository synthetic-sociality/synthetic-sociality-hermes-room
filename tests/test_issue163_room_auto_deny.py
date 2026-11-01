"""ISSUE-163: a private approval prompt in a Room turn fails at once.

The connector answers a suppressed Hermes approval prompt with a private /deny.
Hermes keys the waiting approval by the turn's session, and a Room turn's source
carries the epoch thread, so the /deny has to reach that same session key.

The first class keys real inbound sources with the core's own builder. The
second runs in a child process, because the other suites install gateway stubs:
the core's adapter guard, gateway runner, /deny handler and approval wait are all
real there, and only the model turn around the guarded command is left out.
"""
from __future__ import annotations

import asyncio
from importlib.machinery import PathFinder
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import types
import unittest

ROOM = "room-1"
THREADED_EPOCH = "epoch-2"
LEGACY_EPOCH = "epoch-1"
# The wait the child gives up on: the live default is 300 seconds. A /deny that
# misses the waiting session shows here as this timeout, not as a denial.
PROBE_APPROVAL_TIMEOUT = 8
PROMPT = "⚠️ Dangerous command requires approval. Reply /approve or /deny."
REPORT = "ISSUE163-REPORT "


def _dispatched_turn(*, epoch_id=THREADED_EPOCH):
    """The MessageEvent of a real inbound dispatch, with its adapter and binding."""
    from test_parity_p1a import HUMAN, adapter, build_message

    held = []

    def prepare(instance, binding):
        make_source = instance.build_source

        def build_source(**kwargs):
            # The stub base has no core SessionSource: these are its defaults,
            # which the core's key builder reads.
            source = make_source(**{"thread_id": None, **kwargs})
            source.scope_id = source.user_id_alt = source.prospective_thread_id = None
            return source

        instance.build_source = build_source
        instance.config = types.SimpleNamespace(extra={})
        instance._persist_binding = lambda _binding: True
        instance._buffered_source, instance._buffered_output = {}, {}
        instance._state = types.SimpleNamespace(
            binding=lambda room_id: binding if room_id == binding.room_id else None,
        )

    turn = build_message(HUMAN, epoch_id=epoch_id, prepare=prepare, keep_instance=held)
    instance, binding = held[0]
    return adapter, instance, binding, turn


def _denies_from(instance, trigger):
    """Run a prompt through the adapter and return (result, private /deny events)."""
    denies = []

    async def handle_message(message):
        denies.append(message)

    instance.handle_message = handle_message

    async def run():
        result = await trigger()
        for _ in range(3):
            await asyncio.sleep(0)
        return result

    return asyncio.run(run()), denies


class AutoDenyReachesTheWaitingSessionKey(unittest.TestCase):
    KEY_OPTIONS = (
        {"group_sessions_per_user": False, "thread_sessions_per_user": False},
        {"group_sessions_per_user": True, "thread_sessions_per_user": False},
        {"group_sessions_per_user": True, "thread_sessions_per_user": True},
    )

    def assert_same_session(self, deny, turn):
        from test_delivery_lifecycle import hermes_build_session_key

        for options in self.KEY_OPTIONS:
            with self.subTest(**options):
                self.assertEqual(
                    hermes_build_session_key(deny.source, **options),
                    hermes_build_session_key(turn.source, **options),
                )

    def assert_private_deny(self, deny):
        self.assertEqual(deny.text, "/deny")
        self.assertTrue(deny.message_id.startswith("private-deny-"))
        self.assertEqual(deny.raw_message, {"privateControl": True})

    def test_send_prompt_with_the_core_metadata(self):
        _, instance, _, turn = _dispatched_turn()
        self.assertTrue(turn.source.thread_id.startswith("room-epoch-v1:"))
        # What the core's text fallback passes for this turn's status lane.
        metadata = {"thread_id": turn.source.thread_id, "is_approval_prompt": True, "_interim_send": True}
        result, denies = _denies_from(instance, lambda: instance.send(ROOM, PROMPT, metadata=metadata))
        self.assertTrue(result.success)
        self.assertEqual(result.message_id, "private-approval:approval")
        self.assertEqual(len(denies), 1)
        self.assert_private_deny(denies[0])
        self.assert_same_session(denies[0], turn)

    def test_send_prompt_without_thread_metadata_finds_the_turn(self):
        _, instance, _, turn = _dispatched_turn()
        result, denies = _denies_from(instance, lambda: instance.send(ROOM, PROMPT))
        self.assertTrue(result.success)
        self.assertEqual(len(denies), 1)
        self.assert_same_session(denies[0], turn)

    def test_edit_prompt_finds_the_turn_of_its_preview(self):
        _, instance, _, turn = _dispatched_turn()
        async def preview_then_prompt():
            preview = await instance.send(ROOM, "Working on it", reply_to=turn.message_id)
            return await instance.edit_message(ROOM, preview.message_id, PROMPT)
        result, denies = _denies_from(instance, preview_then_prompt)
        self.assertTrue(result.success)
        self.assertTrue(result.message_id.startswith("private-approval:"))
        self.assertEqual(len(denies), 1)
        self.assert_private_deny(denies[0])
        self.assert_same_session(denies[0], turn)

    def test_edit_prompt_keeps_its_own_turn_after_a_newer_dispatch(self):
        adapter, instance, _, turn = _dispatched_turn()
        instance._event_epoch["newer-event"] = "epoch-3"
        instance._latest_source[ROOM] = "newer-event"
        self.assertNotEqual(turn.source.thread_id, adapter._epoch_thread_id("epoch-3"))
        async def preview_then_prompt():
            preview = await instance.send(ROOM, "Working on it", reply_to=turn.message_id)
            return await instance.edit_message(ROOM, preview.message_id, PROMPT)
        _, denies = _denies_from(instance, preview_then_prompt)
        self.assert_same_session(denies[0], turn)

    def test_turn_in_the_legacy_epoch_stays_without_a_thread(self):
        for name, metadata in (("no metadata", None), ("core metadata", {"_interim_send": True})):
            with self.subTest(name):
                _, instance, _, turn = _dispatched_turn(epoch_id=LEGACY_EPOCH)
                self.assertIsNone(turn.source.thread_id)
                _, denies = _denies_from(instance, lambda: instance.send(ROOM, PROMPT, metadata=metadata))
                self.assertIsNone(denies[0].source.thread_id)
                self.assert_same_session(denies[0], turn)

    def test_prompt_with_no_turn_stays_suppressed_and_private(self):
        for name, prepare in (
            ("no dispatch yet", lambda instance, _binding: (instance._event_epoch.clear(), instance._latest_source.clear())),
            ("routing not initialized", lambda _instance, binding: setattr(binding, "epoch_session_routing_initialized", False)),
            ("room not configured", lambda instance, _binding: setattr(
                instance, "_state", types.SimpleNamespace(binding=lambda _room_id: None))),
        ):
            with self.subTest(name):
                _, instance, binding, _ = _dispatched_turn()
                prepare(instance, binding)
                result, denies = _denies_from(instance, lambda: instance.send(ROOM, PROMPT))
                self.assertTrue(result.success)
                self.assertEqual(result.message_id, "private-approval:approval")
                self.assertEqual(len(denies), 1)
                self.assert_private_deny(denies[0])
                self.assertIsNone(denies[0].source.thread_id)

    def test_reply_to_the_threaded_deny_stays_suppressed(self):
        _, instance, _, turn = _dispatched_turn()
        posted = []

        async def send_final(*args, **kwargs):
            posted.append((args, kwargs))

        instance._send_final = send_final
        metadata = {"thread_id": turn.source.thread_id}
        _, denies = _denies_from(instance, lambda: instance.send(ROOM, PROMPT, metadata=metadata))
        for reply in ("❌ Command denied.", "No pending command to deny."):
            with self.subTest(reply):
                result = asyncio.run(instance.send(
                    ROOM, reply, reply_to=denies[0].message_id, metadata={**metadata, "notify": True},
                ))
                self.assertEqual(result.message_id, "private-control-suppressed")
        self.assertEqual(posted, [])


def probe(case):
    """Block one guarded command on the real approval wait; report how it ended."""
    from types import SimpleNamespace

    from gateway.config import GatewayConfig, PlatformConfig
    from gateway.platform_registry import PlatformEntry, platform_registry
    from gateway.platforms.base import _thread_metadata_for_source
    from gateway.run import GatewayRunner, _format_exec_approval_fallback
    try:
        from gateway.run import _interim_metadata
    except ImportError:  # an older supported core has no such marker; this test only uses it to send the prompt
        def _interim_metadata(metadata=None):
            return dict(metadata or {})
    from gateway.session import build_session_key
    from tools import approval
    try:
        from tools.approval_context import set_current_session_key
    except ImportError:  # an older supported core keeps it in tools.approval
        from tools.approval import set_current_session_key
    from support import load_module

    plugin = load_module("adapter")
    report = {"case": case, "denies": [], "replies": [], "posted": []}

    platform_registry.register(PlatformEntry(
        name=plugin.NAME, label="Room", adapter_factory=plugin.SyntheticSocialityAdapter, check_fn=lambda: True,
    ))
    instance = plugin.SyntheticSocialityAdapter(PlatformConfig(enabled=True))
    binding = plugin.RoomBinding(
        "https://room.example/api", ROOM, "member-1", "credential",
        installation_id="installation-1", display_name="Agent",
    )
    binding.epoch_session_routing_initialized = True
    binding.legacy_session_epoch_id = LEGACY_EPOCH
    instance._state = SimpleNamespace(binding=lambda room_id: binding if room_id == ROOM else None)
    instance._persist_binding = lambda _binding: True

    state = {
        "title": "Room", "purpose": "ISSUE-163", "headSeq": 5, "rules": [],
        "activeEpoch": {"id": LEGACY_EPOCH if case == "legacy" else THREADED_EPOCH, "startsAtSeq": 1},
        "roster": [{"membershipId": "owner-1", "displayName": "Owner"},
                   {"membershipId": "member-1", "displayName": "Agent"}],
    }
    event = {"id": "evt-5", "seq": 5, "type": "message.posted", "actorId": "owner-1", "actorRole": "human",
             "payload": {"body": "Run it"}}
    api = SimpleNamespace(
        room_state=lambda _room: state,
        room_policy=lambda _room: {"policy": {}},
        events=lambda _room, after, _wait: {"events": [item for item in [event] if item["seq"] > after]},
        artifacts=lambda _room: {"items": []},
        artifact=lambda _room, _artifact_id: {},
    )
    instance._call = lambda _binding, operation, **_options: asyncio.sleep(0, result=operation(api))
    instance._publish = lambda *_args, **_kwargs: asyncio.sleep(0)

    runner = object.__new__(GatewayRunner)
    runner.config = GatewayConfig(platforms={instance.platform: PlatformConfig(enabled=True)})
    runner.adapters = {instance.platform: instance}
    runner.session_store = None
    runner._running_agents = {}
    runner._pending_approvals = {}
    runner.hooks = SimpleNamespace(emit=lambda *_a, **_k: asyncio.sleep(0), loaded_hooks=False)

    waiting = {}
    finished = None

    async def run_turn(message):
        """What the core does around a guarded command in this turn's session."""
        key = runner._session_key_for_source(message.source)
        waiting["key"] = key
        report["turn_thread"] = message.source.thread_id
        runner._running_agents[key] = SimpleNamespace(interrupt=lambda *_a, **_k: None)
        loop = asyncio.get_running_loop()
        lane = _thread_metadata_for_source(message.source, message.message_id)

        def notify(data):
            if case == "edit":
                return
            text = _format_exec_approval_fallback(data["command"], data["description"], "/")
            asyncio.run_coroutine_threadsafe(
                instance.send(message.source.chat_id, text, metadata=_interim_metadata(lane)), loop,
            ).result(timeout=15)

        def guarded():
            set_current_session_key(key)
            started = time.monotonic()
            result = approval.check_all_command_guards("rm -rf /tmp/issue163-probe", "local")
            return result, time.monotonic() - started

        approval.register_gateway_notify(key, notify)
        try:
            result, elapsed = await asyncio.to_thread(guarded)
        finally:
            approval.unregister_gateway_notify(key)
            runner._running_agents.pop(key, None)
        report["approved"] = result.get("approved")
        report["message"] = result.get("message")
        report["elapsed"] = round(elapsed, 2)
        finished.set()
        return None

    async def message_handler(message):
        if message.get_command():
            return await runner._handle_message(message)
        return await run_turn(message)

    instance.set_message_handler(message_handler)

    core_handle = instance.handle_message

    async def handle_message(message):
        if str(message.message_id).startswith("private-deny-"):
            extra = instance.config.extra
            report["denies"].append({
                "key": runner._session_key_for_source(message.source),
                "thread": message.source.thread_id,
                # The guard of the core's handle_message: a busy session takes
                # /deny through its bypass, never through the pending queue.
                "busy_session": build_session_key(
                    message.source,
                    group_sessions_per_user=extra.get("group_sessions_per_user", True),
                    thread_sessions_per_user=extra.get("thread_sessions_per_user", False),
                ) in instance._active_sessions,
            })
        await core_handle(message)

    instance.handle_message = handle_message

    core_send = instance.send

    async def send(chat_id, content, reply_to=None, metadata=None):
        result = await core_send(chat_id, content, reply_to=reply_to, metadata=metadata)
        report["replies"].append({"content": content[:60], "message_id": result.message_id})
        return result

    instance.send = send

    async def send_final(*args, **_kwargs):
        report["posted"].append(str(args[2])[:60])

    instance._send_final = send_final
    core_complete = instance.on_processing_complete

    async def on_processing_complete(message, outcome):
        # The Room lifecycle of the turn itself is not under test; the private
        # control event goes through the connector's real callback.
        if str(message.message_id).startswith("private-deny-"):
            await core_complete(message, outcome)

    instance.on_processing_complete = on_processing_complete

    async def main():
        nonlocal finished
        finished = asyncio.Event()
        if case == "no_turn":
            sent = await instance.send(ROOM, PROMPT)
            report["prompt_result"] = sent.message_id
            await asyncio.sleep(1.0)
            return
        dispatch = asyncio.create_task(instance._dispatch_room_event(binding, event, "generation-5"))
        deadline = time.monotonic() + 20
        while not (waiting.get("key") and approval.has_blocking_approval(waiting["key"])):
            if time.monotonic() > deadline:
                raise AssertionError(f"the guarded command never reached the approval wait: {report}")
            await asyncio.sleep(0.02)
        report["waiting_key"] = waiting["key"]
        if case == "edit":
            source_ref = plugin._dispatch_source_ref(event["id"], "generation-5")
            preview = await instance.send(ROOM, "Working on it", reply_to=source_ref)
            await instance.edit_message(ROOM, preview.message_id, PROMPT)
        await asyncio.wait_for(finished.wait(), PROBE_APPROVAL_TIMEOUT + 15)
        await dispatch
        await asyncio.sleep(0.2)

    asyncio.run(main())
    print(REPORT + json.dumps(report, sort_keys=True))


class RealCoreWaitEndsOnThePrivateDeny(unittest.TestCase):
    def run_probe(self, case):
        spec = PathFinder.find_spec("model_tools", sys.path)
        self.assertIsNotNone(spec, "Put a supported Hermes core on PYTHONPATH")
        core = Path(os.environ.get("HERMES_CORE_PATH", Path(spec.origin).parent))
        env = {key: value for key, value in os.environ.items() if key != "HERMES_YOLO_MODE"}
        with tempfile.TemporaryDirectory() as home:
            (Path(home) / "config.yaml").write_text(
                "approvals:\n"
                f"  timeout: {PROBE_APPROVAL_TIMEOUT}\n"
                "  mode: manual\n"
                "security:\n"
                "  tirith_enabled: false\n"
            )
            env.update(
                HERMES_HOME=home, HERMES_EXEC_ASK="1", PYTHONDONTWRITEBYTECODE="1",
                PYTHONPATH=os.pathsep.join([str(core), str(Path(__file__).parent)]),
            )
            result = subprocess.run(
                [sys.executable, str(Path(__file__).resolve()), "--probe", case],
                env=env, text=True, capture_output=True, timeout=120,
            )
        self.assertEqual(result.returncode, 0, result.stdout[-2000:] + result.stderr[-4000:])
        line = next(item for item in result.stdout.splitlines() if item.startswith(REPORT))
        return json.loads(line[len(REPORT):])

    def assert_denied_at_once(self, report):
        self.assertEqual(len(report["denies"]), 1, report)
        self.assertEqual(report["denies"][0]["key"], report["waiting_key"], report)
        self.assertTrue(report["denies"][0]["busy_session"], report)
        self.assertIs(report["approved"], False, report)
        self.assertIn("denied by user", report["message"], report)
        self.assertNotIn("timed out", report["message"], report)
        self.assertLess(report["elapsed"], PROBE_APPROVAL_TIMEOUT - 2, report)
        self.assertEqual(report["posted"], [], report)
        answers = [item for item in report["replies"] if item["message_id"] == "private-control-suppressed"]
        self.assertEqual(len(answers), 1, report)
        self.assertIn("denied", answers[0]["content"], report)

    def test_send_prompt_ends_the_wait_of_a_threaded_turn(self):
        report = self.run_probe("send")
        self.assertTrue(report["turn_thread"].startswith("room-epoch-v1:"), report)
        self.assert_denied_at_once(report)

    def test_edit_prompt_ends_the_wait_of_a_threaded_turn(self):
        report = self.run_probe("edit")
        self.assertTrue(report["turn_thread"].startswith("room-epoch-v1:"), report)
        self.assert_denied_at_once(report)

    def test_send_prompt_ends_the_wait_of_a_legacy_epoch_turn(self):
        report = self.run_probe("legacy")
        self.assertIsNone(report["turn_thread"], report)
        self.assert_denied_at_once(report)

    def test_prompt_with_no_turn_answers_privately(self):
        report = self.run_probe("no_turn")
        self.assertEqual(report["prompt_result"], "private-approval:approval", report)
        self.assertEqual(report["posted"], [], report)
        self.assertEqual(len(report["denies"]), 1, report)
        self.assertIsNone(report["denies"][0]["thread"], report)
        answers = [item for item in report["replies"] if item["message_id"] == "private-control-suppressed"]
        self.assertEqual([item["content"] for item in answers], ["No pending command to deny."], report)


if __name__ == "__main__":
    if "--probe" in sys.argv:
        probe(sys.argv[sys.argv.index("--probe") + 1])
    else:
        unittest.main()
