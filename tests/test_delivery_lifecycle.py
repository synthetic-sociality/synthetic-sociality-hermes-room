from __future__ import annotations

import asyncio
import copy
import hashlib
import importlib.util
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path

from gateway.session import build_session_key as hermes_build_session_key

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = "synthetic_sociality_delivery_lifecycle_test"


def load_adapter():
    gateway = types.ModuleType("gateway")
    config = types.ModuleType("gateway.config")
    platforms = types.ModuleType("gateway.platforms")
    base = types.ModuleType("gateway.platforms.base")

    class Platform(str):
        @property
        def value(self):
            return str(self)

    class PlatformConfig:
        def __init__(self):
            self.extra = {}

    class BasePlatformAdapter:
        def __init__(self, config, platform):
            self.config = config
            self.platform = platform

        def build_source(self, **kwargs):
            return types.SimpleNamespace(platform=self.platform, **kwargs)

    class MessageEvent:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class MessageType:
        TEXT = "text"

    class SendResult:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    config.Platform, config.PlatformConfig = Platform, PlatformConfig
    base.BasePlatformAdapter = BasePlatformAdapter
    base.MessageEvent, base.MessageType, base.SendResult = MessageEvent, MessageType, SendResult
    sys.modules.update({
        "gateway": gateway,
        "gateway.config": config,
        "gateway.platforms": platforms,
        "gateway.platforms.base": base,
    })
    spec = importlib.util.spec_from_file_location(
        PACKAGE, ROOT / "__init__.py", submodule_search_locations=[str(ROOT)],
    )
    package = importlib.util.module_from_spec(spec)
    sys.modules[PACKAGE] = package
    package.__path__ = [str(ROOT)]
    for name in ("state", "protocol", "cli"):
        child_spec = importlib.util.spec_from_file_location(f"{PACKAGE}.{name}", ROOT / f"{name}.py")
        child = importlib.util.module_from_spec(child_spec)
        sys.modules[f"{PACKAGE}.{name}"] = child
        child_spec.loader.exec_module(child)
    adapter_spec = importlib.util.spec_from_file_location(f"{PACKAGE}.adapter", ROOT / "adapter.py")
    adapter = importlib.util.module_from_spec(adapter_spec)
    sys.modules[f"{PACKAGE}.adapter"] = adapter
    adapter_spec.loader.exec_module(adapter)
    return adapter


adapter = load_adapter()
state_store = sys.modules[f"{PACKAGE}.state"]


def persisted_instance(path):
    instance = object.__new__(adapter.SyntheticSocialityAdapter)
    instance._persist_binding = types.MethodType(adapter.SyntheticSocialityAdapter._persist_binding, instance)
    original_update = adapter.update
    adapter.update = lambda mutator: state_store.update(mutator, path)
    return instance, original_update


def lifecycle_binding():
    binding = adapter.RoomBinding(
        "https://room.example/api", "room-1", "member-1", "credential",
        installation_id="installation-1", cursor=5, acknowledged_cursor=4,
    )
    binding.delivery_intents["evt-5"] = {
        "delivery_state": "posted",
        "lifecycle_state": "pending",
        "state": "lifecycle_pending",
        "canonical_event": {"id": "posted-6", "seq": 6, "ts": "2026-08-17T00:00:00Z"},
    }
    binding.delivery_lifecycle["evt-5"] = {
        "state": "lifecycle_pending",
        "delivery_state": "posted",
        "lifecycle_state": "pending",
        "receipt": {
            "source_event_id": "evt-5", "source_seq": 5,
            "canonical_event_id": "posted-6", "canonical_seq": 6,
            "canonical_ts": "2026-08-17T00:00:00Z",
        },
        "completion": {
            "kind": "cycle", "cycle_id": "cycle-1", "attempt_id": "attempt-1",
            "payload": {"generation": 3, "action": "contribute", "eventId": "posted-6"},
        },
        "attempts": 1,
        "automatic_retry": True,
        "binding": {
            "membership_id": "member-1", "installation_id": "installation-1", "identity_version": 0,
        },
    }
    return binding


def turn_lifecycle_binding():
    binding = lifecycle_binding()
    binding.delivery_lifecycle["evt-5"]["completion"] = {
        "kind": "turn", "turn_id": "turn-1", "observed_seq": 6,
        "source_event_id": "evt-5", "idempotency_key": "finish-key",
    }
    return binding


def configured_instance(binding, cycle_attempt, snapshots):
    instance = object.__new__(adapter.SyntheticSocialityAdapter)
    instance._binding = lambda _room_id: binding
    instance._binding_generation_active = lambda _binding: True
    instance._persist_binding = lambda current: snapshots.append(copy.deepcopy(current.delivery_intents)) or True
    instance._event_seq = {binding.room_id: {"evt-5": 5}}
    instance._event_epoch = {"evt-5": "epoch-1"}
    instance._source_coordination_modes = {"evt-5": "open"}
    instance._open_reply_recipients = {}
    instance._cycle_attempts = {"evt-5": cycle_attempt}
    instance._cycle_response_sources = {"evt-5": "evt-human"}
    instance._superseded_sources = set()
    instance._attempt_renewal_tasks = {}
    instance._terminal_sources = {}
    instance._terminal_results = {}
    instance._run_for_event = {"evt-5": "run-1"}
    instance._activity_seq = {}
    instance._state = None
    return instance


class DeliveryLifecycleContractTests(unittest.TestCase):
    def test_extract_visible_body_recovers_escaped_layout_outside_json_strings(self):
        response = (
            r'{\n  "action": "contribute",\n  "body": '
            r'"**Real — SDG 14.**\n\nCentral trade-off: coastal runoff."\n}'
        )

        self.assertEqual(
            adapter.extract_visible_body(response),
            "**Real — SDG 14.**\n\nCentral trade-off: coastal runoff.",
        )

    def test_epoch_thread_id_is_stable_within_epoch_and_rotates_between_epochs(self):
        expected = "room-epoch-v1:" + hashlib.sha256(b"epoch-1").hexdigest()
        self.assertEqual(adapter._epoch_thread_id("epoch-1"), expected)
        self.assertEqual(adapter._epoch_thread_id("epoch-1"), adapter._epoch_thread_id("epoch-1"))
        self.assertNotEqual(adapter._epoch_thread_id("epoch-1"), adapter._epoch_thread_id("epoch-2"))

    def test_epoch_thread_id_hashes_exact_utf8_without_normalizing_hostile_values(self):
        hostile = " epoch\n\x00\t"
        very_long = "x" * 10000
        self.assertEqual(
            adapter._epoch_thread_id(hostile),
            "room-epoch-v1:" + hashlib.sha256(hostile.encode("utf-8")).hexdigest(),
        )
        self.assertEqual(len(adapter._epoch_thread_id(very_long)), len("room-epoch-v1:") + 64)
        self.assertNotEqual(adapter._epoch_thread_id("epoch"), adapter._epoch_thread_id(" epoch"))
        self.assertNotEqual(adapter._epoch_thread_id("epoch"), adapter._epoch_thread_id("epoch "))

    def test_epoch_thread_id_rejects_missing_blank_and_non_string_epoch(self):
        for value in ("", " \t\n", None, 7):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "active epoch"):
                adapter._epoch_thread_id(value)

    def test_existing_binding_keeps_baseline_epoch_but_rotates_later_epoch(self):
        binding = lifecycle_binding()
        binding.epoch_session_routing_initialized = True
        binding.legacy_session_epoch_id = "epoch-1"
        self.assertIsNone(adapter._session_thread_for_epoch(binding, "epoch-1"))
        self.assertEqual(adapter._session_thread_for_epoch(binding, "epoch-2"), adapter._epoch_thread_id("epoch-2"))

    def test_dispatch_source_integrates_with_real_hermes_session_key_routing(self):
        async def dispatch(binding, epoch_id, actor, event_id):
            captured = []
            instance = object.__new__(adapter.SyntheticSocialityAdapter)
            instance.platform = adapter.Platform(adapter.NAME)
            instance._inflight_events = set()
            instance._event_seq = {}
            instance._event_epoch = {}
            instance._latest_source = {}
            instance._run_for_event = {}
            instance._cycle_attempts = {}
            instance._call = lambda _binding, operation: asyncio.sleep(
                0, result=operation(types.SimpleNamespace(
                    room_state=lambda _room_id: {
                        "headSeq": 9,
                        "activeEpoch": {"id": epoch_id, "startsAtSeq": 1},
                        "members": [{"id": actor, "displayName": actor}],
                    },
                    events=lambda *_args: {"events": []},
                )),
            )
            instance._publish = lambda *_args, **_kwargs: asyncio.sleep(0)
            instance.build_source = types.MethodType(
                lambda self, **kwargs: types.SimpleNamespace(
                    platform=self.platform, scope_id=None, user_id_alt=None,
                    prospective_thread_id=None, **kwargs,
                ),
                instance,
            )
            instance.handle_message = lambda event: asyncio.sleep(0, result=captured.append(event))
            event = {
                "id": event_id, "seq": 9, "type": "message.created",
                "actorMembershipId": actor, "payload": {"body": "hello"},
            }
            self.assertTrue(await instance._dispatch_room_event(binding, event, "generation"))
            return captured[0].source

        binding = lifecycle_binding()
        binding.epoch_session_routing_initialized = True
        binding.legacy_session_epoch_id = "baseline"
        alice = asyncio.run(dispatch(binding, "epoch-2", "alice", "evt-a"))
        bob = asyncio.run(dispatch(binding, "epoch-2", "bob", "evt-b"))
        later = asyncio.run(dispatch(binding, "epoch-3", "alice", "evt-c"))
        alice_key = hermes_build_session_key(alice, group_sessions_per_user=False, profile="real")
        bob_key = hermes_build_session_key(bob, group_sessions_per_user=False, profile="real")
        later_key = hermes_build_session_key(later, group_sessions_per_user=False, profile="real")
        self.assertEqual(alice.chat_id, "room-1")
        self.assertEqual(alice_key, bob_key)
        self.assertNotEqual(alice_key, later_key)
        self.assertTrue(alice_key.startswith("agent:real:synthetic_sociality:group:room-1:"))

    def test_historical_selected_recovery_is_fenced_before_delivery_io(self):
        async def run():
            binding = lifecycle_binding()
            binding.delivery_intents["evt-old"] = {
                "selected": {
                    "state": "selected", "action": "post", "source_event_id": "evt-old",
                    "source_seq": 4, "body": "stale", "observed_epoch_id": "old",
                    "binding": {"membership_id": "member-1", "installation_id": "installation-1", "identity_version": 0},
                },
            }
            instance = object.__new__(adapter.SyntheticSocialityAdapter)
            instance._inflight_events = set()
            instance._event_seq = {}
            instance._call = lambda _binding, operation: asyncio.sleep(
                0, result=operation(types.SimpleNamespace(room_state=lambda _room_id: {
                    "activeEpoch": {"id": "new", "startsAtSeq": 5},
                })),
            )
            completed = []
            instance._complete_event = lambda *_args, **kwargs: asyncio.sleep(0, result=completed.append(kwargs))
            instance._send_final = lambda *_args, **_kwargs: self.fail("stale recovery performed delivery I/O")
            result = await instance._dispatch_room_event(
                binding, {"id": "evt-old", "seq": 4, "payload": {"body": "old"}}, "restart",
            )
            self.assertFalse(result)
            self.assertEqual(completed[0]["reason"], "historical_epoch_before_dispatch")

        asyncio.run(run())

    def test_inflight_old_epoch_output_is_superseded_before_selection_or_post(self):
        async def run():
            binding = lifecycle_binding()
            binding.delivery_intents.clear()
            instance = configured_instance(binding, None, [])
            instance._event_seq = {"room-1": {"evt-5": 5}}
            instance._event_epoch = {"evt-5": "old"}
            instance._call = lambda _binding, operation: asyncio.sleep(
                0, result=operation(types.SimpleNamespace(room_state=lambda _room_id: {
                    "headSeq": 8, "activeEpoch": {"id": "new", "startsAtSeq": 6},
                })),
            )
            instance._select_delivery_intent = lambda *_args: self.fail("stale output was frozen")
            instance._publish = lambda *_args, **_kwargs: asyncio.sleep(0)
            result = await instance._send_final_owned(
                "room-1", adapter._dispatch_source_ref("evt-5", "generation"), "stale output",
            )
            self.assertTrue(result.success)
            self.assertEqual(instance._terminal_results[adapter._dispatch_source_ref("evt-5", "generation")]["reason"], "stale_epoch")
            self.assertEqual(binding.delivery_intents, {})

        asyncio.run(run())

    def test_uninitialized_epoch_routing_fails_closed(self):
        binding = lifecycle_binding()
        with self.assertRaisesRegex(ValueError, "not initialized"):
            adapter._session_thread_for_epoch(binding, "epoch-1")

    def test_epoch_routing_initialization_preserves_existing_context_unless_rotation_is_authorized(self):
        async def initialize(binding):
            instance = object.__new__(adapter.SyntheticSocialityAdapter)
            instance._call = lambda _binding, operation: asyncio.sleep(
                0, result=operation(types.SimpleNamespace(
                    room_state=lambda _room_id: {"activeEpoch": {"id": "epoch-current"}},
                )),
            )
            instance._persist_binding = lambda _binding: True
            await instance._ensure_epoch_session_routing(binding)

        existing = lifecycle_binding()
        existing.cursor = 5
        asyncio.run(initialize(existing))
        self.assertTrue(existing.epoch_session_routing_initialized)
        self.assertEqual(existing.legacy_session_epoch_id, "epoch-current")

        empty_existing = lifecycle_binding()
        empty_existing.cursor = 0
        asyncio.run(initialize(empty_existing))
        self.assertEqual(empty_existing.legacy_session_epoch_id, "epoch-current")

        rotated = lifecycle_binding()
        rotated.cursor = 5
        rotated.rotate_current_epoch_session = True
        asyncio.run(initialize(rotated))
        self.assertTrue(rotated.epoch_session_routing_initialized)
        self.assertEqual(rotated.legacy_session_epoch_id, "")
        self.assertFalse(rotated.rotate_current_epoch_session)

    def test_epoch_routing_markers_roundtrip_real_state_persistence(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            binding = lifecycle_binding()
            binding.epoch_session_routing_initialized = True
            binding.legacy_session_epoch_id = "epoch-current"
            state_store.save(state_store.PluginState(bindings=[binding]), path)
            reloaded = state_store.load(path).binding("room-1")
            self.assertTrue(reloaded.epoch_session_routing_initialized)
            self.assertEqual(reloaded.legacy_session_epoch_id, "epoch-current")

    def test_epoch_routing_initialization_uses_real_merge_and_survives_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            binding = lifecycle_binding()
            binding.epoch_session_routing_initialized = False
            binding.legacy_session_epoch_id = ""
            state_store.save(state_store.PluginState(bindings=[binding]), path)
            instance, original_update = persisted_instance(path)
            instance._call = lambda _binding, operation: asyncio.sleep(
                0, result=operation(types.SimpleNamespace(room_state=lambda _room_id: {
                    "activeEpoch": {"id": " epoch-current "},
                })),
            )
            try:
                asyncio.run(instance._ensure_epoch_session_routing(binding))
            finally:
                adapter.update = original_update
            restarted = state_store.load(path).binding("room-1")
            self.assertTrue(restarted.epoch_session_routing_initialized)
            self.assertEqual(restarted.legacy_session_epoch_id, " epoch-current ")
            self.assertIsNone(adapter._session_thread_for_epoch(restarted, " epoch-current "))
            self.assertEqual(
                adapter._session_thread_for_epoch(restarted, "next"),
                adapter._epoch_thread_id("next"),
            )

    def test_epoch_routing_initialization_rolls_back_memory_when_persistence_fails(self):
        async def run():
            binding = lifecycle_binding()
            binding.rotate_current_epoch_session = True
            instance = object.__new__(adapter.SyntheticSocialityAdapter)
            instance._call = lambda _binding, operation: asyncio.sleep(
                0, result=operation(types.SimpleNamespace(room_state=lambda _room_id: {
                    "activeEpoch": {"id": "current"},
                })),
            )
            instance._persist_binding = lambda _binding: False
            with self.assertRaisesRegex(adapter.ProtocolError, "before epoch session routing was saved"):
                await instance._ensure_epoch_session_routing(binding)
            self.assertFalse(binding.epoch_session_routing_initialized)
            self.assertEqual(binding.legacy_session_epoch_id, "")
            self.assertTrue(binding.rotate_current_epoch_session)

        asyncio.run(run())

    def test_operator_authorized_current_epoch_rotation_is_durable_and_one_shot(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            binding = lifecycle_binding()
            binding.epoch_session_routing_initialized = True
            binding.legacy_session_epoch_id = "current"
            state_store.save(state_store.PluginState(bindings=[binding]), path)
            stale_runtime_binding = copy.deepcopy(binding)
            room_cli = adapter.cli
            original_load, original_update = room_cli.load, room_cli.update
            room_cli.load = lambda: state_store.load(path)
            room_cli.update = lambda mutator: state_store.update(mutator, path)
            try:
                self.assertEqual(room_cli._rotate_current_epoch_session("room-1", confirmed=True), 0)
            finally:
                room_cli.load, room_cli.update = original_load, original_update
            authorized = state_store.load(path).binding("room-1")
            self.assertFalse(authorized.epoch_session_routing_initialized)
            self.assertEqual(authorized.legacy_session_epoch_id, "")
            self.assertTrue(authorized.rotate_current_epoch_session)

            stale_instance, original_adapter_update = persisted_instance(path)
            try:
                self.assertTrue(stale_instance._persist_binding(stale_runtime_binding))
            finally:
                adapter.update = original_adapter_update
            still_authorized = state_store.load(path).binding("room-1")
            self.assertFalse(still_authorized.epoch_session_routing_initialized)
            self.assertEqual(still_authorized.legacy_session_epoch_id, "")
            self.assertTrue(still_authorized.rotate_current_epoch_session)

            restarted_instance, original_adapter_update = persisted_instance(path)
            restarted_instance._call = lambda _binding, operation: asyncio.sleep(
                0, result=operation(types.SimpleNamespace(room_state=lambda _room_id: {
                    "activeEpoch": {"id": "current"},
                })),
            )
            try:
                asyncio.run(restarted_instance._ensure_epoch_session_routing(still_authorized))
            finally:
                adapter.update = original_adapter_update
            consumed = state_store.load(path).binding("room-1")
            self.assertTrue(consumed.epoch_session_routing_initialized)
            self.assertEqual(consumed.legacy_session_epoch_id, "")
            self.assertFalse(consumed.rotate_current_epoch_session)

    def test_real_persist_merge_roundtrips_nested_lifecycle_journal(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            binding = lifecycle_binding()
            initial = copy.deepcopy(binding)
            initial.delivery_lifecycle = {}
            state_store.save(state_store.PluginState(bindings=[initial]), path)
            expected = copy.deepcopy(binding.delivery_lifecycle)
            instance, original_update = persisted_instance(path)
            try:
                self.assertTrue(instance._persist_binding(binding))
            finally:
                adapter.update = original_update

            reloaded = state_store.load(path).binding(binding.room_id)
            self.assertIsNotNone(reloaded)
            self.assertEqual(reloaded.delivery_lifecycle, expected)
            self.assertEqual(reloaded.delivery_lifecycle["evt-5"]["completion"]["payload"]["eventId"], "posted-6")
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_lifecycle_journal_survives_intent_cleanup_and_source_ack(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            binding = lifecycle_binding()
            initial = copy.deepcopy(binding)
            initial.delivery_lifecycle = {}
            state_store.save(state_store.PluginState(bindings=[initial]), path)
            expected = copy.deepcopy(binding.delivery_lifecycle["evt-5"])
            binding.delivery_intents.pop("evt-5")
            binding.inbox["5"] = "posted"
            binding.terminal_evidence["5"] = {
                "status": "posted", "sourceEventId": "evt-5", "canonicalEventId": "posted-6",
            }
            binding.acknowledged_cursor = 5
            instance, original_update = persisted_instance(path)
            try:
                self.assertTrue(instance._persist_binding(binding))
            finally:
                adapter.update = original_update

            reloaded = state_store.load(path).binding(binding.room_id)
            self.assertNotIn("evt-5", reloaded.delivery_intents)
            self.assertEqual(reloaded.acknowledged_cursor, 5)
            self.assertEqual(reloaded.delivery_lifecycle["evt-5"], expected)

    def test_actual_source_ack_preserves_unfinished_lifecycle_journal(self):
        async def exercise(path, lifecycle_state):
            binding = lifecycle_binding()
            binding.turn_sequences["evt-5"] = 5
            journal = binding.delivery_lifecycle["evt-5"]
            journal["lifecycle_state"] = lifecycle_state
            journal["state"] = f"lifecycle_{lifecycle_state}"
            journal["automatic_retry"] = lifecycle_state == "pending"
            state_store.save(state_store.PluginState(bindings=[copy.deepcopy(binding)]), path)
            instance, original_update = persisted_instance(path)
            instance._ledger_locks = {}
            instance._context_activity_pending = {}
            acknowledgements = []

            class API:
                def acknowledge(self, room_id, seq):
                    persisted = state_store.load(path).binding(room_id)
                    if "evt-5" in persisted.delivery_intents:
                        raise AssertionError("terminal intent cleanup was not persisted before ack")
                    if persisted.delivery_lifecycle.get("evt-5", {}).get("lifecycle_state") != lifecycle_state:
                        raise AssertionError("unfinished lifecycle was lost before ack")
                    if persisted.terminal_evidence.get("5") != {
                        "status": "posted",
                        "sourceEventId": "evt-5",
                        "canonicalEventId": "posted-6",
                        "canonicalSeq": 6,
                        "canonicalTs": "2026-08-17T00:00:00Z",
                        "reason": "",
                    }:
                        raise AssertionError("complete canonical receipt was not durable before ack")
                    acknowledgements.append(seq)
                    return {"acknowledgedSeq": seq}

            api = API()
            instance._call = lambda _binding, operation: asyncio.sleep(0, result=operation(api))
            try:
                await instance._complete_event(
                    binding, 5, terminal_status="posted", source_id="evt-5",
                    canonical_event_id="posted-6",
                )
            finally:
                adapter.update = original_update
            return acknowledgements

        for lifecycle_state in ("pending", "blocked"):
            with self.subTest(lifecycle_state=lifecycle_state), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "state.json"
                acknowledgements = asyncio.run(exercise(path, lifecycle_state))
                reloaded = state_store.load(path).binding("room-1")
                self.assertEqual(acknowledgements, [5])
                self.assertEqual(reloaded.acknowledged_cursor, 5)
                self.assertNotIn("evt-5", reloaded.delivery_intents)
                self.assertEqual(
                    reloaded.delivery_lifecycle["evt-5"]["lifecycle_state"], lifecycle_state,
                )

    def test_lifecycle_completion_cleanup_is_durable_and_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            binding = lifecycle_binding()
            binding.delivery_intents.pop("evt-5")
            binding.terminal_evidence["5"] = {
                "status": "posted", "sourceEventId": "evt-5", "canonicalEventId": "posted-6",
            }
            state_store.save(state_store.PluginState(bindings=[copy.deepcopy(binding)]), path)
            instance, original_update = persisted_instance(path)
            try:
                instance._record_lifecycle_complete(binding, "evt-5")
                instance._record_lifecycle_complete(binding, "evt-5")
            finally:
                adapter.update = original_update

            reloaded = state_store.load(path).binding(binding.room_id)
            self.assertNotIn("evt-5", reloaded.delivery_lifecycle)
            self.assertEqual(reloaded.terminal_evidence, binding.terminal_evidence)
            self.assertNotIn("evt-5", reloaded.delivery_intents)

    def test_restart_repairs_only_persisted_lifecycle_and_durably_cleans_journal(self):
        async def exercise(path):
            reloaded = state_store.load(path).binding("room-1")
            instance, original_update = persisted_instance(path)
            calls = {"complete": 0}

            class API:
                def complete_discussion_attempt(self, room_id, cycle_id, attempt_id, payload):
                    calls["complete"] += 1
                    persisted = state_store.load(path).binding(room_id).delivery_lifecycle["evt-5"]
                    if persisted["attempts"] != 2 or persisted["receipt"]["canonical_event_id"] != "posted-6":
                        raise AssertionError("receipt and incremented attempt were not durable before lifecycle I/O")
                    if persisted["completion"] != reloaded.delivery_lifecycle["evt-5"]["completion"]:
                        raise AssertionError("persisted lifecycle request changed before lifecycle I/O")
                    if (room_id, cycle_id, attempt_id, payload["eventId"]) != (
                        "room-1", "cycle-1", "attempt-1", "posted-6",
                    ):
                        raise AssertionError("persisted lifecycle request changed")
                    return {"state": "completed"}

                def __getattr__(self, name):
                    raise AssertionError(f"restart must not invoke {name}")

            api = API()
            instance._call = lambda _binding, operation: asyncio.sleep(0, result=operation(api))
            try:
                await instance._repair_pending_lifecycles(reloaded)
            finally:
                adapter.update = original_update
            return calls

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            state_store.save(state_store.PluginState(bindings=[lifecycle_binding()]), path)
            calls = asyncio.run(exercise(path))
            reloaded = state_store.load(path).binding("room-1")
            self.assertEqual(calls, {"complete": 1})
            self.assertNotIn("evt-5", reloaded.delivery_lifecycle)

    def test_blocked_lifecycle_survives_reload_and_is_not_retried(self):
        async def exercise(path):
            reloaded = state_store.load(path).binding("room-1")
            instance, original_update = persisted_instance(path)
            instance._call = lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("blocked lifecycle must not perform network I/O")
            )
            try:
                await instance._repair_pending_lifecycles(reloaded)
            finally:
                adapter.update = original_update

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            binding = lifecycle_binding()
            journal = binding.delivery_lifecycle["evt-5"]
            journal.update(
                state="lifecycle_blocked", lifecycle_state="blocked",
                automatic_retry=False, last_error_code="cycle_conflict",
            )
            state_store.save(state_store.PluginState(bindings=[binding]), path)
            asyncio.run(exercise(path))
            reloaded = state_store.load(path).binding("room-1")
            self.assertEqual(reloaded.delivery_lifecycle["evt-5"]["lifecycle_state"], "blocked")
            self.assertFalse(reloaded.delivery_lifecycle["evt-5"]["automatic_retry"])

    def test_repair_blocks_exhausted_and_nonretryable_lifecycle_work(self):
        async def exercise(path, error, attempts):
            binding = lifecycle_binding()
            binding.delivery_lifecycle["evt-5"]["attempts"] = attempts
            state_store.save(state_store.PluginState(bindings=[binding]), path)
            reloaded = state_store.load(path).binding("room-1")
            instance, original_update = persisted_instance(path)
            calls = 0

            class API:
                def complete_discussion_attempt(self, *_args):
                    nonlocal calls
                    calls += 1
                    raise error

            api = API()
            instance._call = lambda _binding, operation: asyncio.sleep(0, result=operation(api))
            try:
                await instance._repair_pending_lifecycles(reloaded)
                await instance._repair_pending_lifecycles(reloaded)
            finally:
                adapter.update = original_update
            return calls, state_store.load(path).binding("room-1").delivery_lifecycle["evt-5"]

        cases = (
            ("already exhausted", adapter.ProtocolError("temporary", code="busy", retryable=True), 3, 0, 3),
            ("retryable reaches budget", adapter.ProtocolError("temporary", code="busy", retryable=True), 2, 1, 3),
            ("nonretryable", adapter.ProtocolError("conflict", code="cycle_conflict", retryable=False), 1, 1, 2),
        )
        for name, error, attempts, expected_calls, expected_attempts in cases:
            with self.subTest(case=name), tempfile.TemporaryDirectory() as directory:
                calls, journal = asyncio.run(exercise(Path(directory) / "state.json", error, attempts))
                self.assertEqual(calls, expected_calls)
                self.assertEqual(journal["attempts"], expected_attempts)
                self.assertEqual(journal["lifecycle_state"], "blocked")
                self.assertEqual(journal["state"], "lifecycle_blocked")
                self.assertFalse(journal["automatic_retry"])

    def test_posted_state_requires_complete_receipt_before_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            binding = lifecycle_binding()
            binding.delivery_intents["evt-5"].update(
                delivery_state="posted", lifecycle_state="pending", state="lifecycle_pending",
                canonical_event={"id": "posted-6", "seq": 6},
            )
            state_store.save(state_store.PluginState(bindings=[binding]), path)
            with self.assertRaisesRegex(ValueError, "canonical receipt"):
                state_store.load(path)

    def test_legacy_incomplete_posted_evidence_loads_but_cannot_ack(self):
        async def exercise(path):
            binding = state_store.load(path).binding("room-1")
            instance, original_update = persisted_instance(path)
            instance._ledger_locks = {}
            calls = []
            instance._call = lambda *_args, **_kwargs: calls.append("ack")
            try:
                with self.assertRaisesRegex(ValueError, "no valid terminal evidence"):
                    await instance._complete_event(binding, 5)
            finally:
                adapter.update = original_update
            return calls

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            binding = lifecycle_binding()
            binding.delivery_intents.clear()
            binding.delivery_lifecycle.clear()
            binding.inbox["5"] = "posted"
            binding.terminal_evidence["5"] = {
                "status": "posted", "sourceEventId": "evt-5",
                "canonicalEventId": "posted-6", "reason": "",
            }
            state_store.save(state_store.PluginState(bindings=[binding]), path)
            calls = asyncio.run(exercise(path))
            self.assertEqual(calls, [])
            reloaded = state_store.load(path).binding("room-1")
            self.assertEqual(reloaded.acknowledged_cursor, 4)
            self.assertEqual(reloaded.inbox["5"], "posted")

    def test_malformed_lifecycle_journal_is_rejected_before_restart_io(self):
        cases = (
            ("missing cycle id", lifecycle_binding, lambda record: record["completion"].pop("cycle_id")),
            ("integer cycle id", lifecycle_binding, lambda record: record["completion"].update(cycle_id=7)),
            ("integer attempt id", lifecycle_binding, lambda record: record["completion"].update(attempt_id=7)),
            ("integer canonical event id", lifecycle_binding, lambda record: (
                record["receipt"].update(canonical_event_id=7),
                record["completion"]["payload"].update(eventId=7),
            )),
            ("boolean source sequence", lifecycle_binding, lambda record: record["receipt"].update(source_seq=True)),
            ("receipt sequence zero", lifecycle_binding, lambda record: record["receipt"].update(canonical_seq=0)),
            ("boolean canonical sequence", lifecycle_binding, lambda record: record["receipt"].update(canonical_seq=True)),
            ("integer canonical timestamp", lifecycle_binding, lambda record: record["receipt"].update(canonical_ts=7)),
            ("completion event mismatch", lifecycle_binding, lambda record: record["completion"]["payload"].update(eventId="other")),
            ("source mismatch", lifecycle_binding, lambda record: record["receipt"].update(source_event_id="other")),
            ("attempt budget exceeded", lifecycle_binding, lambda record: record.update(attempts=4)),
            ("foreign binding", lifecycle_binding, lambda record: record["binding"].update(membership_id="other")),
            ("inconsistent state label", lifecycle_binding, lambda record: record.update(state="posted")),
            ("integer turn id", turn_lifecycle_binding, lambda record: record["completion"].update(turn_id=7)),
            ("integer turn idempotency key", turn_lifecycle_binding, lambda record: record["completion"].update(idempotency_key=9)),
            ("boolean turn observed sequence", turn_lifecycle_binding, lambda record: record["completion"].update(observed_seq=True)),
        )
        for name, factory, mutate in cases:
            with self.subTest(case=name), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "state.json"
                binding = factory()
                mutate(binding.delivery_lifecycle["evt-5"])
                state_store.save(state_store.PluginState(bindings=[binding]), path)
                with self.assertRaises(ValueError):
                    state_store.load(path)

    def test_selection_persistence_failure_prevents_post(self):
        async def exercise():
            binding = adapter.RoomBinding(
                "https://room.example/api", "room-1", "member-1", "credential",
                cursor=5, acknowledged_cursor=4,
            )
            instance = configured_instance(binding, None, [])
            instance._cycle_attempts = {}
            instance._persist_binding = lambda _binding: False
            posts = 0

            class API:
                def room_state(self, _room_id):
                    return {"headSeq": 5, "activeEpoch": {"id": "epoch-1", "startsAtSeq": 1}}

                def post_message(self, *_args, **_kwargs):
                    nonlocal posts
                    posts += 1
                    raise AssertionError("post must not run after persistence failure")

            api = API()
            instance._call = lambda _binding, operation: asyncio.sleep(0, result=operation(api))
            result = await instance._send_final(
                "room-1", adapter._dispatch_source_ref("evt-5", "generation-persist"),
                "Frozen answer",
            )
            return result, posts

        result, posts = asyncio.run(exercise())
        self.assertFalse(result.success)
        self.assertEqual(posts, 0)

    def test_canonical_delivery_rejects_malformed_receipts_without_state_change(self):
        cases = {
            "integer event id": {"id": 7, "seq": 6, "ts": "2026-08-17T00:00:00Z"},
            "empty event id": {"id": "", "seq": 6, "ts": "2026-08-17T00:00:00Z"},
            "boolean sequence": {"id": "posted-6", "seq": True, "ts": "2026-08-17T00:00:00Z"},
            "nonpositive sequence": {"id": "posted-6", "seq": 0, "ts": "2026-08-17T00:00:00Z"},
            "integer timestamp": {"id": "posted-6", "seq": 6, "ts": 7},
            "malformed timestamp": {"id": "posted-6", "seq": 6, "ts": "not-a-timestamp"},
            "empty timestamp": {"id": "posted-6", "seq": 6, "ts": ""},
        }
        for name, receipt in cases.items():
            with self.subTest(receipt=name):
                binding = lifecycle_binding()
                binding.delivery_lifecycle.clear()
                before = copy.deepcopy(binding)
                instance = object.__new__(adapter.SyntheticSocialityAdapter)
                instance._persist_binding = lambda _binding: True
                with self.assertRaisesRegex(ValueError, "event ID, sequence, and timestamp"):
                    instance._record_canonical_delivery(
                        binding, "evt-5", receipt,
                        completion={
                            "kind": "cycle", "cycle_id": "cycle-1", "attempt_id": "attempt-1",
                            "payload": {"generation": 3, "action": "contribute", "eventId": "posted-6"},
                        },
                    )
                self.assertEqual(binding, before)

    def test_canonical_receipt_persist_failure_rolls_back_posted_state(self):
        binding = adapter.RoomBinding(
            "https://room.example/api", "room-1", "member-1", "credential",
            identity_version=1, installation_id="installation-1",
        )
        binding.delivery_intents["evt-5"] = {
            "state": "delivery_pending", "delivery_state": "delivery_pending",
            "selected": {"source_seq": 5, "body": "Frozen answer"},
        }
        before = copy.deepcopy(binding.delivery_intents["evt-5"])
        instance = object.__new__(adapter.SyntheticSocialityAdapter)
        instance._persist_binding = lambda _binding: False
        with self.assertRaisesRegex(adapter.ProtocolError, "receipt could not be persisted"):
            instance._record_canonical_delivery(
                binding, "evt-5",
                {"id": "posted-6", "seq": 6, "ts": "2026-08-17T00:00:00Z"},
                completion={"kind": "cycle"},
            )
        self.assertEqual(binding.delivery_intents["evt-5"], before)
        self.assertNotIn("evt-5", binding.delivery_lifecycle)

    def test_canonical_receipt_persist_exception_rolls_back_posted_state(self):
        binding = adapter.RoomBinding(
            "https://room.example/api", "room-1", "member-1", "credential",
            identity_version=1, installation_id="installation-1",
        )
        binding.delivery_intents["evt-5"] = {
            "state": "delivery_pending", "delivery_state": "delivery_pending",
            "selected": {"source_seq": 5, "body": "Frozen answer"},
        }
        before = copy.deepcopy(binding.delivery_intents["evt-5"])
        instance = object.__new__(adapter.SyntheticSocialityAdapter)
        instance._persist_binding = lambda _binding: (_ for _ in ()).throw(OSError("disk full"))
        with self.assertRaisesRegex(OSError, "disk full"):
            instance._record_canonical_delivery(
                binding, "evt-5",
                {"id": "posted-6", "seq": 6, "ts": "2026-08-17T00:00:00Z"},
                completion={"kind": "cycle"},
            )
        self.assertEqual(binding.delivery_intents["evt-5"], before)
        self.assertNotIn("evt-5", binding.delivery_lifecycle)

    def test_canonical_receipt_is_delivery_success_before_cycle_completion(self):
        async def exercise():
            binding = adapter.RoomBinding(
                "https://room.example/api", "room-1", "member-1", "credential",
                cursor=5, acknowledged_cursor=4,
            )
            cycle_attempt = {
                "cycle": {"id": "cycle-1", "generation": 3},
                "attempt": {"id": "attempt-1", "membershipId": "member-1"},
            }
            snapshots = []
            instance = configured_instance(binding, cycle_attempt, snapshots)
            calls = {"post": 0, "complete": 0}

            class API:
                def claim_discussion_attempt(self, *_args):
                    return cycle_attempt

                def room_state(self, _room_id):
                    return {"headSeq": 5, "activeEpoch": {"id": "epoch-1"}}

                def post_message(self, *_args, **_kwargs):
                    calls["post"] += 1
                    return {"id": "posted-6", "seq": 6, "ts": "2026-08-17T00:00:00Z"}

                def complete_discussion_attempt(self, *_args):
                    calls["complete"] += 1
                    journal = binding.delivery_lifecycle["evt-5"]
                    assert journal["delivery_state"] == "posted"
                    assert journal["lifecycle_state"] == "pending"
                    assert journal["receipt"]["canonical_event_id"] == "posted-6"
                    assert journal["completion"]["payload"]["eventId"] == "posted-6"
                    raise adapter.ProtocolError(
                        "temporary lifecycle service failure",
                        code="busy", retryable=True,
                    )

            api = API()
            instance._call = lambda _binding, operation: asyncio.sleep(0, result=operation(api))
            instance._publish = lambda *_args, **_kwargs: asyncio.sleep(0)
            result = await instance._send_final(
                "room-1", adapter._dispatch_source_ref("evt-5", "generation-5"),
                "A coordinated answer.",
            )
            return result, binding, snapshots, calls

        result, binding, snapshots, calls = asyncio.run(exercise())
        intent = binding.delivery_intents["evt-5"]
        self.assertTrue(result.success)
        self.assertEqual(result.message_id, "posted-6")
        self.assertEqual(calls, {"post": 1, "complete": 1})
        self.assertEqual(intent["delivery_state"], "posted")
        self.assertEqual(intent["lifecycle_state"], "pending")
        self.assertEqual(intent["canonical_event"]["id"], "posted-6")
        journal = binding.delivery_lifecycle["evt-5"]
        self.assertEqual(journal["delivery_state"], "posted")
        self.assertEqual(journal["lifecycle_state"], "pending")
        self.assertTrue(journal["automatic_retry"])
        self.assertEqual(journal["last_error_code"], "busy")
        self.assertNotEqual(intent.get("state"), "quarantined")
        self.assertTrue(any(
            snapshot.get("evt-5", {}).get("delivery_state") == "selected"
            and snapshot.get("evt-5", {}).get("lifecycle_state") == "not_started"
            for snapshot in snapshots
        ), "model output must be frozen in the selected state before Room I/O")
        self.assertTrue(any(
            snapshot.get("evt-5", {}).get("delivery_state") == "posted"
            and snapshot.get("evt-5", {}).get("canonical_event", {}).get("id") == "posted-6"
            for snapshot in snapshots
        ), "canonical receipt must be persisted before cycle completion")

    def test_post_receipt_persistence_exception_cannot_escape_as_delivery_failure(self):
        async def exercise():
            binding = adapter.RoomBinding(
                "https://room.example/api", "room-1", "member-1", "credential",
                cursor=5, acknowledged_cursor=4,
            )
            cycle_attempt = {
                "cycle": {"id": "cycle-1", "generation": 3},
                "attempt": {"id": "attempt-1", "membershipId": "member-1"},
            }
            instance = configured_instance(binding, cycle_attempt, [])
            durable_persist = instance._persist_binding

            def fail_after_receipt(current):
                journal = current.delivery_lifecycle.get("evt-5") or {}
                if journal.get("delivery_state") == "posted" and int(journal.get("attempts") or 0) >= 1:
                    raise OSError("disk failed while recording lifecycle debt")
                return durable_persist(current)

            instance._persist_binding = fail_after_receipt
            calls = {"post": 0, "complete": 0}

            class API:
                def claim_discussion_attempt(self, *_args):
                    return cycle_attempt

                def room_state(self, _room_id):
                    return {"headSeq": 5, "activeEpoch": {"id": "epoch-1"}}

                def post_message(self, *_args, **_kwargs):
                    calls["post"] += 1
                    return {"id": "posted-6", "seq": 6, "ts": "2026-08-17T00:00:00Z"}

                def complete_discussion_attempt(self, *_args):
                    calls["complete"] += 1
                    raise AssertionError("lifecycle I/O must not start before its attempt is durable")

            api = API()
            instance._call = lambda _binding, operation: asyncio.sleep(0, result=operation(api))
            instance._publish = lambda *_args, **_kwargs: asyncio.sleep(0)
            result = await instance._send_final(
                "room-1", adapter._dispatch_source_ref("evt-5", "generation-5"),
                "A coordinated answer.",
            )
            return result, binding, calls

        result, binding, calls = asyncio.run(exercise())
        self.assertTrue(result.success)
        self.assertEqual(result.message_id, "posted-6")
        self.assertEqual(calls, {"post": 1, "complete": 0})
        self.assertEqual(binding.delivery_intents["evt-5"]["delivery_state"], "posted")
        self.assertEqual(binding.delivery_intents["evt-5"]["lifecycle_state"], "blocked")

    def test_lifecycle_result_persistence_failure_rolls_back_and_raises(self):
        instance = object.__new__(adapter.SyntheticSocialityAdapter)
        binding = lifecycle_binding()
        binding.cycle_attempt_owners = {"cycle-owner": "evt-5"}
        before = copy.deepcopy(binding)
        instance._persist_binding = lambda _binding: False

        with self.assertRaises(adapter.ProtocolError):
            instance._record_lifecycle_complete(binding, "evt-5")
        self.assertEqual(binding.delivery_intents, before.delivery_intents)
        self.assertEqual(binding.delivery_lifecycle, before.delivery_lifecycle)
        self.assertEqual(binding.cycle_attempt_owners, before.cycle_attempt_owners)

        retryable = adapter.ProtocolError("busy", code="busy", retryable=True)
        with self.assertRaises(adapter.ProtocolError):
            instance._record_lifecycle_pending(binding, "evt-5", retryable, error_code="busy")
        self.assertEqual(binding.delivery_intents, before.delivery_intents)
        self.assertEqual(binding.delivery_lifecycle, before.delivery_lifecycle)
        self.assertEqual(binding.cycle_attempt_owners, before.cycle_attempt_owners)

    def test_restart_with_canonical_receipt_preserves_journal_budget_and_success(self):
        async def exercise():
            binding = adapter.RoomBinding(
                "https://room.example/api", "room-1", "member-1", "credential",
                cursor=5, acknowledged_cursor=4,
            )
            cycle_attempt = {
                "cycle": {"id": "cycle-1", "generation": 3},
                "attempt": {"id": "attempt-1", "membershipId": "member-1"},
            }
            snapshots = []
            instance = configured_instance(binding, cycle_attempt, snapshots)
            selected = {
                "action": "post", "body": "Frozen answer", "responds_to": "evt-human",
                "recipient_membership_ids": [], "coordination_mode": "open",
                "observed_seq": 5, "source_seq": 5, "observed_epoch_id": "epoch-1",
                "cycle": {"cycle_id": "cycle-1", "attempt_id": "attempt-1", "generation": 3},
                "binding": instance._intent_binding(binding),
            }
            binding.delivery_intents["evt-5"] = {
                "selected": selected,
                "post": {
                    "coordination_mode": "open", "turn_id": "", "observed_seq": 5,
                    "observed_epoch_id": "epoch-1", "body": "Frozen answer",
                    "responds_to": "evt-human", "recipient_membership_ids": [],
                    "cycle": selected["cycle"], "idempotency_key": "message-key",
                    "binding": instance._intent_binding(binding),
                },
                "delivery_state": "posted",
                "canonical_event": {"id": "posted-6", "seq": 6, "ts": "2026-08-17T00:00:00Z"},
                "lifecycle_state": "pending",
                "state": "lifecycle_pending",
            }
            binding.delivery_lifecycle["evt-5"] = {
                "state": "lifecycle_pending", "delivery_state": "posted", "lifecycle_state": "pending",
                "receipt": {
                    "source_event_id": "evt-5", "source_seq": 5,
                    "canonical_event_id": "posted-6", "canonical_seq": 6,
                    "canonical_ts": "2026-08-17T00:00:00Z",
                },
                "completion": {
                    "kind": "cycle", "cycle_id": "cycle-1", "attempt_id": "attempt-1",
                    "payload": {"generation": 3, "action": "contribute", "eventId": "posted-6"},
                },
                "attempts": 2, "automatic_retry": True,
                "binding": instance._intent_binding(binding),
            }
            calls = {"post": 0, "complete": 0}

            class API:
                def room_policy(self, _room_id):
                    return {"coordinationMode": "open"}

                def post_message(self, *_args, **_kwargs):
                    calls["post"] += 1
                    raise AssertionError("a canonical receipt must prevent reposting")

                def complete_discussion_attempt(self, _room_id, cycle_id, attempt_id, payload):
                    calls["complete"] += 1
                    if (cycle_id, attempt_id, payload) != (
                        "cycle-1", "attempt-1",
                        {"generation": 3, "action": "contribute", "eventId": "posted-6"},
                    ):
                        raise AssertionError("persisted lifecycle request changed")
                    raise adapter.ProtocolError("busy", code="busy", retryable=True)

            api = API()
            instance._call = lambda _binding, operation: asyncio.sleep(0, result=operation(api))
            instance._publish = lambda *_args, **_kwargs: asyncio.sleep(0)
            result = await instance._send_final(
                "room-1", adapter._dispatch_source_ref("evt-5", "generation-6"),
                "Regenerated text must be ignored.",
            )
            return result, binding, calls

        result, binding, calls = asyncio.run(exercise())
        self.assertTrue(result.success)
        self.assertEqual(result.message_id, "posted-6")
        self.assertEqual(calls, {"post": 0, "complete": 1})
        intent = binding.delivery_intents["evt-5"]
        self.assertEqual(intent["delivery_state"], "posted")
        self.assertEqual(intent["lifecycle_state"], "blocked")
        self.assertEqual(intent["state"], "lifecycle_blocked")
        journal = binding.delivery_lifecycle["evt-5"]
        self.assertEqual(journal["attempts"], 3)
        self.assertEqual(journal["receipt"]["canonical_event_id"], "posted-6")

    def test_retryable_post_failure_before_receipt_remains_delivery_pending(self):
        async def exercise():
            binding = adapter.RoomBinding(
                "https://room.example/api", "room-1", "member-1", "credential",
                cursor=5, acknowledged_cursor=4,
            )
            snapshots = []
            instance = configured_instance(binding, None, snapshots)
            instance._cycle_attempts = {}

            class API:
                def room_state(self, _room_id):
                    return {"headSeq": 5, "activeEpoch": {"id": "epoch-1"}}

                def post_message(self, *_args, **_kwargs):
                    raise adapter.ProtocolError("temporarily unavailable", code="busy", retryable=True)

            api = API()
            instance._call = lambda _binding, operation: asyncio.sleep(0, result=operation(api))
            instance._publish = lambda *_args, **_kwargs: asyncio.sleep(0)
            result = await instance._send_final(
                "room-1", adapter._dispatch_source_ref("evt-5", "generation-7"),
                "A frozen answer.",
            )
            return result, binding

        result, binding = asyncio.run(exercise())
        self.assertFalse(result.success)
        self.assertTrue(result.retryable)
        intent = binding.delivery_intents["evt-5"]
        self.assertEqual(intent["delivery_state"], "delivery_pending")
        self.assertEqual(intent["lifecycle_state"], "not_started")
        self.assertNotIn("canonical_event", intent)
        self.assertEqual(binding.inbox["5"], "failed-retryable")

    def test_nonretryable_post_failure_is_quarantined_and_cannot_repost(self):
        async def exercise():
            binding = adapter.RoomBinding(
                "https://room.example/api", "room-1", "member-1", "credential",
                cursor=5, acknowledged_cursor=4,
            )
            instance = configured_instance(binding, None, [])
            instance._cycle_attempts = {}
            posts = 0

            class API:
                def room_state(self, _room_id):
                    return {"headSeq": 5, "activeEpoch": {"id": "epoch-1"}}

                def post_message(self, *_args, **_kwargs):
                    nonlocal posts
                    posts += 1
                    raise adapter.ProtocolError(
                        "invalid canonical payload", code="validation_error", retryable=False,
                    )

            api = API()
            instance._call = lambda _binding, operation: asyncio.sleep(0, result=operation(api))
            instance._publish = lambda *_args, **_kwargs: asyncio.sleep(0)
            source_ref = adapter._dispatch_source_ref("evt-5", "generation-quarantine")
            first = await instance._send_final("room-1", source_ref, "Frozen answer")
            second = await instance._send_final("room-1", source_ref, "Frozen answer")
            return first, second, binding, posts

        first, second, binding, posts = asyncio.run(exercise())
        self.assertFalse(first.success)
        self.assertFalse(second.success)
        self.assertEqual(posts, 1)
        intent = binding.delivery_intents["evt-5"]
        self.assertEqual(intent["delivery_state"], "quarantined")
        self.assertEqual(intent["lifecycle_state"], "not_started")
        self.assertNotIn("canonical_event", intent)

    def test_restart_drains_lifecycle_journal_without_model_or_post(self):
        async def exercise():
            binding = adapter.RoomBinding(
                "https://room.example/api", "room-1", "member-1", "credential",
                cursor=6, acknowledged_cursor=5,
            )
            binding.terminal_evidence["5"] = {
                "status": "posted", "sourceEventId": "evt-5",
                "canonicalEventId": "posted-6", "reason": "",
            }
            binding.delivery_lifecycle["evt-5"] = {
                "state": "lifecycle_pending",
                "delivery_state": "posted",
                "lifecycle_state": "pending",
                "receipt": {
                    "source_event_id": "evt-5", "source_seq": 5,
                    "canonical_event_id": "posted-6", "canonical_seq": 6,
                },
                "completion": {
                    "kind": "cycle", "cycle_id": "cycle-1", "attempt_id": "attempt-1",
                    "payload": {"generation": 3, "action": "contribute", "eventId": "posted-6"},
                },
            }
            instance = object.__new__(adapter.SyntheticSocialityAdapter)
            instance._persist_binding = lambda _binding: True
            calls = []

            class API:
                def complete_discussion_attempt(self, room_id, cycle_id, attempt_id, payload):
                    calls.append((room_id, cycle_id, attempt_id, copy.deepcopy(payload)))
                    return {"state": "completed"}

                def post_message(self, *_args, **_kwargs):
                    raise AssertionError("lifecycle repair must never post")

            api = API()
            instance._call = lambda _binding, operation: asyncio.sleep(0, result=operation(api))
            instance.handle_message = lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("lifecycle repair must never invoke the model")
            )
            await instance._repair_pending_lifecycles(binding)
            await instance._repair_pending_lifecycles(binding)
            return binding, calls

        binding, calls = asyncio.run(exercise())
        self.assertEqual(calls, [(
            "room-1", "cycle-1", "attempt-1",
            {"generation": 3, "action": "contribute", "eventId": "posted-6"},
        )])
        self.assertEqual(binding.delivery_lifecycle, {})
        self.assertEqual(binding.terminal_evidence["5"]["status"], "posted")

    def test_posted_receipt_closes_gap_and_acknowledges_contiguous_tail(self):
        async def exercise():
            tail = {
                str(seq): {
                    "status": "ignored", "sourceEventId": f"event-{seq}",
                    "canonicalEventId": "", "reason": "self_event",
                }
                for seq in range(121, 142)
            }
            binding = adapter.RoomBinding(
                "https://room.example/api", "room-berlin", "member-berlin", "credential",
                cursor=141, acknowledged_cursor=119,
                inbox={"120": "pending", **{str(seq): "ignored" for seq in range(121, 142)}},
                terminal_evidence=tail,
            )
            binding.delivery_lifecycle["event-120"] = {
                "state": "lifecycle_pending", "delivery_state": "posted", "lifecycle_state": "pending",
                "receipt": {
                    "source_event_id": "event-120", "source_seq": 120,
                    "canonical_event_id": "posted-120", "canonical_seq": 142,
                    "canonical_ts": "2026-08-17T00:00:00Z",
                },
                "completion": {"kind": "cycle"},
            }
            instance = object.__new__(adapter.SyntheticSocialityAdapter)
            instance._persist_binding = lambda _binding: True
            acknowledgements = []

            class API:
                def acknowledge(self, room_id, seq):
                    acknowledgements.append((room_id, seq))
                    return {"acknowledgedSeq": seq}

            api = API()
            instance._call = lambda _binding, operation: asyncio.sleep(0, result=operation(api))
            await instance._complete_event(
                binding, 120, terminal_status="posted", source_id="event-120",
                canonical_event_id="posted-120",
            )
            return binding, acknowledgements

        binding, acknowledgements = asyncio.run(exercise())
        self.assertEqual(acknowledgements, [("room-berlin", 141)])
        self.assertEqual(binding.acknowledged_cursor, 141)
        self.assertEqual(binding.cursor, 141)
        self.assertEqual(binding.inbox, {})
        self.assertEqual(binding.terminal_evidence, {})
        self.assertIn("event-120", binding.delivery_lifecycle)

    def test_posted_receipt_gap_rejects_ack_beyond_locally_proven_frontier(self):
        async def exercise():
            binding = adapter.RoomBinding(
                "https://room.example/api", "room-1", "member-1", "credential",
                cursor=7, acknowledged_cursor=4,
                inbox={"5": "pending", "7": "ignored"},
                terminal_evidence={
                    "7": {"status": "ignored", "sourceEventId": "event-7", "canonicalEventId": "", "reason": "self_event"},
                },
            )
            binding.delivery_lifecycle["event-5"] = {
                "state": "lifecycle_pending", "delivery_state": "posted", "lifecycle_state": "pending",
                "receipt": {
                    "source_event_id": "event-5", "source_seq": 5,
                    "canonical_event_id": "posted-6", "canonical_seq": 6,
                    "canonical_ts": "2026-08-17T00:00:00Z",
                },
                "completion": {"kind": "cycle"},
            }
            instance = object.__new__(adapter.SyntheticSocialityAdapter)
            instance._persist_binding = lambda _binding: True

            class API:
                def acknowledge(self, _room_id, seq):
                    self.requested = seq
                    return {"acknowledgedSeq": 99}

            api = API()
            instance._call = lambda _binding, operation: asyncio.sleep(0, result=operation(api))
            with self.assertRaisesRegex(adapter.ProtocolError, "locally proven contiguous frontier"):
                await instance._complete_event(
                    binding, 5, terminal_status="posted", source_id="event-5",
                    canonical_event_id="posted-6",
                )
            return binding, api.requested

        binding, requested = asyncio.run(exercise())
        self.assertEqual(requested, 5)
        self.assertEqual(binding.acknowledged_cursor, 4)
        self.assertIn("5", binding.terminal_evidence)
        self.assertIn("7", binding.terminal_evidence)
        self.assertNotIn("6", binding.terminal_evidence)

    def test_legacy_complete_receipt_promotes_without_repost_and_runs_lifecycle_only(self):
        async def exercise():
            binding = adapter.RoomBinding(
                "https://room.example/api", "room-1", "member-1", "credential",
                identity_version=1, installation_id="installation-1",
                cursor=5, acknowledged_cursor=4, inbox={"5": "quarantined"},
            )
            generation = adapter.SyntheticSocialityAdapter._intent_binding(binding)
            selected = {
                "action": "post", "source_event_id": "evt-5", "source_seq": 5,
                "body": "Frozen answer", "binding": generation,
                "cycle": {"cycle_id": "cycle-1", "attempt_id": "attempt-1", "generation": 3},
            }
            binding.delivery_intents["evt-5"] = {
                "state": "quarantined", "selected": selected,
                "post": {
                    "body": "Frozen answer", "coordination_mode": "coordinated",
                    "observed_seq": 5, "binding": generation,
                    "cycle": selected["cycle"], "idempotency_key": "legacy-key",
                },
                "canonical_event": {
                    "id": "posted-6", "seq": 6, "ts": "2026-08-17T00:00:00Z",
                },
            }
            activated = adapter.SyntheticSocialityAdapter._activate_legacy_canonical_receipt(
                binding, "evt-5", 5,
            )
            calls = {"post": 0, "complete": 0}
            instance = configured_instance(binding, None, [])

            class API:
                def room_policy(self, _room_id):
                    return {"coordinationMode": "coordinated"}

                def post_message(self, *_args, **_kwargs):
                    calls["post"] += 1
                    raise AssertionError("complete canonical receipt must prevent repost")

                def complete_discussion_attempt(self, room_id, cycle_id, attempt_id, payload):
                    calls["complete"] += 1
                    return {"state": "completed"}

            api = API()
            instance._call = lambda _binding, operation: asyncio.sleep(0, result=operation(api))
            instance._publish = lambda *_args, **_kwargs: asyncio.sleep(0)
            result = await instance._send_final(
                "room-1", adapter._dispatch_source_ref("evt-5", "generation-receipt"),
                "Regenerated text must be ignored.",
            )
            return activated, result, binding, calls

        activated, result, binding, calls = asyncio.run(exercise())
        self.assertTrue(activated)
        self.assertTrue(result.success)
        self.assertEqual(result.message_id, "posted-6")
        self.assertEqual(calls, {"post": 0, "complete": 1})
        intent = binding.delivery_intents["evt-5"]
        self.assertEqual(intent["delivery_state"], "posted")
        self.assertEqual(intent["lifecycle_state"], "complete")
        self.assertEqual(intent["migration"], "legacy-complete-canonical-receipt")

    def test_legacy_complete_receipt_near_misses_remain_fail_closed(self):
        binding = adapter.RoomBinding(
            "https://room.example/api", "room-1", "member-1", "credential",
            identity_version=1, installation_id="installation-1",
            cursor=5, acknowledged_cursor=4, inbox={"5": "quarantined"},
        )
        generation = adapter.SyntheticSocialityAdapter._intent_binding(binding)
        selected = {
            "action": "post", "source_event_id": "evt-5", "source_seq": 5,
            "body": "Frozen answer", "binding": generation,
        }
        binding.delivery_intents["evt-5"] = {
            "state": "quarantined", "selected": selected,
            "post": {"body": "Frozen answer", "binding": generation},
            "canonical_event": {"id": "posted-6", "seq": 6, "ts": "2026-08-17T00:00:00Z"},
        }
        cases = {
            "missing event id": lambda current: current.delivery_intents["evt-5"]["canonical_event"].pop("id"),
            "nonpositive event sequence": lambda current: current.delivery_intents["evt-5"]["canonical_event"].update(seq=0),
            "boolean event sequence": lambda current: current.delivery_intents["evt-5"]["canonical_event"].update(seq=True),
            "missing timestamp": lambda current: current.delivery_intents["evt-5"]["canonical_event"].pop("ts"),
            "foreign source": lambda current: current.delivery_intents["evt-5"]["selected"].update(source_event_id="other"),
            "foreign binding": lambda current: current.delivery_intents["evt-5"]["post"].update(binding={"other": True}),
            "body mismatch": lambda current: current.delivery_intents["evt-5"]["post"].update(body="Different"),
        }
        for name, mutate in cases.items():
            with self.subTest(near_miss=name):
                current = copy.deepcopy(binding)
                mutate(current)
                before = copy.deepcopy(current)
                self.assertFalse(adapter.SyntheticSocialityAdapter._activate_legacy_canonical_receipt(
                    current, "evt-5", 5,
                ))
                self.assertEqual(current, before)

    def test_redacted_actual_berlin_state_fails_closed_without_exact_observed_sequence(self):
        fixture_path = ROOT / "tests" / "fixtures" / "berlin-seq120-redacted-offline.json"
        fixture = json.loads(fixture_path.read_text())
        proof = fixture["proof"]
        self.assertEqual(
            [name for name, value in proof.items() if value is False],
            ["post_observed_matches"],
        )
        self.assertEqual(proof["candidate_count"], 1)

        binding = state_store.RoomBinding.from_dict(fixture["binding"])
        before = copy.deepcopy(binding)
        receipt_activated = adapter.SyntheticSocialityAdapter._activate_legacy_canonical_receipt(
            binding, "event-120-redacted", 120,
        )
        replay_activated = adapter.SyntheticSocialityAdapter._activate_legacy_post_commit_recovery(
            binding, "event-120-redacted", 120,
        )

        self.assertFalse(receipt_activated)
        self.assertFalse(replay_activated)
        self.assertEqual(binding, before)
        intent = binding.delivery_intents["event-120-redacted"]
        self.assertEqual(binding.inbox["120"], "quarantined")
        self.assertEqual(binding.pending_retries["120"], 2)
        self.assertEqual(binding.acknowledged_cursor, 119)
        self.assertEqual(intent["state"], "quarantined")
        self.assertNotIn("delivery_state", intent)
        self.assertNotIn("migration", intent)

    def test_exact_legacy_berlin_signature_reopens_only_frozen_idempotent_post(self):
        binding = adapter.RoomBinding(
            "https://room.example/api", "room-berlin", "member-berlin", "credential",
            cursor=141, acknowledged_cursor=119,
        )
        generation = adapter.SyntheticSocialityAdapter._intent_binding(binding)
        binding.inbox = {"120": "quarantined", "121": "ignored", "122": "ignored"}
        binding.pending_retries = {"120": 2}
        binding.terminal_evidence = {
            "121": {"status": "ignored", "sourceEventId": "self-121"},
            "122": {"status": "ignored", "sourceEventId": "self-122"},
        }
        message_key = adapter.stable_key(
            "message", "event-120", room_id=binding.room_id, membership_id=binding.membership_id,
        )
        cycle = {"cycle_id": "cycle-1", "attempt_id": "attempt-1", "generation": 3}
        owner_key = adapter._cycle_attempt_owner_key(
            {"cycle": {"id": "cycle-1", "generation": 3}, "attempt": {"id": "attempt-1"}},
            binding.membership_id,
        )
        binding.cycle_attempt_owners = {owner_key: "event-120"}
        binding.delivery_intents["event-120"] = {
            "state": "quarantined",
            "last_error": "discussion cycle input conflicts with persisted cycle",
            "last_error_code": "cycle_conflict",
            "failed_at": "2026-08-14T18:14:27Z",
            "selected": {
                "action": "post", "source_event_id": "event-120", "source_seq": 120,
                "observed_seq": 120, "body": "Frozen Berlin answer",
                "message_idempotency_key": message_key, "cycle": cycle,
                "binding": generation,
            },
            "post": {
                "body": "Frozen Berlin answer", "observed_seq": 120,
                "idempotency_key": message_key, "cycle": cycle, "binding": generation,
            },
        }

        legacy = copy.deepcopy(binding)
        activated = adapter.SyntheticSocialityAdapter._activate_legacy_post_commit_recovery(
            binding, "event-120", 120,
        )
        self.assertTrue(activated)
        intent = binding.delivery_intents["event-120"]
        self.assertEqual(binding.inbox["120"], "pending")
        self.assertEqual(binding.pending_retries["120"], 0)
        self.assertEqual(binding.acknowledged_cursor, 119)
        self.assertNotIn("120", binding.terminal_evidence)
        self.assertEqual(intent["delivery_state"], "delivery_pending")
        self.assertEqual(intent["lifecycle_state"], "not_started")
        self.assertEqual(intent["post"]["idempotency_key"], message_key)
        self.assertEqual(intent["legacy_post_commit_error"]["code"], "cycle_conflict")

        near_misses = {
            "different error": lambda current: current.delivery_intents["event-120"].update(last_error_code="busy"),
            "missing selected binding": lambda current: current.delivery_intents["event-120"]["selected"].pop("binding"),
            "different body": lambda current: current.delivery_intents["event-120"]["post"].update(body="Different"),
            "different idempotency key": lambda current: current.delivery_intents["event-120"]["post"].update(idempotency_key="wrong"),
            "different cycle": lambda current: current.delivery_intents["event-120"]["post"]["cycle"].update(cycle_id="other"),
            "unrelated owner": lambda current: current.cycle_attempt_owners.update({owner_key: "other-source"}),
            "already migrated": lambda current: current.delivery_intents["event-120"].update(delivery_state="delivery_pending"),
        }
        for name, mutate in near_misses.items():
            with self.subTest(near_miss=name):
                unrelated = copy.deepcopy(legacy)
                mutate(unrelated)
                self.assertFalse(adapter.SyntheticSocialityAdapter._activate_legacy_post_commit_recovery(
                    unrelated, "event-120", 120,
                ))
                self.assertEqual(unrelated.inbox["120"], "quarantined")


if __name__ == "__main__":
    unittest.main()
