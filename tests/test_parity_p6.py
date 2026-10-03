"""Parity release P6: clean start and bounds (contract section P6, P5-2).

The shared cases carry the names listed in ``conformance.json``
``parityP6Cases`` and ``parityP5Cases``; OpenClaw implements the same names.
Owner, ISSUE-015/016: "/start-discussion should also reset the tokens in the
room". No test uses a real credential or talks to a live Room.
"""
from __future__ import annotations

import asyncio
import contextlib
import hashlib
import io
import json
import logging
import tempfile
import time
import types
import unittest
from pathlib import Path
from unittest import mock

from test_delivery_lifecycle import ROOT, adapter, lifecycle_binding, persisted_instance, state_store
from test_parity_l1 import LinkTestCase, link
from test_parity_p1a import HUMAN, PEER, START, attempt_ready, build_message
from test_parity_p5 import ARTIFACTS, BRIEF_TEXT, GAP, PEER_TEXT, SOURCE, artifact, claimed, contribution, manifest

CONFORMANCE = json.loads((ROOT / "conformance.json").read_text())
context_module = __import__(adapter.__name__.rsplit(".", 1)[0] + ".context", fromlist=["context"])

OLD_TEXT = "Earlier discussion's document: the rejected plan."
# Events of the discussion before the active one (epoch-2 starts at seq 41).
OLD_MESSAGE = {"id": "m-30", "seq": 30, "type": "message.posted", "actorRole": "human_owner", "actorId": "owner-1",
               "payload": {"body": "Earlier discussion: the rejected plan.", "attachments": [manifest("old")]}}
OLD_ARTIFACTS = {**ARTIFACTS, "old": artifact("old", OLD_TEXT)}


def keep_persisting(calls):
    def prepare(instance, binding):
        instance._persist_binding = lambda current: calls.append(current) or True
    return prepare


def threaded_calls(instance):
    """Run each Room read in a worker thread, as the production ``_call`` does."""
    inline = instance._call

    async def call(binding, operation):
        pending = await asyncio.to_thread(inline, binding, operation)
        return await pending

    instance._call = call


class ParityP6Tests(unittest.TestCase):
    def test_p6_01_a_new_discussion_runs_in_a_new_host_session(self):
        # Every block that holds Room events holds only events at or above the
        # epoch start: transcript, current item, missed source, kept reply,
        # cycle source and documents.
        calls = []

        def prepare(instance, binding):
            keep_persisting(calls)(instance, binding)
            binding.missed_sources = [{"source_event_id": "m-30", "source_seq": 30}]
            binding.kept_reply = {"source_event_id": "m-30", "source_seq": 30, "epoch_id": "epoch-1",
                                  "produced_at": "2026-10-03T20:00:00Z", "reason": "superseded",
                                  "text": "Kept reply from the earlier discussion."}

        context = (OLD_MESSAGE, START, HUMAN, PEER, *GAP[8:20])
        for event, attempt in (
            (attempt_ready(70, 1, source="m-30"), claimed("m-30")),
            (attempt_ready(70, 1, source="m-43"), claimed("m-30")),
            ({**HUMAN, "id": "m-69", "seq": 69}, None),
        ):
            with self.subTest(event=event["id"]):
                message = build_message(event, attempt=attempt, context=context, artifacts=OLD_ARTIFACTS,
                                        prepare=prepare)
                for earlier in ("rejected plan", OLD_TEXT, "Kept reply from the earlier discussion", "#30"):
                    self.assertNotIn(earlier, message.text)
                self.assertIn("Disclosure first, then support.", message.text)
                self.assertEqual(message.source.thread_id, adapter._epoch_thread_id("epoch-2"))
        # A new discussion: a new host session; the earlier one is not reused.
        nxt = build_message(attempt_ready(70, 1, source="m-43"), attempt=claimed("m-43"), context=context,
                            epoch_id="epoch-3")
        self.assertEqual(nxt.source.thread_id, adapter._epoch_thread_id("epoch-3"))
        self.assertNotEqual(nxt.source.thread_id, adapter._epoch_thread_id("epoch-2"))

    def test_p6_02_an_epoch_without_a_start_gives_no_earlier_events(self):
        for starts_at in (0, None, -3):
            with self.subTest(starts_at=starts_at):
                calls = []
                # An open entry and one the Room named and that was offered (L1-13).
                missed = [{"source_event_id": "m-42", "source_seq": 42},
                          {"source_event_id": "m-40", "source_seq": 0, "offered": True}]
                kept = {"source_event_id": "m-42", "source_seq": 42, "epoch_id": "epoch-2",
                        "produced_at": "2026-10-03T20:00:00Z", "reason": "superseded", "text": "Kept text."}

                def prepare(instance, binding):
                    keep_persisting(calls)(instance, binding)
                    binding.missed_sources = [dict(item) for item in missed]
                    binding.kept_reply = dict(kept)
                    prepared.append(binding)

                prepared = []
                reads = []
                with self.assertLogs(adapter.logger, logging.WARNING) as logs, \
                        mock.patch.object(adapter, "_cycle_source_message",
                                          side_effect=lambda *a, **k: reads.append(a) or None):
                    message = build_message(attempt_ready(70, 1, source="m-43"), attempt=claimed("m-src"),
                                            context=(OLD_MESSAGE, START, SOURCE, HUMAN, PEER), starts_at=starts_at,
                                            artifacts=OLD_ARTIFACTS, prepare=prepare)
                for earlier in ("rejected plan", "Should the doctor disclose", "Disclosure first", "Kept text.",
                                BRIEF_TEXT, OLD_TEXT, "Missed earlier"):
                    self.assertNotIn(earlier, message.text)
                self.assertEqual(reads, [], "no scan without an epoch start")
                self.assertEqual(sum("code epoch_start_unknown" in line for line in logs.output), 1, logs.output)
                # Neither offered nor cleared.
                self.assertEqual(prepared[0].missed_sources, missed)
                self.assertEqual(prepared[0].kept_reply, kept)
                self.assertEqual(calls, [])
        # The pure window functions read nothing without a start.
        pages = []
        for floor in (0, -1, None):
            self.assertEqual(context_module.recent_room_messages(
                lambda after: pages.append(after) or {"events": [OLD_MESSAGE]}, before_seq=50,
                active_epoch_starts_at=floor), [])
            self.assertIsNone(context_module.cycle_source_message(
                lambda after: pages.append(after) or {"events": [OLD_MESSAGE]}, "m-30", before_seq=50,
                active_epoch_starts_at=floor))
        self.assertEqual(pages, [])

    def test_p6_03_rotate_current_epoch_session(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            binding = lifecycle_binding()
            binding.epoch_session_routing_initialized = True
            binding.legacy_session_epoch_id = "current"
            state_store.save(state_store.PluginState(bindings=[binding]), path)
            before = path.read_bytes()
            room_cli = adapter.cli
            original_load, original_update = room_cli.load, room_cli.update
            room_cli.load = lambda: state_store.load(path)
            room_cli.update = lambda mutator: state_store.update(mutator, path)
            try:
                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    self.assertEqual(room_cli._rotate_current_epoch_session("room-1", confirmed=False), 0)
                self.assertIn("dry run", output.getvalue())
                self.assertIn("legacy Room session", output.getvalue())
                self.assertEqual(path.read_bytes(), before, "a dry run changes nothing")
                with contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(room_cli._rotate_current_epoch_session("room-1", confirmed=True), 0)
                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    room_cli._rotate_current_epoch_session("room-1", confirmed=False)
                self.assertIn("already authorized", output.getvalue())
                with self.assertRaisesRegex(ValueError, "not configured"):
                    room_cli._rotate_current_epoch_session("room-other", confirmed=False)
            finally:
                room_cli.load, room_cli.update = original_load, original_update
            authorized = state_store.load(path).binding("room-1")
            self.assertEqual((authorized.epoch_session_routing_initialized, authorized.legacy_session_epoch_id,
                              authorized.rotate_current_epoch_session), (False, "", True))
            # Consumed once at the next start: the current epoch gets an epoch session.
            restarted, original_adapter_update = persisted_instance(path)
            restarted._call = lambda _binding, operation: asyncio.sleep(
                0, result=operation(types.SimpleNamespace(room_state=lambda _room_id: {"activeEpoch": {"id": "current"}})),
            )
            try:
                asyncio.run(restarted._ensure_epoch_session_routing(authorized))
            finally:
                adapter.update = original_adapter_update
            consumed = state_store.load(path).binding("room-1")
            self.assertFalse(consumed.rotate_current_epoch_session)
            self.assertEqual(adapter._session_thread_for_epoch(consumed, "current"), adapter._epoch_thread_id("current"))

    def test_p6_04_guidance_bound(self):
        vectors = CONFORMANCE["parityP6GuidanceVector"]
        self.assertEqual([vector["name"] for vector in vectors], ["cut", "drop", "fit", "one"])
        for vector in vectors:
            with self.subTest(vector=vector["name"]):
                rendered = "\n".join(context_module.bounded_guidance(vector["rules"]))
                self.assertEqual(len(rendered), vector["codePoints"])
                self.assertLessEqual(len(rendered), context_module.GUIDANCE_CHARACTER_LIMIT)
                self.assertEqual(hashlib.sha256(rendered.encode("utf-8")).hexdigest(), vector["sha256"])
                # The prompt carries exactly that text.
                rules = [{"enforcement": "guidance", "text": text} for text in vector["rules"]]
                text = build_message({**HUMAN, "id": "m-59", "seq": 59}, rules=rules).text
                self.assertIn(rendered + "\n", text)
        self.assertEqual(context_module.GUIDANCE_CHARACTER_LIMIT, 3_000)
        # A private briefing of about 1,700 characters passes whole.
        briefing = "Briefing. " * 170
        text = build_message({**HUMAN, "id": "m-59", "seq": 59},
                             rules=[{"enforcement": "guidance", "text": briefing}]).text
        self.assertIn("- " + briefing.strip() + "\n", text)

    def test_case_names_resolve_to_tests(self):
        cases = CONFORMANCE["parityP6Cases"]
        self.assertEqual(len(cases), 4)
        sources = "\n".join(path.read_text() for path in (ROOT / "tests").glob("test_*.py"))
        for index, (name, method) in enumerate(cases.items(), start=1):
            self.assertTrue(name.startswith(f"P6-{index:02d} "), name)
            self.assertEqual(sources.count(f"def {method}(self"), 1, method)
        self.assertEqual(CONFORMANCE["parityP6Contract"],
                         "docs/delivery-lifecycle-contract-v1.md#p6--clean-start-and-bounds-2026-10-04")


def debate_turn(instance, binding, ready, attempt):
    """Run one more turn on a kept instance and return its message."""
    captured = []
    instance._cycle_attempts = {ready["id"]: attempt}
    instance._event_epoch[ready["id"]] = "epoch-2"
    instance.handle_message = lambda message: asyncio.sleep(0, result=captured.append(message))
    assert asyncio.run(instance._dispatch_room_event(binding, ready, "generation-x"))
    return captured[0]


def counted_scans(calls, effect=None):
    real = adapter._cycle_source_message

    def counting(*args, **kwargs):
        if kwargs.get("types") == adapter.CYCLE_DOCUMENT_SOURCE_TYPES:
            calls.append(args[1])
            if effect is not None:
                return effect(*args, **kwargs)
        return real(*args, **kwargs)

    return mock.patch.object(adapter, "_cycle_source_message", side_effect=counting)


class ParityP5BoundTests(unittest.TestCase):
    def test_p5_02_a_slow_or_unresolvable_cycle_source_costs_one_bounded_lookup(self):
        # Unresolvable: one lookup for the cycle, then remembered.
        events = [START, *GAP, *[contribution(seq) for seq in range(200, 230)]]
        kept, scans = [], []
        first = attempt_ready(230, 13, source="m-229")
        with counted_scans(scans), self.assertLogs(adapter.logger, logging.WARNING) as logs:
            message = build_message(first, attempt=claimed("m-gone"), context=[*events, first], artifacts=ARTIFACTS,
                                    keep_instance=kept)
            instance, binding = kept[0]
            second = debate_turn(instance, binding, attempt_ready(231, 14, source="m-229"), claimed("m-gone"))
        self.assertEqual(scans, ["m-gone"])
        self.assertIn("Contribution 229.", message.text)
        self.assertIn("Contribution 229.", second.text)
        self.assertEqual(sum("code cycle_source_unresolved" in line for line in logs.output), 1, logs.output)

        # Slow: the turn waits at most the bound, then proceeds; the cycle is
        # not looked up again.
        def slow(*args, **kwargs):
            time.sleep(1.5)
            return None

        kept, scans, handed = [], [], []

        def prepare(instance, _binding):
            threaded_calls(instance)
            original = instance.handle_message
            instance.handle_message = lambda message: handed.append(time.monotonic()) or original(message)

        started = time.monotonic()
        with counted_scans(scans, slow), mock.patch.object(adapter, "CYCLE_DOCUMENT_SOURCE_TIMEOUT_SECONDS", 0.2), \
                self.assertLogs(adapter.logger, logging.WARNING) as logs:
            message = build_message(first, attempt=claimed("m-src"), context=[START, SOURCE, *events[1:], first],
                                    artifacts=ARTIFACTS, keep_instance=kept, prepare=prepare)
        self.assertLess(handed[0] - started, 1.2, "the turn is never delayed beyond the bound")
        self.assertNotIn(BRIEF_TEXT, message.text)
        self.assertTrue(any("code cycle_source_unresolved" in line and "TimeoutError" in line for line in logs.output),
                        logs.output)
        instance, binding = kept[0]
        with counted_scans(scans, slow):
            debate_turn(instance, binding, attempt_ready(231, 14, source="m-229"), claimed("m-src"))
        self.assertEqual(scans, ["m-src"])
        self.assertEqual(adapter.CYCLE_DOCUMENT_SOURCE_TIMEOUT_SECONDS, 10)

    def test_cycle_source_cache_is_keyed_per_cycle_and_source(self):
        # OP-12 P5 review: "cache key reduced to the Room" survived. Two cycles
        # of one Room, each with its own source outside the loaded context.
        other = {**SOURCE, "id": "m-src2", "seq": 43, "payload": {"body": "Second brief.",
                                                                   "attachments": [manifest("peer")]}}
        events = [START, SOURCE, other, *GAP[1:], *[contribution(seq) for seq in range(200, 230)]]
        kept = []
        first = attempt_ready(230, 13, source="m-229")
        message = build_message(first, attempt=claimed("m-src"), context=[*events, first], artifacts=ARTIFACTS,
                                keep_instance=kept)
        self.assertIn(BRIEF_TEXT, message.text)
        instance, binding = kept[0]
        second_cycle = {**claimed("m-src2"), "cycle": {**claimed("m-src2")["cycle"], "id": "cycle-2"}}
        second = debate_turn(instance, binding, attempt_ready(231, 1, source="m-229"), second_cycle)
        self.assertIn(PEER_TEXT, second.text)
        self.assertNotIn(BRIEF_TEXT, second.text)

    def test_cycle_source_scan_and_cache_respect_the_epoch_floor(self):
        # OP-12 P5 review: "epoch floor ignored in the scan" survived; OpenClaw
        # review note 2: a cache hit must respect the floor.
        events = [OLD_MESSAGE, START, *GAP, *[contribution(seq) for seq in range(200, 230)]]
        ready = attempt_ready(230, 13, source="m-229")
        message = build_message(ready, attempt=claimed("m-30"), context=[*events, ready], artifacts=OLD_ARTIFACTS)
        self.assertNotIn(OLD_TEXT, message.text)
        # A remembered source below the current floor, or for another floor, is not used.
        kept = []
        build_message(ready, attempt=claimed("m-src"), context=[START, SOURCE, *events[2:], ready],
                      artifacts=ARTIFACTS, keep_instance=kept)
        instance, binding = kept[0]
        key = next(iter(instance._cycle_source_events))
        instance._cycle_source_events[key] = (1, OLD_MESSAGE)
        scans = []
        with counted_scans(scans):
            later = debate_turn(instance, binding, attempt_ready(231, 14, source="m-229"), claimed("m-src"))
        self.assertEqual(scans, ["m-src"])
        self.assertNotIn(OLD_TEXT, later.text)
        self.assertIn(BRIEF_TEXT, later.text)

    def test_cycle_source_cache_is_bounded(self):
        # OP-12 P5 review: "cache bound removed" survived.
        instance = object.__new__(adapter.SyntheticSocialityAdapter)
        instance._call = lambda _binding, operation: asyncio.sleep(0, result=operation(types.SimpleNamespace(
            events=lambda _room, after, _wait: {"events": [{**SOURCE, "seq": 100}] if after < 100 else []})))
        binding = adapter.RoomBinding("https://room.example/api", "room-1", "member-1", "credential")

        async def run():
            for index in range(adapter.CYCLE_DOCUMENT_SOURCE_CACHE + 5):
                attempt = {"cycle": {"id": f"cycle-{index}", "sourceEventId": "m-src"}}
                self.assertIsNotNone(await instance._cycle_document_source(binding, attempt, [], 150, 41))

        asyncio.run(run())
        self.assertEqual(len(instance._cycle_source_events), adapter.CYCLE_DOCUMENT_SOURCE_CACHE)
        self.assertIn(("room-1", f"cycle-{adapter.CYCLE_DOCUMENT_SOURCE_CACHE + 4}", "m-src"),
                      instance._cycle_source_events)


class PluginManifestTests(unittest.TestCase):
    def test_declared_and_registered_hooks_are_equal(self):
        # Plugin Doctor warned on 1.0.78 and 1.0.79: transform_tool_result was
        # registered but not declared.
        declared, section = [], False
        for line in (ROOT / "plugin.yaml").read_text().splitlines():
            if not line.startswith(" "):
                section = line.strip() == "provides_hooks:"
                continue
            if section and line.strip().startswith("- "):
                declared.append(line.strip()[2:].strip())
        context = mock.MagicMock()
        adapter.register(context)
        registered = [call.args[0] for call in context.register_hook.call_args_list]
        self.assertEqual(sorted(declared), sorted(registered))
        self.assertIn("transform_tool_result", declared)


class P6LinkTests(LinkTestCase):
    def test_a_pending_directory_does_not_block_unlink(self):
        self.store.mkdir(parents=True, exist_ok=True)
        pending = link.pending_path(self.record_path)
        # Empty: removed with the link.
        self.record_path.write_text("{}")
        pending.mkdir()
        self.assertIsNotNone(link.delete_link(self.record_path))
        self.assertFalse(pending.exists())
        self.assertFalse(self.record_path.exists())
        # With content: never a recursive delete; left in place and named.
        self.record_path.write_text("{}")
        pending.mkdir()
        (pending / "keep.txt").write_text("hand-made")
        removed = link.delete_link(self.record_path)
        self.assertTrue(removed.get("pendingNotRemoved"))
        self.assertFalse(self.record_path.exists())
        self.assertEqual((pending / "keep.txt").read_text(), "hand-made")
        # Only the pending directory: unlink still completes.
        removed = link.delete_link(self.record_path)
        self.assertTrue(removed.get("pendingOnly"))

    def test_a_malformed_dashboard_address_is_a_coded_refusal(self):
        # OP-12 P5 review note 4: never an uncaught ValueError.
        for url in ("http://[::1", "http://[::1]x:80", "http://[zz]/"):
            with self.assertRaises(link.LocalGatewayError) as caught:
                link.resolve_local_gateway(url)
            self.assertEqual(caught.exception.code, "local_gateway_auth_unavailable", url)


if __name__ == "__main__":
    unittest.main()
