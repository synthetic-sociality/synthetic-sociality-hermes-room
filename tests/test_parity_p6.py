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
from test_parity_p1a import HUMAN, PEER, START, START_WITH_HANDOVER, attempt_ready, build_message, cycle
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

    async def call(binding, operation, **_request_options):
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
                    message = build_message(attempt_ready(170, 1, source="m-43"), attempt=claimed("m-src"),
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
            restarted._call = lambda _binding, operation, **_request_options: asyncio.sleep(
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
        self.assertEqual([vector["name"] for vector in vectors], ["cut", "drop", "fit", "one", "edges"])
        for vector in vectors:
            with self.subTest(vector=vector["name"]):
                if "roomRules" in vector:
                    # Contract P6-4 edges: rule shapes, the trimmed set, a lone surrogate.
                    rules = vector["roomRules"]
                    rendered = "\n".join(context_module.saved_guidance(rules)[1])
                else:
                    rules = [{"enforcement": "guidance", "text": text} for text in vector["rules"]]
                    rendered = "\n".join(context_module.bounded_guidance(vector["rules"]))
                self.assertEqual(len(rendered), vector["codePoints"])
                self.assertLessEqual(len(rendered), context_module.GUIDANCE_CHARACTER_LIMIT)
                self.assertEqual(hashlib.sha256(rendered.encode("utf-8")).hexdigest(), vector["sha256"])
                # The prompt carries exactly that text.
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
        self.assertEqual(len(cases), 10)
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

        # Slow: the turn waits at most the bound, then proceeds; a timeout is
        # not remembered, so the next turn tries again, under the same bound.
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
        handed.clear()
        started = time.monotonic()
        with counted_scans(scans, slow), mock.patch.object(adapter, "CYCLE_DOCUMENT_SOURCE_TIMEOUT_SECONDS", 0.2), \
                self.assertLogs(adapter.logger, logging.WARNING):
            later = debate_turn(instance, binding, attempt_ready(231, 14, source="m-229"), claimed("m-src"))
        self.assertEqual(scans, ["m-src", "m-src"], "one bounded attempt per turn")
        self.assertNotIn(BRIEF_TEXT, later.text)
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
        instance._call = lambda _binding, operation, **_request_options: asyncio.sleep(0, result=operation(types.SimpleNamespace(
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
    def test_manifest_equals_1_0_79_plus_document_and_share_tools(self):
        # The installers (install-hermes-systemd.sh, install-hermes-mac.sh) refuse
        # a release whose plugin.yaml differs from the installed one in anything
        # but the version: line ("STOP: plugin.yaml differs beyond version");
        # 1.0.80 was uninstallable for that reason. A manifest change needs an
        # installer decision by the owner first: P7d (owner, 2026-10-05T00:30:41Z,
        # "installers accept reviewed manifest changes") adds exactly the
        # document tool and the transform_tool_result hook; the installers admit
        # the hash pinned as parityP7dHermesManifestSha256. P14 adds the five
        # ArtCraft tools; the README rollback note records the new manifest hash.
        def without_version(path):
            return [line for line in path.read_text().splitlines() if not line.startswith("version:")]

        fixture = without_version(ROOT / "tests" / "fixtures" / "plugin-1.0.79.yaml")
        fixture.insert(fixture.index("  - synthetic_sociality_room_post") + 1, "  - synthetic_sociality_room_document")
        fixture.insert(fixture.index("  - synthetic_sociality_room_document") + 1, "  - synthetic_sociality_room_share_file")
        offset = fixture.index("  - synthetic_sociality_room_share_file") + 1
        fixture[offset:offset] = ["  - " + name for name in (
            "artcraft_capabilities", "artcraft_create", "artcraft_run", "artcraft_job", "artcraft_artifact",
        )]
        fixture.insert(fixture.index("  - pre_tool_call") + 1, "  - transform_tool_result")
        self.assertEqual(without_version(ROOT / "plugin.yaml"), fixture)
        self.assertIn(f"version: {adapter.CONNECTOR_VERSION}", (ROOT / "plugin.yaml").read_text().splitlines())

    def test_transform_tool_result_is_registered_at_runtime(self):
        # Registered and, since P7d, declared (the Plugin Doctor warning of
        # 1.0.79 to 1.0.83 is gone).
        context = mock.MagicMock()
        adapter.register(context)
        registered = [call.args[0] for call in context.register_hook.call_args_list]
        self.assertIn("transform_tool_result", registered)
        self.assertIn("  - transform_tool_result\n", (ROOT / "plugin.yaml").read_text())


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



def switchable_start(holder, page_start=None):
    """Serve the Room state with ``holder["start"]`` as the epoch start, and
    optionally event pages that carry their own (disagreeing) start."""
    def prepare(instance, _binding):
        inline = instance._call

        def call(binding, operation, **_request_options):
            def wrapped(api):
                state = api.room_state(binding.room_id)
                pages = api.events

                def events(room, after, wait):
                    page = dict(pages(room, after, wait))
                    if page_start is not None:
                        page["activeEpochStartsAtSeq"] = page_start
                    return page

                fields = {**vars(api), "events": events, "room_state": lambda _room: {
                    **state, "activeEpoch": {"id": "epoch-2", "startsAtSeq": holder["start"]}}}
                return operation(types.SimpleNamespace(**fields))
            return inline(binding, wrapped)

        instance._call = call
    return prepare


class ParityP6Round2Tests(unittest.TestCase):
    def test_p5_03_a_failed_cycle_source_lookup_is_tried_again_on_the_next_turn(self):
        # Turn 1 holds the source in its loaded context. The Room then moves on
        # (the fake API reads this same list), so turn 2 looks the source up and
        # meets one Room error; on turn 3 the Room is healthy and the brief is back.
        room = [START, SOURCE, *[contribution(seq) for seq in range(43, 49)]]
        first = attempt_ready(49, 1, source="m-48")
        room.append(first)
        kept, scans = [], []
        message = build_message(first, attempt=claimed(), context=room, artifacts=ARTIFACTS, keep_instance=kept)
        self.assertEqual(message.text.count(BRIEF_TEXT), 1)
        instance, binding = kept[0]
        room.extend([*GAP[7:], *[contribution(seq) for seq in range(200, 230)]])
        failures = [adapter.ProtocolError("Room unavailable", code="unavailable", retryable=True)]
        real = adapter._cycle_source_message

        def once(*args, **kwargs):
            if failures:
                raise failures.pop()
            return real(*args, **kwargs)

        with counted_scans(scans, once), self.assertLogs(adapter.logger, logging.WARNING) as logs:
            second = debate_turn(instance, binding, attempt_ready(230, 2, source="m-229"), claimed())
        self.assertNotIn(BRIEF_TEXT, second.text)
        self.assertTrue(any("cycle_source_unresolved" in line and "ProtocolError" in line for line in logs.output))
        with counted_scans(scans):
            third = debate_turn(instance, binding, attempt_ready(231, 3, source="m-229"), claimed())
        self.assertEqual(third.text.count(BRIEF_TEXT), 1, "the brief is back on the next turn")
        self.assertEqual(scans, ["m-src", "m-src"], "one attempt per turn")
        # Found, then remembered: no further lookup.
        with counted_scans(scans):
            fourth = debate_turn(instance, binding, attempt_ready(232, 4, source="m-229"), claimed())
        self.assertEqual(scans, ["m-src", "m-src"])
        self.assertEqual(fourth.text.count(BRIEF_TEXT), 1)

    def test_p6_05_an_unknown_start_turn_keeps_nothing_per_epoch(self):
        holder = {"start": 0}
        kept = []
        context = [START_WITH_HANDOVER, HUMAN, PEER]
        first = build_message(attempt_ready(60, 1, source="start-2"), attempt=cycle(), context=context,
                              keep_instance=kept, prepare=switchable_start(holder))
        self.assertNotIn("Decide how to disclose a diagnosis.", first.text)
        self.assertEqual(first.source.user_name, "Room (system)")
        holder["start"] = 41
        instance, binding = kept[0]
        second = debate_turn(instance, binding, attempt_ready(61, 1, source="start-2"), cycle())
        self.assertIn("Decide how to disclose a diagnosis.", second.text)
        # The discussion start is the current item again, attributed to the owner.
        self.assertEqual(second.source.user_name, "Owner")

    def test_p6_06_the_start_is_known_only_as_an_integer_from_the_room_state(self):
        for value in (None, 0, -3, 1.5, "41", "abc", True, False, [41], {"seq": 41}, float("nan"), 2**53):
            with self.subTest(value=value):
                self.assertEqual(adapter._known_epoch_start(value), 0)
        for value, known in ((41, 41), (41.0, 41), (1, 1), (2**53 - 1, 2**53 - 1)):
            self.assertEqual(adapter._known_epoch_start(value), known)
        context = (OLD_MESSAGE, START, HUMAN, PEER)
        for value in ("41", True, 1.5, [41], "abc"):
            with self.subTest(dispatch=value), self.assertLogs(adapter.logger, logging.WARNING) as logs:
                message = build_message({**HUMAN, "id": "m-169", "seq": 169}, context=context,
                                        prepare=switchable_start({"start": value}))
            self.assertNotIn("Disclosure first", message.text)
            self.assertEqual(sum("code epoch_start_unknown" in line for line in logs.output), 1)
        # Page and Room state disagree: the Room state wins, the turn proceeds.
        message = build_message({**HUMAN, "id": "m-69", "seq": 69}, context=context,
                                prepare=switchable_start({"start": 41}, page_start=0))
        self.assertIn("Disclosure first", message.text)
        self.assertNotIn("rejected plan", message.text)
        with self.assertLogs(adapter.logger, logging.WARNING) as logs:
            message = build_message({**HUMAN, "id": "m-169", "seq": 169}, context=context,
                                    prepare=switchable_start({"start": 0}, page_start=41))
        self.assertNotIn("Disclosure first", message.text)
        self.assertTrue(any("code epoch_start_unknown" in line for line in logs.output))

    def test_p6_07_an_unknown_start_still_renders_the_current_item(self):
        answered = {**PEER, "payload": {**PEER["payload"], "attachments": [manifest("peer")]}}
        with self.assertLogs(adapter.logger, logging.WARNING):
            message = build_message(attempt_ready(70, 1, source="m-43"), attempt=cycle(),
                                    context=(OLD_MESSAGE, START, HUMAN, answered), starts_at=0, artifacts=ARTIFACTS)
        self.assertIn("#43 Peer: Disclosure first, then support.", message.text)
        self.assertIn(PEER_TEXT, message.text)
        for earlier in ("Should the doctor disclose", "rejected plan", OLD_TEXT):
            self.assertNotIn(earlier, message.text)
        self.assertEqual(message.source.user_name, "Peer")
        # Outside the ordinary loaded page (100 sequence numbers): no current item.
        with self.assertLogs(adapter.logger, logging.WARNING):
            message = build_message(attempt_ready(170, 1, source="m-43"), attempt=cycle(),
                                    context=(OLD_MESSAGE, START, HUMAN, answered), starts_at=0, artifacts=ARTIFACTS)
        self.assertNotIn("Disclosure first", message.text)

    def test_p6_08_an_unresolvable_or_hanging_turn_source_costs_one_bounded_scan(self):
        events = [START, *GAP, *[contribution(seq) for seq in range(200, 230)]]
        scans = []
        real = adapter._cycle_source_message

        def counting(*args, **kwargs):
            if kwargs.get("types") == adapter.MISSED_SOURCE_EVENT_TYPES:
                scans.append(args[1])
                if hang:
                    time.sleep(1.5)
                    return None
            return real(*args, **kwargs)

        hang = False
        kept = []
        ready = attempt_ready(230, 13, source="m-gone")
        with mock.patch.object(adapter, "_cycle_source_message", side_effect=counting):
            build_message(ready, attempt=cycle(), context=[*events, ready], keep_instance=kept)
            instance, binding = kept[0]
            debate_turn(instance, binding, attempt_ready(231, 14, source="m-gone"), cycle())
        self.assertEqual(scans, ["m-gone"], "not_found is remembered")
        # Hanging: the turn waits at most the bound and the next turn may scan again.
        hang, handed, kept = True, [], []

        def prepare(instance, _binding):
            threaded_calls(instance)
            original = instance.handle_message
            instance.handle_message = lambda message: handed.append(time.monotonic()) or original(message)

        started = time.monotonic()
        with mock.patch.object(adapter, "_cycle_source_message", side_effect=counting), \
                mock.patch.object(adapter, "CYCLE_DOCUMENT_SOURCE_TIMEOUT_SECONDS", 0.2), \
                self.assertLogs(adapter.logger, logging.WARNING) as logs:
            build_message(attempt_ready(230, 13, source="m-slow"), attempt=cycle(),
                          context=[*events, attempt_ready(230, 13, source="m-slow")], keep_instance=kept, prepare=prepare)
        self.assertLess(handed[0] - started, 1.2)
        self.assertTrue(any("cycle source unavailable" in line and "timed out" in line for line in logs.output))
        self.assertEqual(scans, ["m-gone", "m-slow"])
        instance, binding = kept[0]
        with mock.patch.object(adapter, "_cycle_source_message", side_effect=counting), \
                mock.patch.object(adapter, "CYCLE_DOCUMENT_SOURCE_TIMEOUT_SECONDS", 0.2), \
                self.assertLogs(adapter.logger, logging.WARNING):
            debate_turn(instance, binding, attempt_ready(231, 14, source="m-slow"), cycle())
        self.assertEqual(scans, ["m-gone", "m-slow", "m-slow"], "a timeout is not remembered; one scan per turn")

    def test_p6_09_the_room_state_topic_is_authoritative_and_a_cleared_brief_is_gone(self):
        brief = "Brief: the owner's framing of the debate."
        start = {**START, "payload": {"epoch": {**START["payload"]["epoch"],
                                                "topic": {"title": "Old title", "description": brief}}}}
        context = [start, HUMAN, PEER]
        # The brief as the Room state carries it: rendered verbatim.
        text = build_message({**HUMAN, "id": "m-59", "seq": 59}, context=context,
                             topic={"title": "Disclosure", "description": brief}).text
        self.assertIn(f"Current discussion: Disclosure\n{brief}", text)
        for description in ("", None):
            with self.subTest(description=description):
                # Cleared: gone, with no fallback to the discussion.started payload,
                # in the context lines and in the presented start (P1a-5).
                for event, attempt in (({**HUMAN, "id": "m-59", "seq": 59}, None),
                                       (attempt_ready(60, 1, source="start-2"), cycle())):
                    message = build_message(event, attempt=attempt, context=context,
                                            topic={"title": "Disclosure", "description": description})
                    self.assertNotIn(brief, message.text)
                    self.assertNotIn("Old title", message.text)
                    self.assertIn("Current discussion: Disclosure", message.text)
        # A cleared title too: no "Current discussion" line and no start title.
        message = build_message(attempt_ready(60, 1, source="start-2"), attempt=cycle(), context=context,
                                topic={"title": "", "description": ""})
        self.assertNotIn("Current discussion", message.text)
        self.assertNotIn("Old title", message.text)
        self.assertNotIn(brief, message.text)
        # No activeTopic at all: the presented start keeps its own topic.
        message = build_message(attempt_ready(60, 1, source="start-2"), attempt=cycle(), context=context)
        self.assertIn(brief, message.text)
        self.assertIn("Old title", message.text)

    def test_p6_10_a_topic_change_is_acknowledged_without_model_work(self):
        from test_parity_p2a import consume_instance
        changed = {"id": "topic-2970", "seq": 2970, "type": "topic.changed", "actorRole": "human_owner",
                   "actorId": "owner-1", "payload": {"change": "description", "epochId": "epoch-2"}}
        for mode in ("coordinated", "open"):
            with self.subTest(mode=mode):
                instance, binding, starts, completed, _persisted = consume_instance(mode=mode)
                dispatched = []
                instance._dispatch_room_event = lambda *args, **kwargs: asyncio.sleep(0, result=dispatched.append(args))
                asyncio.run(instance._consume(binding, changed))
                self.assertEqual((starts, dispatched), ([], []))
                self.assertEqual(len(completed), 1, completed)
                self.assertEqual((completed[0]["terminal_status"], completed[0]["reason"]),
                                 ("ignored", "topic_change_not_a_trigger"))
                # Even with a body, an edit is no turn.
                instance, binding, starts, completed, _persisted = consume_instance(mode=mode)
                instance._dispatch_room_event = lambda *args, **kwargs: asyncio.sleep(0, result=dispatched.append(args))
                asyncio.run(instance._consume(binding, {**changed, "payload": {**changed["payload"], "body": "New brief."}}))
                self.assertEqual((starts, dispatched), ([], []))


class FakeProtocol:
    """A RoomProtocol stand-in for the production ``_call`` (threads, shield)."""
    reads: list[int] = []
    delay = 0.0

    def __init__(self, _base_url, _credential):
        pass

    def events(self, _room, after, _wait):
        FakeProtocol.reads.append(after)
        time.sleep(FakeProtocol.delay)
        return {"events": []}


class ProductionCallBoundTests(unittest.TestCase):
    """OP-12 P6 review finding 2: the bound with the production ``_call``."""

    def run_lookup(self, delay, timeout):
        FakeProtocol.reads, FakeProtocol.delay = [], delay
        instance = object.__new__(adapter.SyntheticSocialityAdapter)
        binding = adapter.RoomBinding("https://room.example/api", "room-1", "member-1", "credential")
        elapsed = []

        async def run():
            started = time.monotonic()
            found = await instance._cycle_document_source(
                binding, {"cycle": {"id": "cycle-1", "sourceEventId": "m-src"}}, [], 2100, 41)
            elapsed.append(time.monotonic() - started)
            return found

        with mock.patch.object(adapter, "RoomProtocol", FakeProtocol), \
                mock.patch.object(adapter, "CYCLE_DOCUMENT_SOURCE_TIMEOUT_SECONDS", timeout), \
                self.assertLogs(adapter.logger, logging.WARNING):
            self.assertIsNone(asyncio.run(run()))
        return elapsed[0], list(FakeProtocol.reads), instance

    def test_a_hanging_read_is_abandoned_not_awaited(self):
        elapsed, _reads, instance = self.run_lookup(delay=1.5, timeout=0.2)
        self.assertLess(elapsed, 1.0, "the turn never waits for the abandoned read")
        self.assertEqual(getattr(instance, "_cycle_source_events", {}), {}, "a timeout is not remembered")

    def test_the_walk_starts_no_read_after_the_deadline(self):
        # 20 pages of 0.1 s each without the deadline; the walk stops at it.
        elapsed, reads, _instance = self.run_lookup(delay=0.1, timeout=0.25)
        self.assertLess(elapsed, 1.0)
        self.assertLessEqual(len(reads), 5, reads)


class CacheConditionTests(unittest.TestCase):
    def lookup(self, cached, starts_at=41):
        instance = object.__new__(adapter.SyntheticSocialityAdapter)
        instance._call = lambda _binding, operation, **_request_options: asyncio.sleep(0, result=operation(types.SimpleNamespace(
            events=lambda _room, after, _wait: {"events": [SOURCE] if after < 42 else []})))
        binding = adapter.RoomBinding("https://room.example/api", "room-1", "member-1", "credential")
        instance._cycle_source_events = {("room-1", "cycle-1", "m-src"): cached}
        scans = []
        with counted_scans(scans):
            found = asyncio.run(instance._cycle_document_source(
                binding, {"cycle": {"id": "cycle-1", "sourceEventId": "m-src"}}, [], 150, starts_at))
        return found, scans

    def test_a_hit_for_another_epoch_start_is_not_used(self):
        # The floor equality alone (the remembered source itself is above the floor).
        found, scans = self.lookup((30, {**SOURCE, "id": "m-src", "seq": 42, "payload": {"body": "stale"}}))
        self.assertEqual(scans, ["m-src"])
        self.assertEqual(found["payload"]["body"], SOURCE["payload"]["body"])

    def test_a_remembered_source_below_the_floor_is_not_used(self):
        # The seq check alone (same epoch start remembered).
        found, scans = self.lookup((41, {**OLD_MESSAGE, "id": "m-src"}))
        self.assertEqual(scans, ["m-src"])
        self.assertEqual(found["seq"], 42)

    def test_not_found_entries_count_against_the_bound(self):
        instance = object.__new__(adapter.SyntheticSocialityAdapter)
        instance._call = lambda _binding, operation, **_request_options: asyncio.sleep(0, result=operation(types.SimpleNamespace(
            events=lambda _room, _after, _wait: {"events": []})))
        binding = adapter.RoomBinding("https://room.example/api", "room-1", "member-1", "credential")

        async def run():
            for index in range(adapter.CYCLE_DOCUMENT_SOURCE_CACHE + 5):
                attempt = {"cycle": {"id": f"cycle-{index}", "sourceEventId": "m-gone"}}
                self.assertIsNone(await instance._cycle_document_source(binding, attempt, [], 150, 41))

        with self.assertLogs(adapter.logger, logging.WARNING):
            asyncio.run(run())
        self.assertEqual(len(instance._cycle_source_events), adapter.CYCLE_DOCUMENT_SOURCE_CACHE)
        self.assertTrue(all(value == (41, None) for value in instance._cycle_source_events.values()))


class P6Round2LinkTests(LinkTestCase):
    def test_a_pending_symlink_to_a_directory_is_removed_as_a_link(self):
        self.store.mkdir(parents=True, exist_ok=True)
        target = self.scratch / "elsewhere"
        target.mkdir()
        (target / "keep.txt").write_text("not ours")
        pending = link.pending_path(self.record_path)
        pending.symlink_to(target, target_is_directory=True)
        removed = link.delete_link(self.record_path)
        self.assertTrue(removed.get("pendingOnly"))
        self.assertFalse(removed.get("pendingNotRemoved"))
        self.assertFalse(pending.is_symlink())
        self.assertEqual((target / "keep.txt").read_text(), "not ours")

    def test_unlink_never_reports_a_deletion_when_only_a_directory_was_left(self):
        self.store.mkdir(parents=True, exist_ok=True)
        pending = link.pending_path(link.link_path())
        pending.mkdir(parents=True)
        (pending / "keep.txt").write_text("hand-made")
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.assertEqual(adapter.cli._unlink(), 0)
        self.assertNotIn("Deleted", output.getvalue())
        self.assertIn("left in place", output.getvalue())
        self.assertTrue((pending / "keep.txt").exists())

if __name__ == "__main__":
    unittest.main()
