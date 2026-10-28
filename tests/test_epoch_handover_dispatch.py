"""Dispatch-level proof for the reviewed handover and catalog-only library.

Builds the exact prompt the adapter hands to Hermes for a new discussion and
for a later turn in it, asserting what enters the fresh working context and
what stays out (accumulated library text, older epochs).
"""
from __future__ import annotations

import asyncio
import types
import unittest

from test_delivery_lifecycle import adapter


HANDOVER = {
    "sourceEpochId": "epoch-1", "sourceEpochOrdinal": 1, "coveredThroughSeq": 40, "revision": 1,
    "reviewStatus": "human_reviewed", "digest": "f" * 64, "submittedByMembershipId": "owner-1",
    "sections": {
        "goal": "Keep decisions, drop the accumulated history.",
        "decisions": [{"statement": "Fresh session per discussion.", "rationale": "Prompts grew to 59k characters."}],
        "openQuestions": ["Which runtime hook reports usage?"],
    },
    "sourceRefs": [{"kind": "event", "eventId": "evt-40", "seq": 40}],
}
START_EVENT = {
    "id": "start-2", "seq": 41, "type": "discussion.started", "actorRole": "human_owner", "actorId": "owner-1",
    "payload": {"epoch": {"id": "epoch-2", "ordinal": 2, "startsAtSeq": 41, "topic": {"title": "Continue the work"}, "status": "active"}, "handover": HANDOVER},
}
LIBRARY = {"items": [{
    "artifactId": "artifact-1", "visibility": "room_shared", "title": "Kabeltracker",
    "currentVersion": {"versionId": "v1", "name": "kabeltracker.pdf", "mediaType": "application/pdf",
                       "sha256": "a" * 64, "extractionStatus": "ready", "extractedText": "LIBRARY FULL TEXT " * 2_000},
}]}


class EpochHandoverDispatchTests(unittest.TestCase):
    def dispatch(self, event, *, events_pages, calls):
        captured = []
        instance = object.__new__(adapter.SyntheticSocialityAdapter)
        instance.platform = adapter.Platform(adapter.NAME)
        instance._inflight_events = set()
        instance._event_seq = {}
        instance._event_epoch = {}
        instance._latest_source = {}
        instance._run_for_event = {}
        instance._cycle_attempts = {}
        instance._epoch_handovers = self.handover_cache

        def events(room_id, after, wait):
            calls.append(("events", after))
            return events_pages.get(after, {"events": []})

        def artifacts(room_id):
            calls.append(("artifacts", room_id))
            return LIBRARY

        api = types.SimpleNamespace(
            room_state=lambda _room_id: {
                "title": "Room", "headSeq": 43,
                "activeEpoch": {"id": "epoch-2", "startsAtSeq": 41},
                "roster": [{"membershipId": "owner-1", "displayName": "Owner"}, {"membershipId": "member-1", "displayName": "Agent"}],
            },
            room_policy=lambda _room_id: {"policy": {}},
            events=events, artifacts=artifacts,
            artifact=lambda _room_id, _artifact_id: self.fail("catalog listing must not fetch artifacts"),
        )
        instance._call = lambda _binding, operation, **_request_options: asyncio.sleep(0, result=operation(api))
        instance._publish = lambda *_args, **_kwargs: asyncio.sleep(0)
        instance.build_source = types.MethodType(
            lambda self, **kwargs: types.SimpleNamespace(platform=self.platform, **kwargs), instance,
        )
        instance.handle_message = lambda message: asyncio.sleep(0, result=captured.append(message))
        binding = adapter.RoomBinding("https://room.example/api", "room-1", "member-1", "credential", installation_id="inst-1")
        binding.epoch_session_routing_initialized = True
        binding.legacy_session_epoch_id = "epoch-1"
        self.assertTrue(asyncio.run(instance._dispatch_room_event(binding, event, "generation")))
        return captured[0]

    def setUp(self):
        self.handover_cache = {}

    def test_new_discussion_prompt_carries_the_handover_and_only_a_library_catalog(self):
        calls = []
        with self.assertLogs(adapter.logger, level="INFO") as logs:
            message = self.dispatch(START_EVENT, events_pages={40: {"events": [START_EVENT]}}, calls=calls)
        prompt = message.text
        sizes = [line for line in logs.output if "context sizes:" in line]
        self.assertEqual(len(sizes), 1, logs.output)
        self.assertIn(f"prompt={len(prompt)}", sizes[0])
        self.assertIn("documents=", sizes[0])
        self.assertNotIn("Keep decisions", sizes[0])  # sizes only, never content
        self.assertIn("[Reviewed handover from discussion #1", prompt)
        self.assertIn("Keep decisions, drop the accumulated history.", prompt)
        self.assertIn("Fresh session per discussion. — because Prompts grew to 59k characters.", prompt)
        self.assertIn("event evt-40 (seq 40)", prompt)
        self.assertIn("kabeltracker.pdf", prompt)
        self.assertNotIn("LIBRARY FULL TEXT", prompt)
        self.assertIn("synthetic_sociality_room_history", prompt)
        # The start event itself is the epoch opener: no extra read for it.
        self.assertNotIn(("events", 40), calls)
        # The new epoch runs on its own Hermes thread, not the legacy session.
        self.assertEqual(message.source.thread_id, adapter._epoch_thread_id("epoch-2"))
        self.assertLess(len(prompt), 8_000, len(prompt))
        print(f"\nB34 measurement (Hermes first prompt of new discussion incl. handover): {len(prompt)} chars; library text present: {'LIBRARY FULL TEXT' in prompt}")

    def test_later_turn_reuses_the_cached_handover_and_stays_bounded(self):
        calls = []
        later = {
            "id": "m-43", "seq": 43, "type": "message.posted", "actorRole": "human_owner", "actorId": "owner-1",
            "payload": {"body": "What did we decide about sessions?"},
        }
        pages = {40: {"events": [START_EVENT]}, 41: {"events": [START_EVENT, {"id": "m-42", "seq": 42, "type": "message.posted", "payload": {"body": "earlier in this epoch"}}]}}
        first = self.dispatch(later, events_pages=pages, calls=calls)
        self.assertIn("[Reviewed handover from discussion #1", first.text)
        # The bounded transcript loader reads from seq 40 once per dispatch; the
        # handover lookup adds exactly one more read the first time only.
        self.assertEqual(calls.count(("events", 40)), 2)
        self.assertEqual(list(self.handover_cache), [("room-1", "epoch-2")])
        second = self.dispatch(later, events_pages=pages, calls=calls)
        self.assertIn("[Reviewed handover from discussion #1", second.text)
        self.assertEqual(calls.count(("events", 40)), 3, "handover is cached per epoch")
        self.assertNotIn("LIBRARY FULL TEXT", second.text)
        self.assertEqual(second.source.thread_id, adapter._epoch_thread_id("epoch-2"))

    def test_discussion_without_handover_keeps_the_plain_prompt(self):
        plain_start = {**START_EVENT, "payload": {"epoch": START_EVENT["payload"]["epoch"]}}
        message = self.dispatch(plain_start, events_pages={40: {"events": [plain_start]}}, calls=[])
        self.assertNotIn("Reviewed handover", message.text)
        self.assertIn("Continue the work", message.text)


if __name__ == "__main__":
    unittest.main()
