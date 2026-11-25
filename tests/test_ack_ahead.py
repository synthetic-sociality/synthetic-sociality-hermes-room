"""Hermes 1.0.118: a Room acknowledgement ahead of the local cursor is adopted (contract P2c-3i), paired with OpenClaw 0.2.93.

Case names are the keys of ``conformance.json`` ``parityP2c3iCases``; OpenClaw implements the same
names. The fixture is the Berlin trace (Digital sovereignty, 2026-11-20..25): local acknowledged
cursor 3220, terminal evidence only for 3221 (ignored), Room acknowledgedSeq 3222. Before the fix
every replayed acknowledgement of 3221 raised the retryable ack_frontier_mismatch.
No test uses a real credential, and no test talks to a live Room.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

from test_delivery_lifecycle import ROOT, adapter, state_store

CONFORMANCE = json.loads((ROOT / "conformance.json").read_text())
CONTRACT = (ROOT / "docs" / "delivery-lifecycle-contract-v1.md").read_text(encoding="utf-8")
local_status = adapter.local_status

ACK_AHEAD_CASES = [
    "ACK-AHEAD-01 adopt", "ACK-AHEAD-02 one_warning", "ACK-AHEAD-03 behind_errors",
    "ACK-AHEAD-04 non_int_errors", "ACK-AHEAD-05 restart_keeps_cursor",
]
BLOCK_SHA256 = "760f6be0bc71319c4e906545c00df4314c354e35f8927498407cd82da626d361"
MEMBER = "member-1"
ROOM = "duev8o1h936bl9f"


def ignored(seq: int) -> dict:
    return {"status": "ignored", "sourceEventId": f"ev{seq}", "canonicalEventId": "", "canonicalSeq": 0,
            "canonicalTs": "", "reason": "not_addressed"}


def berlin() -> adapter.RoomBinding:
    """The persisted Berlin binding, loaded through RoomBinding.from_dict."""
    raw = {
        "base_url": "https://room.example/api", "room_id": ROOM, "membership_id": MEMBER,
        "credential": "credential", "installation_id": "inst-1",
        "cursor": 3221, "acknowledged_cursor": 3220,
        "inbox": {"3221": "ignored"},
        "terminal_evidence": {"3221": ignored(3221)},
    }
    return adapter.RoomBinding.from_dict(json.loads(json.dumps(raw)))


class AheadRoom:
    """The acknowledgement route only; ``answer`` builds the Room's reply for a requested seq."""

    def __init__(self, answer):
        self.answer = answer
        self.acks = []

    def acknowledge(self, _room, seq):
        self.acks.append(seq)
        return self.answer(seq)


def room_max(held: int):
    return lambda seq: {"acknowledgedSeq": max(seq, held)}


class Harness:
    def __init__(self, binding, api, directory=None, *, persist=False):
        self.scratch = tempfile.TemporaryDirectory()  # removed with the harness
        directory = Path(directory or self.scratch.name)
        self.binding, self.api = binding, api
        self.state_file = directory / "private" / "state.json"
        instance = object.__new__(adapter.SyntheticSocialityAdapter)
        instance._state_file = self.state_file
        instance._ledger_locks = {}
        instance._status = local_status.LocalStatus(directory / "status.json", plugin="hermes",
                                                    version=adapter.CONNECTOR_VERSION, host_label="host-a")
        if persist:
            instance._persist_binding = types.MethodType(adapter.SyntheticSocialityAdapter._persist_binding, instance)
        else:
            instance._persist_binding = lambda _current: True
        instance._call = lambda _binding, operation, **_options: asyncio.sleep(0, result=operation(api))
        instance._inflight_events = set()
        instance._retry_acknowledgement = mock.Mock(side_effect=AssertionError("no reconnect loop"))
        instance._status_room = mock.Mock()
        self.instance = instance

    def complete(self, seq, **options):
        with state_store.state_path_scope(self.state_file):
            asyncio.run(self.instance._complete_event(self.binding, seq, **options))

    def save(self):
        with state_store.state_path_scope(self.state_file):
            state_store.save(state_store.PluginState(bindings=[self.binding]))

    def reload(self) -> adapter.RoomBinding:
        return state_store.load(self.state_file).binding(ROOM)


class AckAheadConformanceTests(unittest.TestCase):
    def test_the_paragraph_follows_p2c_3h_byte_identical(self):
        start = CONTRACT.index("<<<P2c-3i\n")
        end = CONTRACT.index("\nP2c-3i>>>\n", start) + len("\nP2c-3i>>>\n")
        self.assertEqual(hashlib.sha256(CONTRACT[start:end].encode("utf-8")).hexdigest(), BLOCK_SHA256)
        self.assertTrue(CONTRACT[:start].endswith("\nP2c-3h>>>\n\n"))
        self.assertTrue(CONTRACT[end:].startswith("\nP2c-4 Local status. "))
        self.assertEqual(CONTRACT.count("<<<P2c-3i"), 1)

    def test_the_contract_hash_is_the_pinned_one(self):
        digest = hashlib.sha256((ROOT / "docs" / "delivery-lifecycle-contract-v1.md").read_bytes()).hexdigest()
        self.assertEqual(CONFORMANCE["contractSha256"], digest)

    def test_the_case_names_are_the_shared_ones_and_each_has_a_test(self):
        self.assertEqual(CONFORMANCE["parityP2c3iCases"], ACK_AHEAD_CASES)
        names = set(dir(AckAheadCaseTests))
        for case in ACK_AHEAD_CASES:
            number = case.split(" ", 1)[0].lower().replace("-", "_")
            self.assertTrue(any(name.startswith(f"test_{number}_") for name in names), case)


class AckAheadCaseTests(unittest.TestCase):
    def test_ack_ahead_01_a_room_ack_ahead_is_adopted_as_a_fence(self):
        harness = Harness(berlin(), AheadRoom(room_max(3222)))
        before = copy.deepcopy(harness.binding.terminal_evidence["3221"])
        # The replay of the durable 3221 completion that ran every poll.
        harness.complete(3221)
        binding = harness.binding
        self.assertEqual(harness.api.acks, [3221])
        self.assertEqual(binding.acknowledged_cursor, 3222)
        self.assertGreaterEqual(binding.cursor, 3222)
        self.assertEqual(binding.inbox["3222"], "ignored")
        self.assertEqual(binding.terminal_evidence["3222"]["status"], "ignored")
        self.assertEqual(binding.terminal_evidence["3222"]["reason"], "room_ack_ahead")
        self.assertEqual(binding.inbox["3221"], "ignored")
        self.assertEqual(binding.terminal_evidence["3221"], before)
        harness.instance._retry_acknowledgement.assert_not_called()
        for call in harness.instance._status_room.call_args_list:
            self.assertNotEqual(call.kwargs.get("connection"), "reconnecting")
        # Delivery continues from 3223: an earlier seq is not consumed again.
        self.assertIsNone(asyncio.run(harness.instance._consume_locked(binding, {"id": "ev3222", "seq": 3222})))
        self.assertEqual(binding.inbox["3222"], "ignored")

    def test_ack_ahead_01_a_deferred_acknowledgement_adopts_too(self):
        harness = Harness(berlin(), AheadRoom(room_max(3222)))
        harness.complete(3221, defer_ack_failure=True)
        self.assertEqual(harness.binding.acknowledged_cursor, 3222)
        harness.instance._retry_acknowledgement.assert_not_called()

    def test_ack_ahead_01_existing_evidence_in_the_adopted_range_is_kept(self):
        binding = berlin()
        binding.inbox["3223"] = "ignored"
        binding.terminal_evidence["3223"] = ignored(3223)
        binding.inbox["3222"] = "pending"
        harness = Harness(binding, AheadRoom(room_max(3224)))
        before = copy.deepcopy(binding.terminal_evidence["3223"])
        harness.complete(3221)
        self.assertEqual(harness.api.acks, [3221])
        self.assertEqual(binding.acknowledged_cursor, 3224)
        self.assertEqual(binding.terminal_evidence["3223"], before)
        for seq in ("3222", "3224"):
            self.assertEqual(binding.inbox[seq], "ignored")
            self.assertEqual(binding.terminal_evidence[seq]["reason"], "room_ack_ahead")

    def test_ack_ahead_02_one_warning_per_adoption_and_none_for_an_equal_answer(self):
        harness = Harness(berlin(), AheadRoom(room_max(3222)))
        with self.assertLogs(adapter.logger, level=logging.WARNING) as logs:
            harness.complete(3221)
        warnings = [record for record in logs.records if record.levelno >= logging.WARNING]
        self.assertEqual(len(warnings), 1)
        self.assertIn("room_ack_ahead", warnings[0].getMessage())
        self.assertIn(ROOM, warnings[0].getMessage())
        # The next polls replay the same events and the Room answers the same value.
        with self.assertNoLogs(adapter.logger, level=logging.WARNING):
            harness.complete(3221)
            harness.complete(3222)
            asyncio.run(harness.instance._consume_locked(harness.binding, {"id": "ev3221", "seq": 3221}))
        self.assertEqual(harness.binding.acknowledged_cursor, 3222)
        self.assertEqual(harness.api.acks, [3221])

    def test_ack_ahead_03_an_answer_behind_the_cursor_still_raises(self):
        harness = Harness(berlin(), AheadRoom(lambda _seq: {"acknowledgedSeq": 3220}))
        with self.assertRaises(adapter.ProtocolError) as raised:
            harness.complete(3221)
        self.assertEqual(raised.exception.code, "ack_frontier_mismatch")
        self.assertEqual(harness.binding.acknowledged_cursor, 3220)
        self.assertNotIn("3222", harness.binding.inbox)

    def test_ack_ahead_04_a_missing_or_non_int_answer_still_raises(self):
        for answer in (None, {}, {"acknowledgedSeq": None}, {"acknowledgedSeq": "3222"},
                       {"acknowledgedSeq": 3222.0}, {"acknowledgedSeq": True}):
            with self.subTest(answer=answer):
                harness = Harness(berlin(), AheadRoom(lambda _seq, answer=answer: answer))
                with self.assertRaises(adapter.ProtocolError) as raised:
                    harness.complete(3221)
                self.assertEqual(raised.exception.code, "ack_frontier_mismatch")
                self.assertEqual(harness.binding.acknowledged_cursor, 3220)
                self.assertNotIn("3222", harness.binding.inbox)

    def test_ack_ahead_05_a_restart_after_adoption_keeps_the_cursor(self):
        harness = Harness(berlin(), AheadRoom(room_max(3222)), persist=True)
        harness.save()
        harness.complete(3221)
        restarted = harness.reload()
        self.assertEqual(restarted.acknowledged_cursor, 3222)
        self.assertEqual(restarted.inbox.get("3222"), "ignored")
        self.assertEqual(restarted.terminal_evidence["3222"]["reason"], "room_ack_ahead")
        self.assertEqual(restarted.terminal_evidence["3221"], ignored(3221))
        # The restarted connector acknowledges nothing again for the adopted range.
        second = Harness(restarted, AheadRoom(room_max(3222)), harness.scratch.name, persist=True)
        with self.assertNoLogs(adapter.logger, level=logging.WARNING):
            second.complete(3221)
            second.complete(3222)
        self.assertEqual(second.api.acks, [])
        self.assertEqual(second.reload().acknowledged_cursor, 3222)

    def test_ack_ahead_05_another_room_is_untouched(self):
        harness = Harness(berlin(), AheadRoom(room_max(3222)), persist=True)
        other = berlin()
        other.room_id, other.membership_id = "room-other", "member-2"
        with state_store.state_path_scope(harness.state_file):
            state_store.save(state_store.PluginState(bindings=[harness.binding, other]))
        harness.complete(3221)
        state = state_store.load(harness.state_file)
        self.assertEqual(state.binding(ROOM).acknowledged_cursor, 3222)
        self.assertEqual(state.binding("room-other").acknowledged_cursor, 3220)
        self.assertNotIn("3222", state.binding("room-other").inbox)


if __name__ == "__main__":
    unittest.main()
