"""Hermes 1.0.117: stuck events never hold a cursor (contract P2c-3h), paired with OpenClaw 0.2.92.

Case names are the keys of ``conformance.json`` ``parityP2c3hCases``; OpenClaw implements the same
names. The migration fixtures use the trace seqs (R&C-2, 2026-11-25): Zurie 2654 and 2662 (epoch 21
closed, cycle cy2a958b47fa979, discussion.cycle_attempt_ready), Real 2933 and Wu 2987 (message.posted,
epoch 22), Berlin 3223 (peer.acknowledged, epoch 22); each cursor sits one below. Every migration
fixture also holds a later quarantine that must stay held, so a rule that frees the wrong seq fails.
No test uses a real credential, and no test talks to a live Room.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import re
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from test_delivery_lifecycle import ROOT, adapter

CONFORMANCE = json.loads((ROOT / "conformance.json").read_text())
CONTRACT = (ROOT / "docs" / "delivery-lifecycle-contract-v1.md").read_text(encoding="utf-8")
recovery = adapter.recovery
renewal = adapter.renewal
local_status = adapter.local_status
state_module = __import__(adapter.__name__.rsplit(".", 1)[0] + ".state", fromlist=["state"])

STUCK_CASES = [
    "STK-01 epoch_closed", "STK-02 cycle_terminal", "STK-03 cycle_superseded", "STK-04 age",
    "STK-05 hole_epoch_closed", "STK-06 inflight", "STK-07 retry_pending", "STK-08 under_24h",
    "STK-09 canonical_receipt", "STK-10 open_epoch_hole", "STK-11 reason_grammar", "STK-12 terminal_set_parity",
]
BLOCK_SHA256 = "80258410ef966588e98c06d2dc7014ae7743588b7a02fee3c91badf860d93368"
REASON = re.compile(r"^stuck:(epoch_closed|cycle_terminal|cycle_superseded|age|hole_epoch_closed):([a-z0-9_]+)$")

CYCLE = "cy2a958b47fa979"
MEMBER = "member-1"


def ts(text: str) -> float:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()


NOW = ts("2026-11-25T01:10:00Z")
HOUR = 3600.0


def ignored(seq: int) -> dict:
    return {"status": "ignored", "sourceEventId": f"ev{seq}", "canonicalEventId": "", "canonicalSeq": 0,
            "canonicalTs": "", "reason": "not_addressed"}


class StuckRoom:
    """The read-only Room routes the stuck rule uses, plus the acknowledgement.

    Epoch 21 starts at 2600, epoch 22 at 2700; ``active`` names the active one.
    """

    def __init__(self, events, *, active="epoch-22", cycle_state="running", cycle_generation=1):
        self.active = active
        self.cycle = {"id": CYCLE, "state": cycle_state, "generation": cycle_generation}
        self.events = [dict(item) for item in events]
        self.calls, self.acks = [], []

    def epochs(self, _room):
        self.calls.append("epochs")
        return {"epochs": [
            {"id": "epoch-21", "startsAtSeq": 2600, "status": "active" if self.active == "epoch-21" else "closed"},
            {"id": "epoch-22", "startsAtSeq": 2700, "status": "active" if self.active == "epoch-22" else "closed"},
        ]}

    def events_in_epoch(self, _room, after, epoch_id):
        self.calls.append(("events", after, epoch_id))
        return {"events": [dict(item) for item in self.events if item["seq"] > after and item["epoch"] == epoch_id][:10],
                "deliveredSeq": 3300}

    def room_state(self, _room):
        self.calls.append("state")
        return {"activeEpoch": {"id": self.active, "startsAtSeq": 2700 if self.active == "epoch-22" else 2600}}

    def get_discussion_cycle(self, _room, cycle_id):
        self.calls.append(("cycle", cycle_id))
        return dict(self.cycle)

    def acknowledge(self, _room, seq):
        self.acks.append(seq)
        return {"acknowledgedSeq": seq}


def attempt_ready(seq, generation=1, member=MEMBER, epoch="epoch-21"):
    return {"id": f"ev{seq}", "seq": seq, "type": "discussion.cycle_attempt_ready", "epoch": epoch,
            "payload": {"cycleId": CYCLE, "generation": generation, "membershipId": member}}


def posted(seq, epoch="epoch-22"):
    return {"id": f"ev{seq}", "seq": seq, "type": "message.posted", "epoch": epoch,
            "payload": {"body": "a body that never reaches an audit record"}}


def peer_ack(seq, epoch="epoch-22"):
    return {"id": f"ev{seq}", "seq": seq, "type": "peer.acknowledged", "epoch": epoch, "payload": {}}


def quarantined_intent(seq, code="", **fields):
    intent = {"state": "quarantined", "delivery_state": "quarantined", "lifecycle_state": "not_started",
              "last_error": "quarantined for operator recovery", "last_error_code": code, "failed_at": NOW - 30 * HOUR,
              "selected": {"source_event_id": f"ev{seq}", "source_seq": seq, "state": "selected"}}
    intent.update(fields)
    return intent


def persisted_binding(ack, inbox, evidence_seqs, *, intents=None, pending_since=None):
    """A binding as the state file stores it, loaded through RoomBinding.from_dict (migration)."""
    raw = {
        "base_url": "https://room.example/api", "room_id": "room-1", "membership_id": MEMBER,
        "credential": "credential", "installation_id": "inst-1",
        "cursor": ack, "acknowledged_cursor": ack,
        "inbox": {**{str(seq): "ignored" for seq in evidence_seqs}, **{str(k): v for k, v in inbox.items()}},
        "terminal_evidence": {str(seq): ignored(seq) for seq in evidence_seqs},
        "pending_since": {str(k): v for k, v in (pending_since or {}).items()},
        "delivery_intents": dict(intents or {}),
    }
    return adapter.RoomBinding.from_dict(json.loads(json.dumps(raw)))


def zurie():
    """Digital sovereignty: ack 2653, 2654 and 2662 quarantined in closed epoch 21."""
    return persisted_binding(
        2653, {2654: "quarantined", 2662: "quarantined", 2705: "quarantined"},
        [*range(2655, 2662), *range(2663, 2705)],
        intents={"ev2662": quarantined_intent(2662, "attempt_timeout"),
                 "ev2705": quarantined_intent(2705, "provider_error", failed_at=NOW - 1 * HOUR)},
        pending_since={2654: ts("2026-11-21T09:00:00Z"), 2662: ts("2026-11-21T09:05:00Z"), 2705: NOW - 1 * HOUR},
    ), StuckRoom([attempt_ready(2654), attempt_ready(2662), posted(2705)])


def aged(seq, event, since):
    """Real, Wu, Berlin: one quarantine at ack + 1 in open epoch 22, a later one under 24 h."""
    later = seq + 7
    return persisted_binding(
        seq - 1, {seq: "quarantined", later: "quarantined"}, [*range(seq + 1, later), later + 1],
        pending_since={seq: since, later: NOW - 2 * HOUR},
    ), StuckRoom([event, posted(later)])


def real():
    return aged(2933, posted(2933), ts("2026-11-12T18:00:00Z"))


def wu():
    return aged(2987, posted(2987), ts("2026-11-19T07:30:00Z"))


def berlin():
    return aged(3223, peer_ack(3223), ts("2026-11-24T00:40:00Z"))


class Harness:
    def __init__(self, binding, api, directory):
        self.binding, self.api = binding, api
        self.state_file = Path(directory) / "state.json"
        instance = object.__new__(adapter.SyntheticSocialityAdapter)
        instance._state_file = self.state_file
        instance._ledger_locks = {}
        instance._status = local_status.LocalStatus(Path(directory) / "status.json", plugin="hermes",
                                                    version=adapter.CONNECTOR_VERSION, host_label="host-a")
        instance._persist_binding = lambda _current: True
        instance._call = lambda _binding, operation, **_options: asyncio.sleep(0, result=operation(api))
        instance._inflight_events = set()
        instance._active_dispatch_rooms = {}
        instance._event_dispatch_generation = {}
        instance._queued_events = {}
        instance._event_seq = {}
        instance._dispatch_room_event = mock.AsyncMock(side_effect=AssertionError("stuck work is never replayed"))
        self.instance = instance

    def scan(self, now=NOW):
        with mock.patch.object(adapter.time, "time", lambda: now):
            asyncio.run(self.instance._apply_stuck_events(self.binding))

    def stuck_records(self):
        return recovery.read_stuck_records(recovery.journal_path(self.state_file))

    def status_entry(self):
        data = json.loads((self.state_file.parent / "status.json").read_text())
        return data["memberships"][MEMBER]


def run_case(factory, scans=(NOW,)):
    directory = tempfile.mkdtemp()
    binding, api = factory()
    harness = Harness(binding, api, directory)
    before = copy.deepcopy(binding.delivery_intents)
    for now in scans:
        harness.scan(now)
    return harness, before


class StuckConformanceTests(unittest.TestCase):
    def test_the_paragraph_follows_p2c_3f_byte_identical(self):
        start = CONTRACT.index("<<<P2c-3h\n")
        end = CONTRACT.index("\nP2c-3h>>>\n", start) + len("\nP2c-3h>>>\n")
        self.assertEqual(hashlib.sha256(CONTRACT[start:end].encode("utf-8")).hexdigest(), BLOCK_SHA256)
        before = CONTRACT[:start]
        self.assertTrue(before.endswith("re-admission rules permit it.\n\n"), before[-80:])
        self.assertGreater(start, CONTRACT.index("P2c-3f Request discipline "))
        self.assertTrue(CONTRACT[end:].startswith("\nP2c-4 Local status. "))
        self.assertEqual(CONTRACT.count("<<<P2c-3h"), 1)

    def test_the_case_names_are_the_shared_ones_and_each_has_a_test(self):
        self.assertEqual(CONFORMANCE["parityP2c3hCases"], STUCK_CASES)
        source = Path(__file__).read_text(encoding="utf-8")
        for index, name in enumerate(STUCK_CASES, start=1):
            self.assertTrue(name.startswith(f"STK-{index:02d} "), name)
            self.assertEqual(source.count(f"    def test_stk_{index:02d}_"), 1, name)

    def test_the_contract_hash_is_the_pinned_one(self):
        digest = hashlib.sha256((ROOT / "docs" / "delivery-lifecycle-contract-v1.md").read_bytes()).hexdigest()
        self.assertEqual(CONFORMANCE["contractSha256"], digest)
        self.assertIn("P2c", CONFORMANCE["parityContractSectionSha256"])

    def test_the_bounds_are_the_contract_ones(self):
        self.assertEqual(adapter.STUCK_AGE_SECONDS, 24 * 3600.0)
        self.assertEqual(recovery.STUCK_TRIGGERS,
                         ("epoch_closed", "cycle_terminal", "cycle_superseded", "age", "hole_epoch_closed"))


class StuckTriggerTests(unittest.TestCase):
    def assert_stuck(self, harness, before, expected, ack):
        binding = harness.binding
        self.assertEqual(binding.acknowledged_cursor, ack)
        self.assertEqual(harness.api.acks[-1:], [ack])
        reasons = {int(seq): mark["reason"] for seq, mark in binding.recovery_closures.items()}
        self.assertEqual(reasons, expected)
        for reason in reasons.values():
            self.assertRegex(reason, REASON)
        # Intent and event bytes unchanged; nothing replayed, nothing deleted.
        self.assertEqual(binding.delivery_intents, before)
        harness.instance._dispatch_room_event.assert_not_called()
        records = harness.stuck_records()
        self.assertEqual({record["seq"]: record["reason"] for record in records}, expected)
        for record in records:
            self.assertEqual((record["verb"], record["operator"], record["membership"]), ("stuck", "connector", MEMBER))
        # A stuck record is never read as an operator closure.
        self.assertEqual(recovery.read_records(recovery.journal_path(harness.state_file)), [])
        entry = harness.status_entry()
        self.assertEqual(entry["stuckSeqs"], sorted(expected))
        self.assertEqual({int(k): v for k, v in entry["stuckReasons"].items()}, expected)
        self.assertEqual(entry["stuckCount"], len(expected))
        self.assertEqual(entry["executionFailure"], {"layer": "runtime", "code": "events_stuck",
                                                     "count": len(expected), "lastSeq": max(expected)})

    def test_stk_01_epoch_closed_frees_exactly_the_trace_seqs_2654_and_2662(self):
        harness, before = run_case(zurie)
        self.assert_stuck(harness, before, {2654: "stuck:epoch_closed:none", 2662: "stuck:epoch_closed:attempt_timeout"},
                          2704)
        binding = harness.binding
        # Wrong-seq guard: 2705 (open epoch 22, quarantined 1 h) still holds the frontier.
        self.assertEqual(binding.inbox.get("2705"), "quarantined")
        record = next(item for item in harness.stuck_records() if item["seq"] == 2654)
        self.assertEqual((record["eventId"], record["eventType"], record["eventEpochId"], record["evidenceRef"]),
                         ("ev2654", "discussion.cycle_attempt_ready", "epoch-21", "epoch:epoch-21"))
        self.assertEqual(record["priorState"]["kind"], "quarantine")

    def test_stk_02_cycle_terminal_in_an_open_epoch(self):
        def factory():
            binding = persisted_binding(2799, {2800: "quarantined"}, [2801, 2802],
                                        intents={"ev2800": quarantined_intent(2800, "attempt_timeout")},
                                        pending_since={2800: NOW - 2 * HOUR})
            return binding, StuckRoom([attempt_ready(2800, epoch="epoch-22")], cycle_state="interrupted")
        harness, before = run_case(factory)
        self.assert_stuck(harness, before, {2800: "stuck:cycle_terminal:attempt_timeout"}, 2802)
        record = harness.stuck_records()[0]
        self.assertEqual((record["evidenceRef"], record["proof"]["cycleState"]), (f"cycle:{CYCLE}", "interrupted"))

    def test_stk_03_cycle_superseded_in_an_open_epoch(self):
        def factory():
            binding = persisted_binding(2799, {2800: "quarantined"}, [2801],
                                        intents={"ev2800": quarantined_intent(2800, "cycle_superseded")},
                                        pending_since={2800: NOW - 2 * HOUR})
            return binding, StuckRoom([attempt_ready(2800, generation=1, epoch="epoch-22")],
                                      cycle_state="running", cycle_generation=3)
        harness, before = run_case(factory)
        self.assert_stuck(harness, before, {2800: "stuck:cycle_superseded:cycle_superseded"}, 2801)
        # Another member's attempt never proves a cycle trigger for this member.
        def other():
            binding = persisted_binding(2799, {2800: "quarantined"}, [2801], pending_since={2800: NOW - 2 * HOUR})
            return binding, StuckRoom([attempt_ready(2800, generation=1, member="member-2", epoch="epoch-22")],
                                      cycle_state="running", cycle_generation=3)
        harness, _ = run_case(other)
        self.assertEqual((harness.binding.acknowledged_cursor, harness.binding.recovery_closures), (2799, {}))

    def test_stk_04_age_frees_exactly_the_trace_seqs_2933_2987_3223(self):
        for name, factory, seq in (("Real", real, 2933), ("Wu", wu, 2987), ("Berlin", berlin, 3223)):
            with self.subTest(name):
                harness, before = run_case(factory)
                self.assert_stuck(harness, before, {seq: "stuck:age:none"}, seq + 6)
                # Wrong-seq guard: the later quarantine (2 h old) still holds the frontier.
                self.assertEqual(harness.binding.inbox.get(str(seq + 7)), "quarantined")
                record = harness.stuck_records()[0]
                self.assertEqual((record["eventId"], record["evidenceRef"], record["priorState"]["kind"]),
                                 (f"ev{seq}", "age:24h", "quarantine"))

    def test_stk_05_hole_epoch_closed_persists_the_room_proof_bytes(self):
        def factory():
            binding = persisted_binding(2660, {}, [2662, 2663])
            return binding, StuckRoom([attempt_ready(2661)])
        harness, before = run_case(factory)
        self.assert_stuck(harness, before, {2661: "stuck:hole_epoch_closed:none"}, 2663)
        record = harness.stuck_records()[0]
        self.assertEqual(record["priorState"]["kind"], "hole")
        proof_bytes = json.loads(record["proofBytes"])
        self.assertEqual(proof_bytes["epoch"], {"id": "epoch-21", "startsAtSeq": 2600, "status": "closed"})
        self.assertEqual(proof_bytes["activeEpoch"]["id"], "epoch-22")
        self.assertEqual(record["proofSha256"], hashlib.sha256(record["proofBytes"].encode("utf-8")).hexdigest())

    def test_stk_06_inflight_attempt_is_never_stuck(self):
        harness_binding, api = zurie()
        harness = Harness(harness_binding, api, tempfile.mkdtemp())
        harness.instance._inflight_events.add("ev2654")
        harness.instance._event_seq = {"room-1": {"ev2654": 2654}}
        harness.instance._active_dispatch_rooms = {"room-1": "gen-1"}
        harness.instance._event_dispatch_generation = {"ev2654": "gen-1"}
        harness.scan()
        self.assertEqual((harness_binding.acknowledged_cursor, harness_binding.recovery_closures, api.acks), (2653, {}, []))
        self.assertEqual(harness.stuck_records(), [])

    def test_stk_07_retry_pending_blocks_age(self):
        def factory():
            binding, api = real()
            binding.delivery_intents["ev2933"] = quarantined_intent(
                2933, "provider_error", state="failed-retryable", delivery_state="delivery_pending")
            return binding, api
        harness, _ = run_case(factory)
        self.assertEqual((harness.binding.acknowledged_cursor, harness.binding.recovery_closures), (2932, {}))
        self.assertEqual(harness.binding.inbox["2933"], "quarantined")

    def test_stk_08_under_24h_holds_and_the_proof_is_not_reread_every_scan(self):
        since = NOW - 23 * HOUR
        def factory():
            binding, api = real()
            binding.pending_since["2933"] = since
            return binding, api
        harness, _ = run_case(factory, scans=(NOW, NOW + 10, NOW + 20))
        self.assertEqual((harness.binding.acknowledged_cursor, harness.binding.recovery_closures), (2932, {}))
        self.assertEqual(harness.api.calls.count("epochs"), 1, "one proof read within the recheck interval")
        # At exactly 24 h the age trigger applies.
        harness.scan(since + 24 * HOUR)
        self.assertEqual(harness.binding.acknowledged_cursor, 2939)
        self.assertEqual(harness.binding.recovery_closures["2933"]["reason"], "stuck:age:none")

    def test_stk_09_canonical_receipt_is_never_stuck(self):
        def factory():
            binding, api = zurie()
            binding.delivery_intents["ev2654"] = quarantined_intent(
                2654, "provider_error", canonical_event={"id": "evc", "seq": 2690, "ts": "2026-11-21T09:01:00Z"})
            return binding, api
        harness, _ = run_case(factory)
        self.assertEqual((harness.binding.acknowledged_cursor, harness.binding.recovery_closures), (2653, {}))

    def test_stk_10_open_epoch_hole_still_holds_the_frontier(self):
        def factory():
            binding = persisted_binding(2932, {}, [2934, 2935])
            return binding, StuckRoom([posted(2933)])
        # Even days later: a hole has no age trigger.
        harness, _ = run_case(factory, scans=(NOW, NOW + 72 * HOUR))
        self.assertEqual((harness.binding.acknowledged_cursor, harness.binding.recovery_closures,
                          harness.api.acks), (2932, {}, []))
        self.assertEqual(harness.stuck_records(), [])

    def test_stk_11_reason_grammar_is_closed(self):
        self.assertEqual(recovery.stuck_reason("epoch_closed", ""), "stuck:epoch_closed:none")
        self.assertEqual(recovery.stuck_reason("age", "attempt_timeout"), "stuck:age:attempt_timeout")
        self.assertEqual(recovery.stuck_reason("cycle_terminal", "Provider-Error.5xx"), "stuck:cycle_terminal:provider_error_5xx")
        self.assertEqual(recovery.stuck_reason("hole_epoch_closed", "anything"), "stuck:hole_epoch_closed:none")
        with self.assertRaises(ValueError):
            recovery.stuck_reason("operator", "x")
        for trigger in recovery.STUCK_TRIGGERS:
            self.assertRegex(recovery.stuck_reason(trigger, "code_1"), REASON)
            self.assertTrue(recovery.valid_stuck_reason(recovery.stuck_reason(trigger, "code_1")))
        for bad in ("stuck", "stuck:age", "stuck:age:", "stuck:other:none", "stuck:age:Bad Code",
                    "recovery_closure:epoch_closed", "stuck:age:none:extra"):
            with self.subTest(bad):
                self.assertFalse(recovery.valid_stuck_reason(bad))
                self.assertFalse(adapter.SyntheticSocialityAdapter._terminal_evidence_valid(
                    "stuck", {"status": "stuck", "reason": bad}))
        self.assertTrue(adapter.SyntheticSocialityAdapter._terminal_evidence_valid(
            "stuck", {"status": "stuck", "reason": "stuck:age:none"}))

    def test_stk_12_terminal_set_parity(self):
        terminal = recovery.TERMINAL_EVENT_STATES
        self.assertIs(adapter.TERMINAL_EVENT_STATES, terminal)
        self.assertEqual(terminal, frozenset({"posted", "skipped", "failed", "cancelled", "superseded", "ignored", "stuck"}))
        # Renewal quiescence and the owner-control fence (which uses it) accept every terminal state.
        for status in terminal:
            with self.subTest(status):
                binding = adapter.RoomBinding("https://room.example/api", "room-1", MEMBER, "c", installation_id="i")
                binding.inbox = {"12": status}
                self.assertTrue(renewal.quiescent(binding))
                self.assertFalse(adapter.SyntheticSocialityAdapter._control_work_in_progress([], [
                    adapter.PluginState(bindings=[binding])]))
                reason = "stuck:age:none" if status == "stuck" else "r"
                evidence = {"status": status, "reason": reason, "canonicalEventId": "e", "canonicalSeq": 1,
                            "canonicalTs": "2026-11-25T00:00:00Z"}
                self.assertTrue(adapter.SyntheticSocialityAdapter._terminal_evidence_valid(status, evidence))
        for status in ("pending", "failed-retryable", "received"):
            binding = adapter.RoomBinding("https://room.example/api", "room-1", MEMBER, "c", installation_id="i")
            binding.inbox = {"12": status}
            self.assertFalse(renewal.quiescent(binding))
        # No second literal copy of the set in the connector's terminal checks.
        for name in ("adapter.py", "renewal.py"):
            source = (ROOT / name).read_text(encoding="utf-8")
            self.assertNotRegex(source, r'\{"posted", "skipped", "failed"', name)
        # Frontier walk: the read frontier passes stuck evidence like any terminal evidence.
        binding = adapter.RoomBinding("https://room.example/api", "room-1", MEMBER, "c", installation_id="i",
                                      cursor=10, acknowledged_cursor=10)
        binding.inbox = {"11": "quarantined", "12": "stuck", "13": "pending"}
        binding.terminal_evidence = {"12": {"status": "stuck", "reason": "stuck:age:none"}}
        instance = object.__new__(adapter.SyntheticSocialityAdapter)
        self.assertEqual(instance._read_frontier(binding), 12)
        # Retention: a stuck event's quarantined intent is kept as evidence, never pruned.
        binding.delivery_intents = {"q": quarantined_intent(12)}
        binding.delivery_intents.update({f"t{n}": {"state": "skipped", "delivery_state": "skipped"} for n in range(40)})
        recovery.prune_terminal_intents(binding)
        self.assertIn("q", binding.delivery_intents)

    def test_a_quarantine_records_its_time_for_the_age_trigger(self):
        binding = adapter.RoomBinding("https://room.example/api", "room-1", MEMBER, "c", installation_id="i",
                                      cursor=10, acknowledged_cursor=10)
        binding.turn_sequences = {"ev11": 11}
        binding.pending_retries = {"11": adapter.PENDING_EVENT_MAX_RETRIES}
        instance = object.__new__(adapter.SyntheticSocialityAdapter)
        instance._persist_binding = lambda _current: True
        with mock.patch.object(adapter.time, "time", lambda: NOW):
            instance._record_delivery_failure(binding, "ev11", "refused", False, "provider_error")
        self.assertEqual((binding.inbox["11"], binding.pending_since["11"]), ("quarantined", NOW))
        # A quarantine without a recorded time is stamped on its first scan, never freed at once.
        def factory():
            binding, api = real()
            binding.pending_since.pop("2933")
            return binding, api
        harness, _ = run_case(factory)
        self.assertEqual((harness.binding.acknowledged_cursor, harness.binding.pending_since["2933"]), (2932, NOW))
        harness.scan(NOW + 24 * HOUR)
        # 2933 (stamped at NOW) and the later 2940 (quarantined 26 h by then) both age out.
        self.assertEqual({seq: mark["reason"] for seq, mark in harness.binding.recovery_closures.items()},
                         {"2933": "stuck:age:none", "2940": "stuck:age:none"})
        self.assertEqual(harness.binding.acknowledged_cursor, 2941)


if __name__ == "__main__":
    unittest.main()
