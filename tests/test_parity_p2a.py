"""Shared conformance cases for parity release P2a.

Delivery contract section "P2a — Equal access after missed turns"; case names
are the keys of ``conformance.json`` ``parityP2aCases``; OpenClaw implements
the same names.
"""

from __future__ import annotations

import asyncio
import json
import logging
import tempfile
import time
import types
import unittest
from pathlib import Path
from unittest import mock

from test_delivery_lifecycle import ROOT, adapter
from test_parity_p1 import cycle_attempt, lease_in, room_instance, source_ref
from test_parity_p1a import DENY_PATTERN, attempt_ready, build_message, cycle

CONFORMANCE = json.loads((ROOT / "conformance.json").read_text())
KEY = "agent:main:synthetic_sociality:group:room-1"
ROSTER = [{"membershipId": "owner-1", "displayName": "TJE"},
          {"membershipId": "member-1", "displayName": "Zurie"},
          {"membershipId": "paula-1", "displayName": "Paula"}]
START = {"id": "start-z", "seq": 2900, "type": "discussion.started", "actorRole": "human_owner", "actorId": "owner-1",
         "payload": {"epoch": {"id": "epoch-2", "ordinal": 2, "startsAtSeq": 2900, "status": "active",
                               "topic": {"title": "Coding", "epochId": "epoch-2"}}}}


def message(seq, actor_id, role, body, **extra):
    return {"id": f"msg-{seq}", "seq": seq, "ts": f"2026-10-02T09:49:{seq % 60:02d}Z", "type": "message.posted",
            "actorRole": role, "actorId": actor_id, "payload": {"body": body, **extra}}


def consume_instance(*, mode="coordinated", start=None):
    """A connector consuming Room events for member-1 against a fake Room."""
    binding = adapter.RoomBinding("https://room.example/api", "room-1", "member-1", "credential",
                                  cursor=2963, acknowledged_cursor=2963)
    instance = object.__new__(adapter.SyntheticSocialityAdapter)
    for name, value in {
        "_inflight_events": set(), "_event_seq": {}, "_event_epoch": {}, "_latest_source": {},
        "_run_for_event": {}, "_activity_seq": {}, "_context_acknowledged_sources": set(),
        "_cycle_attempts": {}, "_cycle_response_sources": {}, "_source_coordination_modes": {},
        "_open_reply_recipients": {}, "_queued_events": {}, "_active_dispatch_rooms": {},
        "_event_dispatch_generation": {}, "_receive_locks": {}, "_ledger_locks": {},
        "_terminal_sources": {}, "_terminal_results": {}, "_attempt_renewal_tasks": {},
    }.items():
        setattr(instance, name, value)
    persisted = []
    instance._persist_binding = lambda current: persisted.append(json.loads(json.dumps(current.missed_sources))) or True
    instance._state = types.SimpleNamespace(binding=lambda _room: binding)
    starts, completed = [], []

    class API:
        def room_state(self, _room):
            return {"headSeq": 2990, "activeEpoch": {"id": "epoch-2", "startsAtSeq": 2900}, "roster": [
                {"membershipId": member, "displayName": member, "role": "participant_agent", "status": "active"}
                for member in ("member-1", "paula-1")
            ]}

        def room_policy(self, _room):
            return {"coordinationMode": mode, "summaryCoordinatorMembershipId": "paula-1"}

        def start_discussion_cycle(self, _room, request):
            starts.append(request)
            if start is not None:
                return start(request)
            return {"id": "cycle-new"}

        def claim_discussion_attempt(self, _room, _cycle):
            raise adapter.ProtocolError("no attempt", status=409, code="cycle_no_attempt")

        def activity(self, *_args):
            return {"accepted": True}

        def acknowledge_peer_contribution(self, *_args):
            return {"accepted": True}

    instance._call = lambda _binding, operation: asyncio.sleep(0, result=operation(API()))
    instance._complete_event = lambda *_args, **kwargs: asyncio.sleep(0, result=completed.append(kwargs))
    instance._dispatch_next_queued = lambda _binding: asyncio.sleep(0)
    return instance, binding, starts, completed, persisted


class P2aConformanceTests(unittest.TestCase):
    def test_case_names_resolve_to_tests(self):
        cases = CONFORMANCE["parityP2aCases"]
        self.assertEqual(len(cases), 6)
        sources = "\n".join(path.read_text() for path in (ROOT / "tests").glob("test_*.py"))
        for name, method in cases.items():
            self.assertTrue(name.startswith("P2a-0"), name)
            self.assertEqual(sources.count(f"def {method}(self"), 1, method)


class P2aTests(unittest.TestCase):
    def test_p2a_01_a_host_wait_the_connector_cannot_observe_is_bounded_and_falls_back(self):
        # Mac128 core 7a1aafb4e1 has no _session_task_is_stale / _session_tasks:
        # a stale _active_sessions entry kept Real silent for hours. The wait is
        # now bounded, then the queue proceeds as without the gate, logged once.
        async def run(observable):
            instance, binding, _api, _published, dispatched = room_instance(None, queued=(6,))
            instance._active_dispatch_rooms = {}
            instance._active_sessions = {KEY: asyncio.Event()}
            instance._room_session_keys = {"room-1": KEY}
            if observable:
                instance._session_tasks = {}
                instance._session_task_is_stale = lambda _key: False  # the run is genuinely active
            for name in ("_session_task_is_stale", "_session_tasks", "_track_session_task"):
                if not observable:
                    self.assertFalse(hasattr(instance, name), name)
            started = time.monotonic()
            await instance._dispatch_next_queued(binding)
            for _ in range(60):
                if dispatched:
                    break
                await asyncio.sleep(0.05)
            return dispatched, time.monotonic() - started

        with mock.patch.object(adapter, "SESSION_UNOBSERVABLE_WAIT_SECONDS", 0.3), \
                mock.patch.object(adapter, "SESSION_RELEASE_POLL_SECONDS", 0.05), \
                self.assertLogs(adapter.logger, level="WARNING") as logs:
            dispatched, elapsed = asyncio.run(run(observable=False))
            adapter.logger.warning("end of P2a-01 case")
        self.assertEqual(dispatched, ["evt-6"])
        self.assertGreaterEqual(elapsed, 0.3)
        fallback = [line for line in logs.output if "cannot report session release" in line]
        self.assertEqual(len(fallback), 1, logs.output)
        # A core that can report the release keeps the gate.
        with mock.patch.object(adapter, "SESSION_UNOBSERVABLE_WAIT_SECONDS", 0.3), \
                mock.patch.object(adapter, "SESSION_RELEASE_POLL_SECONDS", 0.05):
            dispatched, _elapsed = asyncio.run(run(observable=True))
        self.assertEqual(dispatched, [])

    def test_p2a_03_a_missed_source_is_offered_once_as_a_procedural_fact_and_then_cleared(self):
        # Recorded when the turn never reached the model: a refused cycle start ...
        instance, binding, _starts, completed, persisted = consume_instance(
            start=lambda _request: (_ for _ in ()).throw(
                adapter.ProtocolError("refused", status=409, code="cycle_conflict")),
        )
        mention = message(2964, "owner-1", "human_owner", "@Zurie your view?",
                          resolvedRecipientMembershipIds=["member-1"])
        asyncio.run(instance._consume(binding, mention))
        self.assertEqual(completed[-1]["reason"], "cycle_conflict")
        self.assertEqual(binding.missed_sources, [{"source_event_id": "msg-2964", "source_seq": 2964}])
        self.assertEqual(persisted[-1], binding.missed_sources)
        # ... and an unclaimed attempt (cycle_no_attempt), recorded by its source.
        unclaimed, unclaimed_binding, _s, done, _p = consume_instance()
        asyncio.run(unclaimed._consume(unclaimed_binding, {
            **attempt_ready(2966, 1, source="msg-2965"), "payload": {
                **attempt_ready(2966, 1, source="msg-2965")["payload"], "epochId": "epoch-2"}}))
        self.assertEqual(done[-1]["reason"], "discussion_attempt_unavailable")
        self.assertEqual([item["source_event_id"] for item in unclaimed_binding.missed_sources], ["msg-2965"])

        # Offered once on the next model turn, as a fact line only.
        newer = [message(seq, "paula-1" if seq % 2 else "owner-1", "participant_agent" if seq % 2 else "human_owner",
                         f"Note {seq}.") for seq in (2965, 2966, 2967, 2968)]
        paula = message(2969, "paula-1", "participant_agent", "Zurie, over to you.",
                        resolvedRecipientMembershipIds=["member-1"])
        page = (START, mention, *newer, paula)
        saved = []

        def prepare(instance, binding):
            binding.missed_sources = [{"source_event_id": "msg-2964", "source_seq": 2964}]
            instance._persist_binding = lambda current: saved.append(list(current.missed_sources)) or True
            prepared.append(binding)

        prepared = []
        text = build_message(attempt_ready(2970, 1, source="msg-2969"), attempt=cycle(used=0), roster=ROSTER,
                             context=page, starts_at=2900, prepare=prepare).text
        fact = "Missed earlier: #2964 TJE, 09:49 UTC; 5 newer messages since."
        self.assertEqual(text.count("Missed earlier"), 1, text)
        self.assertIn(f"\n{fact}\n", text)
        self.assertEqual(DENY_PATTERN.findall(fact), [])
        # Cleared durably before core runs the turn; the next turn has no offer.
        self.assertEqual(prepared[0].missed_sources, [])
        self.assertEqual(saved[-1], [])
        again = build_message(attempt_ready(2971, 1, source="msg-2969"), attempt=cycle(used=0), roster=ROSTER,
                              context=page, starts_at=2900).text
        self.assertNotIn("Missed earlier", again)

    def test_p2a_04_a_coordinated_ask_without_retry_starts_a_cycle_for_its_target(self):
        ask = {"id": "cmd-2980", "seq": 2980, "type": "human.command", "actorRole": "human_owner", "actorId": "owner-1",
               "payload": {"epochId": "epoch-2", "command": {"command": "ask", "arguments": {"instruction": "Your view?"},
                                                             "resolvedTargetMembershipIds": ["member-1"],
                                                             "targetSelectors": [{"kind": "membership", "membershipId": "member-1"}]}}}
        for mode in ("coordinated", "open"):
            with self.subTest(mode=mode):
                instance, binding, starts, completed, _persisted = consume_instance(mode=mode)
                asyncio.run(instance._consume(binding, ask))
                self.assertEqual([request["sourceEventId"] for request in starts], ["cmd-2980"])
                self.assertEqual([member["membershipId"] for member in starts[0]["roster"]], ["member-1"])
                self.assertEqual(completed[-1]["reason"], "cycle_source_context_only")
        # An ask for another agent starts nothing here.
        other = json.loads(json.dumps(ask))
        other["payload"]["command"]["resolvedTargetMembershipIds"] = ["paula-1"]
        instance, binding, starts, completed, _persisted = consume_instance(mode="open")
        asyncio.run(instance._consume(binding, other))
        self.assertEqual(starts, [])

    def test_p2a_05_the_state_directory_is_never_the_filesystem_root(self):
        state = __import__(adapter.__name__.rsplit(".", 1)[0] + ".state", fromlist=["state"])
        # Only the pure check: no root-level path reaches the filesystem.
        for directory in ("/", "//", "/unused", "/tmp", "/private/", "/a/..", "/a/../b", "/./state"):
            with self.subTest(directory=directory), self.assertRaises(ValueError):
                state.assert_state_directory(Path(directory))
        for directory in ("/a/b", "/tmp/hermes-room", "/root/.hermes/synthetic-sociality-room"):
            self.assertEqual(str(state.assert_state_directory(Path(directory))), directory)
        with tempfile.TemporaryDirectory() as scratch:
            target = Path(scratch) / "profile" / "state.json"
            state.save(state.PluginState(), target)
            self.assertEqual(state.load(target).bindings, [])

    def test_p2a_06_a_transient_renewal_failure_keeps_a_turn_with_a_valid_lease(self):
        # Live sequence (OpenClaw Zurie, 2026-10-02 07:48): claim ok, renewal
        # ok, the next renewal fails without reaching the Room, the lease still
        # valid. Now: one log line (no traceback, no body), retried, renewed;
        # the pre-post re-claim follows the same rule; the reply is posted.
        async def run():
            attempt = cycle_attempt(lease=lease_in(3))
            instance, binding, api, _published, _dispatched = room_instance(attempt)
            instance.ATTEMPT_RENEWAL_RETRY_SECONDS = 0.05
            outcomes = ["ok", "transient", "ok", "transient", "ok", "ok", "ok", "ok"]

            def claim(_n):
                outcome = outcomes.pop(0) if outcomes else "ok"
                if outcome == "transient":
                    raise adapter.ProtocolError("Room API unavailable: other side closed", retryable=True)
                return cycle_attempt(lease=lease_in(3))

            api.claim = claim
            instance._start_attempt_renewal(binding, "evt-5", attempt)
            for _ in range(80):
                if api.claims >= 3:
                    break
                await asyncio.sleep(0.05)
            renewals = api.claims
            key = adapter._attempt_authority_key(binding, "evt-5", attempt)
            state_after_renewals = binding.delivery_authority[key]["state"]
            superseded = "evt-5" in instance._superseded_sources
            result = await instance._send_final("room-1", source_ref(), "A considered answer.")
            return instance, api, renewals, state_after_renewals, superseded, result

        with self.assertLogs(adapter.logger, level="WARNING") as logs:
            instance, api, renewals, authority, superseded, result = asyncio.run(run())
        self.assertGreaterEqual(renewals, 3)
        self.assertEqual(authority, "active")
        self.assertFalse(superseded)
        self.assertTrue(result.success)
        self.assertEqual(len(api.posts), 1)
        failures = [record for record in logs.records if "attempt renewal failed" in record.getMessage()]
        self.assertGreaterEqual(len(failures), 2)
        for record in failures:
            self.assertIsNone(record.exc_info)
            self.assertIn("transient; lease remaining", record.getMessage())
            self.assertNotIn("A considered answer", record.getMessage())
        # Each claim is bounded to 4 s and never past the lease.
        protocol = adapter.RoomProtocol("https://room.example/api", "credential")
        self.assertEqual(adapter._bounded_protocol(protocol, 4.0).timeout, 4.0)
        self.assertEqual(adapter.SyntheticSocialityAdapter.ATTEMPT_RENEWAL_REQUEST_SECONDS, 4.0)
        self.assertEqual(adapter.SyntheticSocialityAdapter.ATTEMPT_RENEWAL_RETRY_SECONDS, 5.0)
        # Classification: refusals are final, transient errors retried.
        self.assertTrue(adapter._is_transient_claim_failure(adapter.ProtocolError("x", retryable=True)))
        self.assertTrue(adapter._is_transient_claim_failure(adapter.ProtocolError("x", status=503, retryable=True)))
        self.assertTrue(adapter._is_transient_claim_failure(
            adapter.ProtocolError("x", code="credential_renewal_pending", retryable=True)))
        self.assertFalse(adapter._is_transient_claim_failure(adapter.ProtocolError("x", status=409, code="stale_epoch")))
        self.assertFalse(adapter._is_transient_claim_failure(adapter.ProtocolError("x", status=403)))


if __name__ == "__main__":
    unittest.main()
