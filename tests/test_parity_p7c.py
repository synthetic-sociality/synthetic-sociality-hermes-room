"""Parity P7c "Moderated debate and own voice": contract section P7c and the
P2c-3a / P2c-4 additions (conformance ``parityP7cCases``; OpenClaw implements
the same names).

Room server hold-signal-47320f7 (deployed 2026-10-05): moderated interruption
(ADR 0008 addendum 2026-10-04), moderator steering, the hold signal, and the
ISSUE-023 follow-ups. No test uses a real credential or talks to a live Room.
"""

from __future__ import annotations

import asyncio
import contextlib
import copy
import json
import socket
import struct
import sys
import tempfile
import threading
import time
import types
import unittest
from pathlib import Path
from unittest import mock

from test_delivery_lifecycle import ROOT, adapter
from test_parity_p1 import cycle_attempt, refusal, room_instance, source_ref
from test_parity_p1a import (CODING_OWNER_BODY, CODING_ROSTER, CODING_START, HUMAN, PEER, START, attempt_ready,
                             build_message, coding_message, command, cycle, every_prompt)
from test_parity_p7a import REAL_SLEEP, FakeRoom, Waits, edge_error, http_error, lost_session, run_watch, watched

CONFORMANCE = json.loads((ROOT / "conformance.json").read_text())
local_status = adapter.local_status
protocol = sys.modules[adapter.__name__.rsplit(".", 1)[0] + ".protocol"]
context_module = sys.modules[adapter.__name__.rsplit(".", 1)[0] + ".context"]
SENTENCE = CONFORMANCE["parityP7cNoStatusSentence"]
# The Room server's instruction for a response turn, verbatim (hold-signal-47320f7
# server/application/cycles/moderation.go:27-30).
# A bound against a hang only: every wait below ends on an event or a condition,
# none of them on the clock (P7c-10, P7c-16, N10).
HANG_GUARD = 120.0
RESPONSE_INSTRUCTION = (
    "The moderator has intervened; the message this turn answers is the moderator's. Respond to it first: "
    "acknowledge it briefly, then answer it. If the moderator's point is unclear to you, ask one short clarifying "
    "question instead of guessing. The discussion then continues in the direction the moderator set."
)
INTERJECTION = {"id": "m-50", "seq": 50, "type": "message.posted", "actorRole": "human_owner", "actorId": "owner-1",
                "payload": {"body": "Please also consider the family's view."}}
ADDRESSED = {**INTERJECTION, "payload": {**INTERJECTION["payload"], "resolvedRecipientMembershipIds": ["member-1"]}}
OWN = {"id": "m-44", "seq": 44, "type": "message.posted", "actorRole": "participant_agent", "actorId": "member-1",
       "payload": {"body": "My own earlier point.", "respondsTo": ["m-43"]}}
ASK = {"id": "cmd-45", "seq": 45, "type": "human.command", "actorRole": "human_owner", "actorId": "owner-1",
       "payload": {"command": {"command": "ask", "arguments": {"instruction": "Your view?"}}, "visibleText": "/ask"}}


def response_ready(seq, source="m-50", **extra):
    return attempt_ready(seq, 2, source=source, phase="response", phaseInstruction=RESPONSE_INSTRUCTION, **extra)


class RecordingRoom:
    """Wraps the P1 FakeRoom to keep each post's body and reply target."""

    def __init__(self, api):
        self.api = api
        self.bodies, self.standalone, self.responds_to = [], [], []
        original = api.post_message

        def post_message(room_id, turn_id, observed, source_event_id, body, *args, **kwargs):
            self.bodies.append(body)
            self.standalone.append(bool(kwargs.get("standalone")))
            self.responds_to.append(kwargs.get("responds_to_id", ""))
            return original(room_id, turn_id, observed, source_event_id, body, *args, **kwargs)

        api.post_message = post_message


def receive_instance(*, state=None, claim=None, policy=None, status_path=None, busy=False):
    """A connector receiving Room events for member-1 (the P2a harness, with a status file)."""
    binding = adapter.RoomBinding("https://room.example/api", "room-1", "member-1", "credential",
                                  cursor=40, acknowledged_cursor=40)
    instance = object.__new__(adapter.SyntheticSocialityAdapter)
    for name, value in {
        "_inflight_events": set(), "_event_seq": {}, "_event_epoch": {}, "_latest_source": {},
        "_run_for_event": {}, "_activity_seq": {}, "_context_acknowledged_sources": set(),
        "_cycle_attempts": {}, "_cycle_response_sources": {}, "_source_coordination_modes": {},
        "_open_reply_recipients": {}, "_queued_events": {}, "_active_dispatch_rooms": {"room-1": "busy"} if busy else {},
        "_event_dispatch_generation": {}, "_receive_locks": {}, "_ledger_locks": {},
        "_terminal_sources": {}, "_terminal_results": {}, "_attempt_renewal_tasks": {},
    }.items():
        setattr(instance, name, value)
    instance._persist_binding = lambda _current: True
    instance._state = types.SimpleNamespace(binding=lambda _room: binding)
    instance._status = None if status_path is None else local_status.LocalStatus(
        status_path, plugin="hermes", version=adapter.CONNECTOR_VERSION, host_label="host")
    record = types.SimpleNamespace(starts=[], claims=[], completed=[], dispatched=[])
    room_state = {"headSeq": 60, "activeEpoch": {"id": "epoch-2", "startsAtSeq": 41}, "activeHold": None,
                  "roster": [{"membershipId": member, "displayName": member, "role": "participant_agent",
                              "status": "active"} for member in ("member-1", "peer-1")], **(state or {})}

    class API:
        def room_state(self, _room):
            return room_state

        def room_policy(self, _room):
            return {"coordinationMode": "coordinated", **(policy or {})}

        def start_discussion_cycle(self, _room, request):
            record.starts.append(request)
            return {"id": "cycle-active", "sourceEventId": "m-42"}

        def claim_discussion_attempt(self, _room, cycle_id):
            record.claims.append(cycle_id)
            answer = claim(len(record.claims)) if claim else None
            if answer is None:
                raise adapter.ProtocolError("no attempt", status=409, code="cycle_no_attempt", retryable=True)
            return answer

        def activity(self, *_args):
            return {"accepted": True}

        def acknowledge_peer_contribution(self, *_args):
            return {"accepted": True}

    instance._call = lambda _binding, operation: asyncio.sleep(0, result=operation(API()))
    instance._complete_event = lambda *_args, **kwargs: asyncio.sleep(0, result=record.completed.append(kwargs))

    async def dispatch_next(current):
        for seq in sorted(instance._queued_events.get(current.room_id, {})):
            if busy:
                return
            record.dispatched.append(instance._queued_events[current.room_id].pop(seq)["id"])

    instance._dispatch_next_queued = dispatch_next
    instance._start_attempt_renewal = lambda *_args: None
    return instance, binding, record, room_state


def claimed_attempt(attempt_id="attempt-1"):
    return {"cycle": {"id": "cycle-1", "generation": 1, "totalTurns": 2, "sourceEventId": "m-42",
                      "budgets": {"totalTurns": 6, "perAgentTurns": 3}},
            "attempt": {"id": attempt_id, "membershipId": "member-1", "round": 2,
                        "leaseExpiresAt": "2099-01-01T00:00:00Z"}}


class P7cConformanceTests(unittest.TestCase):
    def test_case_names_resolve_to_tests(self):
        cases = CONFORMANCE["parityP7cCases"]
        self.assertEqual(len(cases), 23)
        sources = "\n".join(path.read_text() for path in (ROOT / "tests").glob("test_*.py"))
        for index, (name, method) in enumerate(cases.items(), start=1):
            self.assertTrue(name.startswith(f"P7c-{index:02d} "), name)
            self.assertEqual(sources.count(f"def {method}(self"), 1, method)
        self.assertEqual(CONFORMANCE["parityP7cContract"],
                         "docs/delivery-lifecycle-contract-v1.md#p7c--moderated-debate-and-own-voice-2026-10-05")

    def test_the_shared_texts_are_the_connector_constants(self):
        self.assertEqual(adapter.ROOM_NO_SYSTEM_STATUS_INSTRUCTION, SENTENCE)
        bound = CONFORMANCE["parityP7cWordBound"]
        self.assertEqual((adapter.WORD_BOUND_CYCLE_SOURCE, adapter.WORD_BOUND_POLICY_SOURCE),
                         (bound["cycle"], bound["policy"]))
        self.assertEqual(adapter._word_bound_fact(90, bound["policy"]),
                         bound["format"].replace("{N}", "90").replace("{source}", bound["policy"]))
        self.assertIn("held_by_moderator", local_status.GATE_WAIT_REASONS)


class P7cTests(unittest.TestCase):
    def test_p7c_01_a_reply_in_work_is_posted_when_a_human_posts(self):
        # The Room accepted the human message as an interjection: POST /cycles
        # answers the active cycle; the source is context only, nothing claimed.
        instance, binding, record, _state = receive_instance()
        asyncio.run(instance._consume(binding, INTERJECTION))
        self.assertEqual([request["sourceEventId"] for request in record.starts], ["m-50"])
        self.assertEqual(record.claims, [])
        self.assertEqual(record.completed[-1]["reason"], "cycle_source_context_only")

        # The reply in work is posted; the Room's receipt (with
        # writtenBeforeEventId on the canonical event) is an ordinary success.
        async def run():
            instance, binding, api, published, _dispatched = room_instance(cycle_attempt())
            recording = RecordingRoom(api)
            original = api.post_message

            def accepted(*args, **kwargs):
                event = original(*args, **kwargs)
                return {**event, "payload": {"writtenBeforeEventId": "m-50"}}

            api.post_message = accepted
            result = await instance._send_final("room-1", source_ref(), "My point, written before the question.")
            return instance, binding, api, recording, published, result

        instance, binding, api, recording, published, result = asyncio.run(run())
        self.assertTrue(result.success)
        self.assertEqual(recording.bodies, ["My point, written before the question."])
        self.assertEqual([payload["action"] for _id, payload in api.completions], ["contribute"])
        self.assertIsNone(getattr(binding, "kept_reply", None) or None)

    def test_p7c_02_a_response_turn_shows_the_human_message_and_the_rooms_instruction(self):
        prompts = {}
        for label, message in (("for all", INTERJECTION), ("addressed", ADDRESSED)):
            prompts[label] = build_message(response_ready(51), attempt=claimed_attempt(),
                                           context=(START, HUMAN, PEER, message)).text
        for label, prompt in prompts.items():
            with self.subTest(label):
                self.assertIn("\n\n#50 Owner: Please also consider the family's view.\n", prompt)
                self.assertIn(f"[Room phase instruction — server-provided, verbatim]\n{RESPONSE_INSTRUCTION}\n"
                              "[/Room phase instruction]", prompt)
                self.assertIn("[Autonomous discussion phase response, round 2,", prompt)
                # No wording of the connector's own about the intervention.
                self.assertEqual(prompt.count("moderator"), RESPONSE_INSTRUCTION.count("moderator"))
                self.assertEqual(prompt.count(RESPONSE_INSTRUCTION), 1)
        self.assertEqual(prompts["for all"], prompts["addressed"])

    def test_p7c_03_a_regular_turn_after_the_round_continues_with_the_remaining_budget(self):
        attempt = claimed_attempt()
        attempt["cycle"]["totalTurns"] = 3
        prompt = build_message(attempt_ready(53, 2, source="m-50"), attempt=attempt,
                               context=(START, HUMAN, PEER, INTERJECTION)).text
        self.assertIn("\n\n#50 Owner: Please also consider the family's view.\n", prompt)
        self.assertIn("[Autonomous discussion phase follow_up, round 2, turn 4/6]", prompt)
        self.assertNotIn("[Room phase instruction", prompt)

    def test_p7c_04_a_response_turn_announced_during_a_run_is_answered_after_it(self):
        # Hermes reads events at once and queues the model work behind the
        # run in progress: the response turn is claimed and waits in the queue.
        instance, binding, record, _state = receive_instance(claim=lambda _n: claimed_attempt(), busy=True)
        asyncio.run(instance._consume(binding, response_ready(51)))
        self.assertEqual(record.claims, ["cycle-1"])
        self.assertEqual(record.dispatched, [])
        self.assertEqual([event["id"] for event in instance._queued_events["room-1"].values()], ["ready-51"])
        self.assertEqual(instance._cycle_response_sources["ready-51"], "m-50")
        # Not dropped and not superseded: it runs when the Room's slot is free.
        self.assertFalse([item for item in record.completed if item.get("source_id") == "ready-51"])

    def test_p7c_05_with_steering_off_the_refused_reply_is_kept_and_the_message_seeds_a_cycle(self):
        async def run():
            instance, binding, api, published, _dispatched = room_instance(cycle_attempt())
            api.post_error = lambda _n: refusal("cycle_superseded")
            result = await instance._send_final("room-1", source_ref(), "Written before the moderator spoke.")
            return instance, binding, api, result

        instance, binding, api, result = asyncio.run(run())
        self.assertTrue(result.success)
        terminal = instance._terminal_results[source_ref()]
        self.assertEqual((terminal["status"], terminal["reason"]), ("superseded", "cycle_superseded"))
        self.assertEqual(binding.kept_reply["text"], "Written before the moderator spoke.")
        self.assertEqual(binding.kept_reply["reason"], "cycle_superseded")
        # The human message is offered to POST /cycles and seeds the next cycle.
        instance, binding, record, _state = receive_instance(policy={"moderatorSteering": False})
        asyncio.run(instance._consume(binding, INTERJECTION))
        self.assertEqual([request["sourceEventId"] for request in record.starts], ["m-50"])

    def test_p7c_06_a_turn_whose_source_is_a_command_posts_without_responds_to(self):
        cases = {}
        for label, source in (("command", "cmd-45"), ("message", "m-43")):
            # The send-time claim answers a fresh object (as the Room does).
            recording, result, decided, _binding = asyncio.run(command_source_turn(source, renewals=0))
            self.assertTrue(result.success)
            cases[label] = (decided, recording.standalone, recording.responds_to)
        self.assertEqual(cases["command"], ({"ready-52"}, [True], ["cmd-45"]))
        self.assertEqual(cases["message"], (set(), [False], ["m-43"]))
        # The wire: a standalone post carries no respondsTo.
        sent = []
        api = protocol.RoomProtocol("https://room.example/api", "credential")
        with mock.patch.object(api, "request", lambda method, path, payload=None, **_k: sent.append(payload) or {}):
            api.post_message("room-1", "", 52, "ready-52", "My answer.", "epoch-2", cycle_id="cycle-1",
                             attempt_id="attempt-1", cycle_generation=1, responds_to_id="cmd-45", standalone=True)
            api.post_message("room-1", "", 52, "ready-52", "My answer.", "epoch-2", cycle_id="cycle-1",
                             attempt_id="attempt-1", cycle_generation=1, responds_to_id="m-43")
        self.assertEqual([payload["respondsTo"] for payload in sent], [[], ["m-43"]])

    def test_p7c_07_the_transcript_marks_the_agents_own_entries(self):
        vector = CONFORMANCE["parityP7cTranscriptVector"]
        rendered = context_module.canonical_room_context(
            vector["state"], vector["events"], vector["currentEventId"], own_membership_id=vector["membershipId"])
        transcript = rendered.split("Recent canonical transcript:\n", 1)[1].rsplit("\n[/Canonical Room context]", 1)[0]
        self.assertEqual(transcript, vector["transcript"])
        # Without the own membership nothing is marked (the P1a-7 labels).
        plain = context_module.canonical_room_context(vector["state"], vector["events"], vector["currentEventId"])
        self.assertNotIn("(you)", plain)
        # A normal turn and the closing-summary turn of the Room carry the mark.
        roster = [{"membershipId": "owner-1", "displayName": "Owner"}, {"membershipId": "peer-1", "displayName": "Peer"},
                  {"membershipId": "member-1", "displayName": "Agent"}]
        summary = attempt_ready(55, 1, source="term-54", phase="summary",
                                phaseInstruction="You are Agent in this Room. Write the closing summary.")
        for label, event in (("turn", attempt_ready(55, 2, source="m-42")), ("closing summary", summary)):
            with self.subTest(label):
                text = build_message(event, attempt=claimed_attempt(), context=(START, HUMAN, PEER, OWN),
                                     roster=roster).text
                self.assertIn("#44 Agent (you) (reply to Peer): My own earlier point.", text)
                self.assertIn("#43 Peer: Disclosure first, then support.", text)
        # The Room-context tool and the history tool. Another member with the
        # agent's display name is never marked there either (by actorId only).
        tools = __import__(adapter.__name__.rsplit(".", 1)[0] + ".room_tools", fromlist=["room_tools"])
        namesake = {"id": "m-45", "seq": 45, "type": "message.posted", "actorRole": "participant_agent",
                    "actorId": "member-9", "payload": {"body": "Same name, another member."}}
        events = [START, HUMAN, PEER, OWN, namesake]
        tool_roster = [*roster, {"membershipId": "member-9", "displayName": "Agent"}]

        class Protocol:
            def __init__(self, *_args):
                pass

            def room_state(self, _room):
                return {"title": "Room", "headSeq": 45, "roster": tool_roster,
                        "activeEpoch": {"id": "epoch-2", "ordinal": 2, "startsAtSeq": 41}}

            def events(self, _room, after, *_args, **_kwargs):
                return {"events": [item for item in events if item["seq"] > after], "headSeq": 45}

            def epochs(self, _room):
                return {"epochs": [{"id": "epoch-2", "ordinal": 2, "startsAtSeq": 41, "status": "active"}]}

        binding = adapter.RoomBinding("https://room.example/api", "room-1", "member-1", "credential",
                                      installation_id="inst-1")
        tools._ROOM_SCAN_CACHE.clear()
        with mock.patch.object(tools, "RoomProtocol", Protocol), \
                mock.patch.object(tools, "load", lambda: types.SimpleNamespace(bindings=[binding])):
            # Through the handlers the plugin registers, over the saved binding.
            registration = mock.MagicMock()
            adapter.register(registration)
            handlers = {call.kwargs["name"]: call.kwargs["handler"] for call in registration.register_tool.call_args_list}
            context_result = json.loads(handlers["synthetic_sociality_room_context"]({}))
            history = json.loads(handlers["synthetic_sociality_room_history"]({}))
        self.assertIn("#44 Agent (you) (reply to Peer): My own earlier point.", context_result["context"])
        self.assertIn("\n#45 Agent: Same name, another member.", context_result["context"])
        self.assertEqual(context_result["context"].count("(you)"), 1)
        self.assertEqual({entry["seq"]: entry["actor"] for entry in history["messages"]},
                         {42: "Owner", 43: "Peer", 44: "Agent (you)", 45: "Agent"})

    def test_p7c_08_the_word_bound_wording(self):
        policy_text = build_message({**HUMAN, "id": "m-56", "seq": 56},
                                    policy={"coordinationMode": "open", "maxContributionWords": 120}).text
        self.assertIn("At most 120 words per response (Room policy maxContributionWords); there is no minimum.",
                      policy_text)
        cycle_text = build_message(attempt_ready(50, 1), attempt=cycle(maxContributionWords=80),
                                   policy={"maxContributionWords": 120}).text
        self.assertIn("At most 80 words per response (Room policy maxContributionWords, as frozen for this "
                      "discussion cycle); there is no minimum.", cycle_text)
        for value in (0, None, -5, 12.5, "100", True):
            with self.subTest(value=value):
                text = build_message({**HUMAN, "id": "m-56", "seq": 56},
                                     policy={"coordinationMode": "open", "maxContributionWords": value}).text
                self.assertNotIn("words per response", text)
        self.assertIn("At most 100 words", build_message(
            {**HUMAN, "id": "m-56", "seq": 56}, policy={"coordinationMode": "open", "maxContributionWords": 100.0}).text)
        self.assertNotIn("Word limit", policy_text + cycle_text)

    def test_p7c_09_the_no_system_status_sentence_is_in_the_frame_once(self):
        frames = {
            "without guidance": build_message(attempt_ready(50, 1), attempt=cycle()).text,
            "open message": build_message({**HUMAN, "id": "m-56", "seq": 56}).text,
            "guidance carries it": build_message(attempt_ready(50, 1), attempt=cycle(), rules=[
                {"enforcement": "guidance", "text": "Speak plainly."},
                {"enforcement": "guidance", "text": SENTENCE}]).text,
            "guidance carries it among other text": build_message(attempt_ready(50, 1), attempt=cycle(), rules=[
                {"enforcement": "guidance", "text": f"Be brief.\n\n{SENTENCE}\n\nUse plain words."}]).text,
        }
        for label, text in frames.items():
            with self.subTest(label):
                self.assertEqual(text.count(SENTENCE), 1)
        # Every frame a model can get: Hermes builds every Room turn in one
        # frame (_dispatch_room_event); every shape it renders (P1a every_prompt:
        # human reply, opening and follow-up cycle turns, the summarize command
        # with and without an attempt, an unresolved-source cycle turn, the
        # initial greeting, the open-Room message, the summary-coordinator turn)
        # ends with the sentence once, byte-exact; none adds it when the
        # guidance carries it.
        carried = [{"enforcement": "guidance", "text": SENTENCE}]
        plain, with_guidance = every_prompt(), every_prompt(rules=carried)
        self.assertGreaterEqual(len(plain), 10)
        for name, text in plain.items():
            with self.subTest(frame=name):
                self.assertEqual(text.count(SENTENCE), 1)
                self.assertTrue(text.rstrip().endswith(f" {SENTENCE}"), "the last sentence of the frame")
                self.assertEqual(with_guidance[name].count(SENTENCE), 1, "the guidance's copy only")
                self.assertFalse(with_guidance[name].rstrip().endswith(SENTENCE))
        # The turn kinds every_prompt does not build (OP-12 P7c r2 B1, N6): what
        # _dispatch_room_event varies the frame by is the event type, the claimed
        # attempt, the Room's phase and phaseInstruction, the kind of the cycle
        # source (message, command, discussion start, unresolved) and the
        # optional blocks (handover, documents, word bound, missed source, kept reply).
        for name, (text, marker) in turn_kinds().items():
            with self.subTest(turn_kind=name):
                self.assertIn(marker, text)
                self.assertEqual(text.count(SENTENCE), 1)
                self.assertTrue(text.rstrip().endswith(f" {SENTENCE}"), "the last sentence of the frame")
        for name, (text, _marker) in turn_kinds(rules=carried).items():
            with self.subTest(turn_kind=name, guidance="carries it"):
                self.assertEqual(text.count(SENTENCE), 1, "the guidance's copy only")
                self.assertFalse(text.rstrip().endswith(SENTENCE))
        # Not a filter: a reply about missing access is posted unchanged, and
        # the status file holds no message body.
        body = "I have no internet access, so I answer from the brief."
        with tempfile.TemporaryDirectory() as scratch:
            path = Path(scratch) / "status.json"

            async def run():
                instance, binding, api, _published, _dispatched = room_instance(cycle_attempt())
                instance._status = local_status.LocalStatus(path, plugin="hermes", version=adapter.CONNECTOR_VERSION,
                                                            host_label="host")
                instance._status_room(binding, connection="connected")
                recording = RecordingRoom(api)
                result = await instance._send_final("room-1", source_ref(), body)
                return recording, result

            recording, result = asyncio.run(run())
            self.assertTrue(result.success)
            self.assertEqual(recording.bodies, [body])
            status_text = path.read_text()
            self.assertNotIn("internet", status_text)
            self.assertIn("webRead", status_text)

    def test_p7c_10_a_claim_refused_during_a_hold(self):
        hold = {"cycleId": "cycle-1", "holderMembershipId": "owner-1", "holderDisplayName": "Thorsten",
                "startedAt": "2026-10-05T10:00:00Z", "expiresAt": "2026-10-05T10:00:45Z",
                "latestExpiresAt": "2026-10-05T10:03:00Z"}
        with tempfile.TemporaryDirectory() as scratch:
            path = Path(scratch) / "status.json"
            # Refused, refused again (a second hold before the re-announced
            # claim), then claimed: answered exactly once.
            answers = {1: None, 2: None, 3: claimed_attempt()}
            instance, binding, record, room_state = receive_instance(
                state={"activeHold": hold}, claim=lambda n: answers.get(n), status_path=path)
            first = response_ready(51)
            for event in (first, {**first, "id": "ready-51-hold-1", "seq": 52},
                          {**first, "id": "ready-51-hold-2", "seq": 53}):
                asyncio.run(instance._consume(binding, event))
                if event["id"] == "ready-51-hold-1":
                    entry = local_status.read(path)["rooms"]["room-1"]
                    self.assertEqual((entry["lastGateWaitReason"], entry["lastGateWaitSeconds"]),
                                     ("held_by_moderator", 0))
                if event["id"] == "ready-51-hold-1":
                    room_state["activeHold"] = None
            self.assertEqual(record.claims, ["cycle-1"] * 3, "one claim per announcement, no retry loop")
            self.assertEqual(record.dispatched, ["ready-51-hold-2"])
            self.assertEqual([item["terminal_status"] for item in record.completed], ["superseded", "superseded"])
            text = path.read_text()
            self.assertNotIn("Thorsten", text)
            self.assertNotIn("family", text)
            # The later claim of that Room succeeded: the reason is cleared (P2c-4).
            entry = local_status.read(path)["rooms"]["room-1"]
            self.assertEqual((entry["lastGateWaitReason"], entry["lastGateWaitSeconds"]), ("", 0))
        # Only cycle_no_attempt is a hold refusal: cycle_superseded during a hold records no reason.
        with tempfile.TemporaryDirectory() as scratch:
            path = Path(scratch) / "status.json"

            def superseded(_n):
                raise adapter.ProtocolError("superseded", status=409, code="cycle_superseded")

            instance, binding, record, _room_state = receive_instance(
                state={"activeHold": hold}, claim=superseded, status_path=path)
            asyncio.run(instance._consume(binding, response_ready(51)))
            self.assertEqual(record.claims, ["cycle-1"])
            self.assertEqual(record.completed[-1]["reason"], "discussion_attempt_unavailable")
            rooms = (local_status.read(path).get("rooms") or {}) if path.exists() else {}
            self.assertEqual((rooms.get("room-1") or {}).get("lastGateWaitReason", ""), "")
        # A turn already claimed keeps renewing through a hold (the Room answers
        # the renewal as before).

        async def renew():
            instance, binding, api, _published, _dispatched = room_instance(cycle_attempt(lease=_lease_in(3)))
            api.claim = lambda _n: cycle_attempt(lease=_lease_in(3))
            # The renewal's waits are compressed; it is awaited by count, not by time.
            with mock.patch.object(asyncio, "sleep", Waits()):
                instance._start_attempt_renewal(binding, "evt-5", instance._cycle_attempts["evt-5"])
                held = instance._cycle_attempts["evt-5"]
                for _ in range(100_000):
                    if api.claims >= 2 and instance._cycle_attempts.get("evt-5") is not held:
                        break
                    await REAL_SLEEP(0)
                await instance._stop_attempt_renewal("evt-5")
            renewals = api.claims
            result = await instance._send_final("room-1", source_ref(), "Answer after the hold.")
            return renewals, result, api

        renewals, result, api = asyncio.run(renew())
        self.assertGreaterEqual(renewals, 2)
        self.assertTrue(result.success)
        self.assertEqual(len(api.posts), 1)

    def test_p7c_11_a_claim_refused_after_a_timeout(self):
        with tempfile.TemporaryDirectory() as scratch:
            path = Path(scratch) / "status.json"
            instance, binding, record, _state = receive_instance(status_path=path)
            asyncio.run(instance._consume(binding, attempt_ready(51, 2, source="m-43")))
            self.assertEqual(record.claims, ["cycle-1"])
            self.assertEqual(record.dispatched, [])
            self.assertEqual(record.completed[-1]["reason"], "discussion_attempt_unavailable")
            rooms = (local_status.read(path).get("rooms") or {}) if path.exists() else {}
            self.assertEqual((rooms.get("room-1") or {}).get("lastGateWaitReason", ""), "")
        # No time line in any frame: the Room does not send the turn's end (P7c-6).
        attempt = cycle()
        attempt["cycle"]["budgets"]["maxDuration"] = 1_800_000_000_000
        for event in (attempt_ready(50, 1), response_ready(51)):
            text = build_message(event, attempt=attempt, context=(START, HUMAN, PEER, INTERJECTION)).text
            for words in ("seconds after", "turn timeout", "Room turn time", "1800"):
                self.assertNotIn(words, text)

    def test_p7c_12_an_unknown_start_brings_no_earlier_entry_into_a_tool(self):
        tools = __import__(adapter.__name__.rsplit(".", 1)[0] + ".room_tools", fromlist=["room_tools"])
        earlier = {"id": "m-30", "seq": 30, "type": "message.posted", "actorRole": "human_owner",
                   "actorId": "owner-1", "payload": {"body": "Earlier discussion: the rejected plan."}}
        reads = []
        for start in (None, 0, -1, "41", [41], {"n": 41}, 41.5, True, 41):
            with self.subTest(start=start):
                class Protocol:
                    def __init__(self, *_args):
                        pass

                    def room_state(self, _room):
                        return {"title": "Room", "headSeq": 44, "roster": [],
                                "activeEpoch": {"id": "epoch-2", "startsAtSeq": start}}

                    def events(self, _room, after, *_args, **_kwargs):
                        reads.append(after)
                        return {"events": [item for item in (earlier, HUMAN) if item["seq"] > after]}

                binding = adapter.RoomBinding("https://room.example/api", "room-1", "member-1", "credential",
                                              installation_id="inst-1")
                with mock.patch.object(tools, "RoomProtocol", Protocol), \
                        mock.patch.object(tools, "load", lambda: types.SimpleNamespace(bindings=[binding])):
                    result = json.loads(tools.room_context({}))
                self.assertTrue(result["success"], result)
                self.assertNotIn("rejected plan", result["context"])
                if start == 41:
                    self.assertIn("Should the doctor disclose", result["context"])
                else:
                    self.assertNotIn("#42", result["context"])
        # A Room state whose activeEpoch is not an object does not raise either.
        for epoch in ([41], "epoch-2", 7):
            class Odd:
                def __init__(self, *_args):
                    pass

                def room_state(self, _room, epoch=epoch):
                    return {"title": "Room", "headSeq": 44, "activeEpoch": epoch}

                def events(self, *_args, **_kwargs):
                    raise AssertionError("no read without a start")

            with mock.patch.object(tools, "RoomProtocol", Odd), \
                    mock.patch.object(tools, "load", lambda: types.SimpleNamespace(bindings=[binding])):
                self.assertTrue(json.loads(tools.room_context({}))["success"])

    def test_p7c_13_an_unknown_page_start_never_stops_the_channel(self):
        terminal = {"id": "term-42", "seq": 42, "type": "discussion.cycle_terminal", "actorRole": "system",
                    "actorId": "room_coordinator",
                    "payload": {"cycleId": "cycle-0", "state": "interrupted", "reason": "epoch_superseded",
                                "epochId": "epoch-1"}}
        for start in (None, 0, "41"):
            with self.subTest(start=start):
                instance, binding, record, _state = receive_instance(
                    state={"activeEpoch": {"id": "epoch-2", "startsAtSeq": start}})
                asyncio.run(instance._consume(binding, terminal))
                self.assertEqual(record.completed[-1]["reason"], "prior_epoch_lifecycle")
                self.assertEqual(record.completed[-1]["terminal_status"], "ignored")

    def test_p7c_14_the_state_start_fences_delivery(self):
        old = {"id": "m-20", "seq": 20, "type": "message.posted", "actorRole": "human_owner", "actorId": "owner-1",
               "payload": {"body": "A message of the earlier discussion."}}
        instance, binding, record, _state = receive_instance()
        binding.cursor = binding.acknowledged_cursor = 10
        asyncio.run(instance._consume(binding, old))
        self.assertEqual(record.completed[-1]["reason"], "historical_epoch")
        self.assertEqual((record.starts, record.claims, record.dispatched), ([], [], []))

    def test_p7c_15_a_lost_session_after_an_outage_is_registered_within_one_second(self):
        room = FakeRoom()
        room.heartbeat_errors = [http_error(502)] * 6 + [lost_session()]
        instance, binding = watched(room)
        instance._registered_rooms.add(binding.room_id)
        waits = Waits()

        async def heartbeat():
            task = asyncio.create_task(instance._heartbeat_loop(binding, 15))
            while "register" not in room.log:
                await REAL_SLEEP(0.01)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        with mock.patch.object(asyncio, "sleep", waits), self.assertLogs(adapter.logger, level="INFO"):
            asyncio.run(asyncio.wait_for(heartbeat(), 20))
        self.assertEqual(room.log[:8], ["heartbeat failed"] * 7 + ["register"])
        delays = waits.of("_heartbeat_loop")
        # The interval, six growing waits after the 502s, then the first step.
        self.assertGreaterEqual(len(delays), 8, delays)
        self.assertGreater(delays[6], 8.0, delays)
        self.assertLessEqual(delays[7], 1.0, delays)
        self.assertGreaterEqual(delays[7], 0.8, delays)

    def test_p7c_16_a_revocation_on_the_heartbeat_ends_the_stream(self):
        for code, when in (("credential_revoked", "released later"), ("credential_expired", "released later"),
                           ("credential_revoked", "fails at once"), ("credential_expired", "fails at once"),
                           ("credential_revoked", "fails at once with its own error")):
            with self.subTest(code=code, stream=when), tempfile.TemporaryDirectory() as scratch:
                path = Path(scratch) / "status.json"
                room = FakeRoom()
                room.heartbeat_errors = [edge_error(401, json.dumps({"code": code, "message": "refused"}).encode())]
                instance, binding = watched(room, path)
                stream = types.SimpleNamespace(release=__import__("threading").Event(), reads=0, late=[])

                def open_stream(_room_id, after, on_event):
                    # The Room keeps the stream open and sends nothing; it ends
                    # with a network error once released.
                    stream.reads += 1
                    stream.opened.set()
                    if when.startswith("fails at once"):
                        # The stream breaks right after the revocation, before
                        # the loop looks at the binding again; an event read
                        # after the revocation is refused by the reader.
                        stream.revoked.wait(HANG_GUARD)
                        stream.late.append(on_event({**INTERJECTION, "id": "m-late", "seq": 41}))
                    else:
                        stream.release.wait(HANG_GUARD)
                    if when == "fails at once with its own error":
                        raise RuntimeError("the stream reader failed")
                    raise adapter.ProtocolError("Room network failure", retryable=True)

                room.stream_events = open_stream
                stream.opened = __import__("threading").Event()
                original_heartbeat = room.heartbeat

                def heartbeat_once_the_stream_is_open(room_id, session):
                    stream.opened.wait(HANG_GUARD)
                    return original_heartbeat(room_id, session)

                room.heartbeat = heartbeat_once_the_stream_is_open
                stream.revoked = __import__("threading").Event()
                for name in ("_revoke", "_expire"):
                    def end(target, original=getattr(instance, name)):
                        original(target)
                        stream.revoked.set()
                    setattr(instance, name, end)
                instance._heartbeat_tasks = {}
                ended = {}

                async def after():
                    deadline = time.monotonic() + HANG_GUARD
                    while not instance._tasks.get(binding.room_id, types.SimpleNamespace(done=lambda: True)).done() \
                            and time.monotonic() < deadline:
                        await REAL_SLEEP(0.01)
                    ended["watch"] = not instance._tasks
                    stream.release.set()
                    await REAL_SLEEP(0.1)

                with mock.patch.object(adapter, "update", lambda change: change(types.SimpleNamespace(binding=lambda _room: None))), \
                        mock.patch.object(asyncio, "sleep", Waits()), mock.patch.object(adapter, "logger", mock.Mock()):
                    outcome = run_watch(instance, binding, lambda: not binding.enabled, timeout=HANG_GUARD, after=after)
                self.assertTrue(outcome["reached"])
                self.assertTrue(ended["watch"], "the stream loop ended while the stream was still open")
                self.assertEqual(stream.reads, 1)
                self.assertEqual(stream.late, [False] if when.startswith("fails at once") else [])
                entry = local_status.read(path)["rooms"]["room-1"]
                self.assertEqual((entry["connection"], entry["lastErrorCode"]), ("stopped", code))

    def test_p7c_17_a_request_the_runtime_refuses_to_form_is_not_a_failed_contact(self):
        # A credential with a line break: http.client refuses the header before
        # any connection (the port would refuse one, which would be a network
        # failure and a ProtocolError).
        api = protocol.RoomProtocol("http://127.0.0.1:9/api", "credential\nX-Injected: 1")
        with self.assertRaises(ValueError) as raised:
            api.request("GET", "/rooms/room-1/state")
        self.assertNotIsInstance(raised.exception, adapter.ProtocolError)
        self.assertIsInstance(raised.exception, protocol.RequestNotFormed)
        # In the loop it is the connector's own error: it fails closed with its
        # own code, never network, and is not retried.
        outcome, room, entry, _logs, _status_text = refused_request_run(api)
        self.assertTrue(outcome["exited"], "the loop ended")
        self.assertEqual(room.log.count("register failed"), 1, "not retried")
        self.assertEqual((entry["connection"], entry["lastErrorCode"]), ("stopped", "request_not_formed"))

    def test_p7c_18_the_stored_showroom_guidance_already_carries_the_sentence(self):
        # Source: Harness b70aff3e0ff1fe04a4be12e1d90c22a13aba2e57,
        # packages/client/ui-sociality-room/src/client/policy-mode-texts.ts
        # (Showroom principle, POLICY_MODE_COMMON_TEXT and style, joined by
        # blank lines as policy-modes.ts modeGuidanceText stores them).
        stored = CONFORMANCE["parityP7cShowroomGuidance"]
        self.assertIn("b70aff3e0ff1fe04a4be12e1d90c22a13aba2e57", stored["source"])
        self.assertEqual(__import__("hashlib").sha256(stored["text"].encode("utf-8")).hexdigest(), stored["sha256"])
        text = stored["text"]
        frame = build_message(attempt_ready(50, 1), attempt=cycle(),
                              rules=[{"enforcement": "guidance", "text": text}]).text
        self.assertEqual(frame.count(SENTENCE), 1)
        self.assertIn("- Debate for an audience", frame)
        # Paragraph breaks inside the sentence and white space around it are tolerated.
        # A tab inside a run that holds a line feed is part of that break.
        broken = text.replace(" If you cannot read", "\n\nIf you cannot read").replace(
            " You may say", "\r\n  You may say").replace(" model or", "\t\n\tmodel or")
        self.assertTrue(adapter.guidance_carries_no_status_sentence([broken]))
        frame = build_message(attempt_ready(50, 1), attempt=cycle(),
                              rules=[{"enforcement": "guidance", "text": broken}]).text
        self.assertEqual(frame.count(SENTENCE), 0, "the Room's own wording only, not added again")
        self.assertIn("\n\nIf you cannot read", frame)
        # One character changed: the frame adds the sentence.
        for changed in (text.replace("system status.", "system status!"),
                        text.replace("Say nothing", "say nothing"),
                        text.replace("tools, access", "tools,  access"),
                        # The sentence without its final full stop (OP-12 P7c r2 OpenClaw N6, d5).
                        text.replace("is based on.", "is based on"),
                        # A tab or a lone carriage return is not a paragraph break (no line feed).
                        text.replace("tools, access", "tools,\taccess"),
                        text.replace("tools, access", "tools,\raccess")):
            with self.subTest(changed=changed[140:200]):
                self.assertFalse(adapter.guidance_carries_no_status_sentence([changed]))
                frame = build_message(attempt_ready(50, 1), attempt=cycle(),
                                      rules=[{"enforcement": "guidance", "text": changed}]).text
                self.assertEqual(frame.count(SENTENCE), 1)
                self.assertTrue(frame.rstrip().endswith(SENTENCE))


    def test_p7c_19_the_command_source_is_kept_through_a_renewal(self):
        # The fact is decided once at dispatch and kept apart from the attempt,
        # which each renewal replaces with the Room's fresh claim answer.
        cases = {}
        for label, source in (("command", "cmd-45"), ("message", "m-43")):
            recording, result, decided, _binding = asyncio.run(command_source_turn(source, renewals=1))
            self.assertTrue(result.success)
            cases[label] = (decided, recording.standalone, recording.responds_to)
        self.assertEqual(cases["command"], ({"ready-52"}, [True], ["cmd-45"]))
        self.assertEqual(cases["message"], (set(), [False], ["m-43"]))

        # A restart after the request was frozen and before the post landed:
        # the new process holds no in-memory fact; the frozen request carries
        # it and is posted as frozen.
        class Crash(BaseException):
            pass

        def crash(_n):
            raise Crash()

        async def restarted():
            try:
                await command_source_turn("cmd-45", renewals=1, post_error=crash)
            except Crash:
                pass
            crashed = CRASHED[-1]
            instance, fresh, api, _published, _dispatched = room_instance(fresh_attempt("cmd-45"))
            fresh.delivery_intents = copy.deepcopy(crashed.delivery_intents)
            fresh.delivery_authority = copy.deepcopy(crashed.delivery_authority)
            instance._state = types.SimpleNamespace(binding=lambda room_id: fresh if room_id == "room-1" else None)
            instance._cycle_response_sources["evt-5"] = "cmd-45"
            recording = RecordingRoom(api)
            return await instance._send_final("room-1", source_ref(), "My answer."), recording, instance, crashed

        result, recording, instance, crashed = asyncio.run(restarted())
        self.assertTrue(crashed.delivery_intents["evt-5"]["post"]["cycle"]["standalone"])
        self.assertTrue(result.success)
        self.assertEqual(getattr(instance, "_cycle_command_sources", set()), set())
        self.assertEqual((recording.standalone, recording.responds_to), ([True], ["cmd-45"]))

    def test_p7c_20_a_room_url_the_runtime_refuses_to_form_is_the_connectors_own_error(self):
        # A Room URL http.client refuses (a port that is not a number; no host):
        # raised before any connection, the connector's own error, never a
        # failed contact; the loop fails closed with request_not_formed and no
        # request leaves the host.
        # Also (OP-12 P7c r2 N2, N3) a URL without a scheme or with a broken IPv6
        # bracket (urllib cannot build the request), a host outside Latin-1 (the
        # Host header) and a scheme other than http or https: the same error,
        # whose text never quotes the URL.
        connects = []
        with mock.patch("socket.create_connection", lambda *args, **_kwargs: connects.append(args)):
            for url in ("http://127.0.0.1:port/api", "http:///api", "room.example/api", "//room.example/api",
                        "http://[::1/api", "http://例.example/api", "foo://room.example/api"):
                with self.subTest(url=url):
                    api = protocol.RoomProtocol(url, "credential")
                    for call in (lambda: api.request("GET", "/rooms/room-1/state"),
                                 lambda: api.stream_events("room-1", 40, lambda _event: True)):
                        with self.assertRaises(Exception) as raised:
                            call()
                        self.assertIsInstance(raised.exception, protocol.RequestNotFormed)
                        self.assertNotIsInstance(raised.exception, adapter.ProtocolError)
                        self.assertNotIsInstance(raised.exception, OSError)
                        self.assertIsNone(raised.exception.__context__)
                        self.assertNotIn("example", str(raised.exception) + repr(raised.exception.args))
                    outcome, room, entry, _logs, _status_text = refused_request_run(api)
                    self.assertTrue(outcome["exited"], "the loop ended")
                    self.assertEqual(room.log.count("register failed"), 1, "not retried")
                    self.assertEqual((entry["connection"], entry["lastErrorCode"]), ("stopped", "request_not_formed"))
            # The tools print the fixed message only (registered handlers, saved binding).
            tools = __import__(adapter.__name__.rsplit(".", 1)[0] + ".room_tools", fromlist=["room_tools"])
            binding = adapter.RoomBinding("room.example/api", "room-1", "member-1", "credential", installation_id="inst-1")
            tools._ROOM_SCAN_CACHE.clear()
            with mock.patch.object(tools, "load", lambda: types.SimpleNamespace(bindings=[binding])):
                registration = mock.MagicMock()
                adapter.register(registration)
                handlers = {call.kwargs["name"]: call.kwargs["handler"] for call in registration.register_tool.call_args_list}
                for name in ("synthetic_sociality_room_context", "synthetic_sociality_room_history"):
                    with self.subTest(tool=name):
                        result = handlers[name]({"room": "room-1"})
                        self.assertFalse(json.loads(result)["success"])
                        self.assertIn("Room request refused by the runtime before any I/O", result)
                        self.assertNotIn("example", result)
        self.assertEqual(connects, [])
        # The other side (OP-12 P7c r3 N1, N2): a Room URL that worked still forms.
        # An upper-case scheme, leading white space and a Latin-1 host reach the
        # connect (l7, l6); the scheme check is an allow-list: http and https
        # reach it, ftp, file, ws and wss are refused before any name lookup (l2).
        attempts, lookups = [], []

        def refused(address, *_args, **_kwargs):
            attempts.append(address[0])
            raise ConnectionRefusedError(111, "Connection refused")

        def no_lookup(host, *_args, **_kwargs):
            lookups.append(host)
            raise socket.gaierror(socket.EAI_NONAME, "Name or service not known")

        with mock.patch("socket.create_connection", refused), mock.patch("socket.getaddrinfo", no_lookup), \
                mock.patch("socket.gethostbyname", no_lookup), mock.patch("socket.gethostbyname_ex", no_lookup):
            for url, host in (("http://room.example/api", "room.example"), ("https://room.example/api", "room.example"),
                              ("HTTPS://room.example/api", "room.example"), ("Https://Room.Example/API", "Room.Example"),
                              ("HTTP://127.0.0.1:8787/api", "127.0.0.1"), ("  https://room.example/api", "room.example"),
                              ("\thttp://127.0.0.1:8787/api", "127.0.0.1"), ("http://räume.example/api", "räume.example"),
                              ("https://räume.example:8443/api", "räume.example")):
                api = protocol.RoomProtocol(url, "credential")
                for contact, call in (("request", lambda: api.request("GET", "/rooms/room-1/state")),
                                      ("stream", lambda: api.stream_events("room-1", 40, lambda _event: True))):
                    with self.subTest(url=url, contact=contact, formed=True):
                        attempts.clear()
                        with self.assertRaises(adapter.ProtocolError) as raised:
                            call()
                        self.assertNotIsInstance(raised.exception, protocol.RequestNotFormed)
                        self.assertTrue(raised.exception.retryable)
                        self.assertEqual(attempts, [host], "formed: the connect was attempted")
            attempts.clear()
            for url in ("ftp://room.example/api", "file://room.example/api", "ws://room.example/api",
                        "wss://room.example/api", "FTP://room.example/api"):
                api = protocol.RoomProtocol(url, "credential")
                for contact, call in (("request", lambda: api.request("GET", "/rooms/room-1/state")),
                                      ("stream", lambda: api.stream_events("room-1", 40, lambda _event: True))):
                    with self.subTest(url=url, contact=contact, formed=False):
                        with self.assertRaises(protocol.RequestNotFormed):
                            call()
            self.assertEqual(attempts, [])
        self.assertEqual(lookups, [], "no name was looked up")

    def test_p7c_21_a_refused_credential_never_appears_in_an_error_a_log_line_or_the_status(self):
        # A credential with a character the runtime rejects in a header: the
        # plugin's own error (fixed message, code request_not_formed, nothing
        # chained), the loop fails closed, and the credential string is in no
        # log line, no status file and no error message.
        secret = "fixture-secret-7Q2"
        # A line break inside the value (a trailing one is trimmed by the runtimes).
        for credential in (f"{secret}\nX-Injected: 1", f"{secret}\rX-Injected: 1"):
            with self.subTest(credential=repr(credential)):
                api = protocol.RoomProtocol("https://room.example/api", credential)
                errors = []
                for call in (lambda: api.request("GET", "/rooms/room-1/state"),
                             lambda: api.stream_events("room-1", 40, lambda _event: True)):
                    with self.assertRaises(protocol.RequestNotFormed) as raised:
                        call()
                    errors.append(raised.exception)
                for error in errors:
                    self.assertIsNone(error.__cause__)
                    self.assertIsNone(error.__context__)
                    self.assertEqual(error.code, "request_not_formed")
                    rendered = "".join(__import__("traceback").format_exception(error)) + repr(error) + str(error.args)
                    self.assertNotIn(secret, rendered)
                outcome, room, entry, logs, status_text = refused_request_run(api)
                self.assertTrue(outcome["exited"], "the loop ended")
                self.assertEqual(room.log.count("register failed"), 1, "not retried")
                self.assertEqual((entry["connection"], entry["lastErrorCode"]), ("stopped", "request_not_formed"))
                self.assertIn(STOP_LINE, logs.split("\n"), "the stop is logged")
                self.assertNotIn(secret, logs)
                self.assertNotIn(secret, status_text)
                # The heartbeat loop meets the same refused request first (a
                # credential renewed after the registration): it ends as well.
                outcome, beats, entry, logs, status_text = refused_heartbeat_run(api)
                self.assertTrue(outcome["exited"], "the heartbeat loop ended")
                self.assertEqual(beats, ["heartbeat refused"], "not retried")
                self.assertEqual((entry["connection"], entry["lastErrorCode"]), ("stopped", "request_not_formed"))
                self.assertIn(STOP_LINE, logs.split("\n"))
                self.assertNotIn(secret, logs)
                self.assertNotIn(secret, status_text)
        # The other side (OP-12 P7c r3 OpenClaw B1, the same forms here): a
        # credential http.client sends is formed and reaches the connect: a
        # trailing line feed, spaces at both ends, a tab inside, é, ÿ, U+0080.
        attempts = []

        def refused(address, *_args, **_kwargs):
            attempts.append(address[0])
            raise ConnectionRefusedError(111, "Connection refused")

        with mock.patch("socket.create_connection", refused):
            for credential in (f"{secret}\n", f"  {secret}  ", f"{secret}\tX", f"{secret}é", f"{secret}ÿ", f"{secret}\x80X"):
                api = protocol.RoomProtocol("https://room.example/api", credential)
                for contact, call in (("request", lambda: api.request("GET", "/rooms/room-1/state")),
                                      ("stream", lambda: api.stream_events("room-1", 40, lambda _event: True))):
                    with self.subTest(credential=repr(credential), contact=contact, formed=True):
                        attempts.clear()
                        with self.assertRaises(adapter.ProtocolError) as raised:
                            call()
                        self.assertNotIsInstance(raised.exception, protocol.RequestNotFormed)
                        self.assertEqual(attempts, ["room.example"], "formed: the connect was attempted")


    def test_p7c_22_held_by_moderator_only_for_a_hold_of_the_refused_cycle(self):
        # cycle_no_attempt AND an activeHold object naming the same cycle as the
        # refused claim; not {}, not a hold of another cycle, not cycle_superseded.
        # The status never holds the holder's name or membership id.
        hold = {"cycleId": "cycle-1", "holderMembershipId": "holder-7k", "holderDisplayName": "Thorsten",
                "startedAt": "2026-10-05T10:00:00Z", "expiresAt": "2026-10-05T10:00:45Z"}

        def superseded(_n):
            raise adapter.ProtocolError("superseded", status=409, code="cycle_superseded")

        cases = {
            "hold of this cycle": ({"activeHold": hold}, None, "held_by_moderator"),
            "empty object": ({"activeHold": {}}, None, ""),
            "hold of another cycle": ({"activeHold": {**hold, "cycleId": "cycle-other"}}, None, ""),
            "hold without a cycle": ({"activeHold": {key: value for key, value in hold.items() if key != "cycleId"}}, None, ""),
            "cycle_superseded during the hold": ({"activeHold": hold}, superseded, ""),
            "no hold": ({"activeHold": None}, None, ""),
            # A truthy activeHold that is not an object names no cycle (OP-12 P7c r2 N5, h2).
            **{f"activeHold {value!r}": ({"activeHold": value}, None, "")
               for value in ("cycle-1", True, 1, ["cycle-1"], [hold])},
        }
        for label, (state, claim, reason) in cases.items():
            with self.subTest(label), tempfile.TemporaryDirectory() as scratch:
                path = Path(scratch) / "status.json"
                instance, binding, record, _room_state = receive_instance(state=state, claim=claim, status_path=path)
                asyncio.run(instance._consume(binding, response_ready(51)))
                self.assertEqual(record.claims, ["cycle-1"])
                self.assertEqual(record.completed[-1]["reason"], "discussion_attempt_unavailable")
                text = path.read_text() if path.exists() else "{}"
                rooms = json.loads(text).get("rooms") or {}
                self.assertEqual((rooms.get("room-1") or {}).get("lastGateWaitReason", ""), reason)
                for value in ("holder-7k", "Thorsten"):
                    self.assertNotIn(value, text)
        # P2c-4: cleared by a later successful claim of that Room, a renewal
        # included (n2); a later refused claim leaves it (n3).
        for label, later, reason in (("renewal answered", "renewal", ""),
                                     ("later claim refused without a hold", "refused", "held_by_moderator")):
            with self.subTest(label), tempfile.TemporaryDirectory() as scratch:
                path = Path(scratch) / "status.json"
                answers = {2: claimed_attempt()} if later == "renewal" else {}
                instance, binding, record, room_state = receive_instance(
                    state={"activeHold": hold}, claim=lambda n: answers.get(n), status_path=path)
                asyncio.run(instance._consume(binding, response_ready(51)))
                self.assertEqual(local_status.read(path)["rooms"]["room-1"]["lastGateWaitReason"], "held_by_moderator")
                room_state["activeHold"] = None
                if later == "renewal":
                    renewed = asyncio.run(instance._claim_held_attempt(binding, "ready-50", claimed_attempt()))
                    self.assertIsNotNone(renewed)
                else:
                    asyncio.run(instance._consume(binding, {**response_ready(52), "id": "ready-51-hold-1"}))
                self.assertEqual(record.claims, ["cycle-1"] * 2)
                self.assertEqual(local_status.read(path)["rooms"]["room-1"]["lastGateWaitReason"], reason)

    def test_p7c_23_a_transient_failure_is_a_failed_contact_never_the_connectors_own_error(self):
        # OP-12 P7c r3 B1: a stop needs a gateway restart, so a transient failure
        # must never be taken for a request the runtime refused to form. Each kind,
        # real (loopback listeners, this process's connect and resolver), on a
        # request and on the event stream: a retryable ProtocolError without a
        # code, never RequestNotFormed; the watch and heartbeat loops go on.
        with TransientFaults() as faults:
            for kind, marker in faults.KINDS.items():
                for contact in ("register", "stream", "heartbeat"):
                    with self.subTest(kind=kind, contact=contact):
                        with self.assertRaises(adapter.ProtocolError) as raised:
                            faults.fail(kind, contact)
                        self.assertNotIsInstance(raised.exception, protocol.RequestNotFormed)
                        self.assertTrue(raised.exception.retryable)
                        self.assertEqual((raised.exception.code, raised.exception.status), ("", 0))
                        self.assertIn(marker, str(raised.exception), "the failure is of this kind")
                with self.subTest(kind=kind, loop="watch"):
                    room = FakeRoom()
                    room.register_errors = [lambda kind=kind: faults.fail(kind, "register")]
                    room.stream_errors = [lambda kind=kind: faults.fail(kind, "stream")]
                    room.pending_events = [{"id": "evt-after", "type": "message.posted"}]
                    instance, binding = watched(room)
                    statuses = recorded_statuses(instance)
                    with mock.patch.object(asyncio, "sleep", Waits()):
                        outcome = run_watch(instance, binding, lambda: instance.consumed == ["evt-after"],
                                            timeout=HANG_GUARD)
                    self.assertTrue(outcome["reached"] and not outcome["exited"], "the watch loop goes on")
                    self.assertEqual(room.log[:2], ["register failed", "register"])
                    self.assertGreaterEqual(statuses.count(("reconnecting", "network")), 2, statuses)
                    self.assertFalse([entry for entry in statuses if "stopped" in entry or "request_not_formed" in entry])
                with self.subTest(kind=kind, loop="heartbeat"):
                    room, beats = FakeRoom(), []

                    def heartbeat(room_id, session, kind=kind):
                        beats.append(kind)
                        return faults.fail(kind, "heartbeat") if len(beats) <= 2 else {"leaseExpiresAt": "2099-01-01T00:00:00Z"}

                    room.heartbeat = heartbeat
                    instance, binding = watched(room)
                    instance._registered_rooms.add(binding.room_id)
                    statuses = recorded_statuses(instance)
                    outcome = {}

                    async def run():
                        task = asyncio.create_task(instance._heartbeat_loop(binding, 15))
                        deadline = time.monotonic() + HANG_GUARD
                        while not task.done() and len(beats) < 4 and time.monotonic() < deadline:
                            await REAL_SLEEP(0.01)
                        outcome["exited"] = task.done()
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)

                    with mock.patch.object(asyncio, "sleep", Waits()):
                        asyncio.run(run())
                    self.assertFalse(outcome["exited"], "the heartbeat loop goes on")
                    self.assertGreaterEqual(len(beats), 4)
                    self.assertEqual(statuses.count(("reconnecting", "network")), 2, statuses)
                    self.assertFalse([entry for entry in statuses if "stopped" in entry or "request_not_formed" in entry])


class TransientFaults:
    """P7c-23: the transient failures of a Room contact, real: a response
    timeout (a loopback peer that accepts and never answers), a connection reset
    (a loopback peer that reads the request and resets), a refused connection (a
    closed loopback port); a connect timeout and a DNS failure as this process's
    connect and resolver raise them. Each kind with a word of its message."""

    KINDS = {"response timeout": "timed out", "connect timeout": "timed out", "DNS failure": "Name or service not known",
             "connection reset": "reset", "connection refused": "refused"}

    def __enter__(self):
        self.listeners, self.held = {}, []
        for kind in ("response timeout", "connection reset"):
            listener = socket.create_server(("127.0.0.1", 0))
            self.listeners[kind] = listener
            threading.Thread(target=self._serve, args=(kind, listener), daemon=True).start()
        closed = socket.create_server(("127.0.0.1", 0))
        self.closed_port = closed.getsockname()[1]
        closed.close()
        return self

    def __exit__(self, *_exc):
        for item in (*self.listeners.values(), *self.held):
            item.close()

    def _serve(self, kind, listener):
        while True:
            try:
                peer, _address = listener.accept()
            except OSError:
                return
            received = b""
            try:
                peer.settimeout(HANG_GUARD)
                while b"\r\n\r\n" not in received:
                    chunk = peer.recv(65536)
                    if not chunk:
                        break
                    received += chunk
                head, _, body = received.partition(b"\r\n\r\n")
                length = next((int(line.split(b":", 1)[1]) for line in head.split(b"\r\n")
                               if line.lower().startswith(b"content-length:")), 0)
                while len(body) < length:
                    chunk = peer.recv(65536)
                    if not chunk:
                        break
                    body += chunk
            except OSError:
                pass
            if kind == "connection reset":
                # The whole request was read; a close without lingering sends RST.
                peer.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
                peer.close()
            else:
                self.held.append(peer)

    def fail(self, kind, contact):
        """Make the real ``contact`` (register, stream or heartbeat) fail with ``kind``; raise what the client raises."""
        port = self.listeners[kind].getsockname()[1] if kind in self.listeners else self.closed_port
        host = "room.invalid" if kind == "DNS failure" else "127.0.0.1"
        api = protocol.RoomProtocol(f"http://{host}:{port}/api", "credential", timeout=0.3)
        calls = {"register": lambda: api.register_connector("room-1", "inst-1", {}),
                 "stream": lambda: api.stream_events("room-1", 0, lambda _event: True),
                 "heartbeat": lambda: api.heartbeat("room-1", "session-1")}
        real_urlopen = protocol._urlopen

        def timed_out(*_args, **_kwargs):
            raise TimeoutError("timed out")

        def unresolved(*_args, **_kwargs):
            raise socket.gaierror(socket.EAI_NONAME, "Name or service not known")

        with contextlib.ExitStack() as patches:
            # The stream's 75 s read bound, shortened; the request path's is the client's timeout.
            patches.enter_context(mock.patch.object(protocol, "_urlopen", lambda request, *, timeout: real_urlopen(
                request, timeout=min(timeout, 0.3))))
            if kind == "connect timeout":
                patches.enter_context(mock.patch("socket.create_connection", timed_out))
            if kind == "DNS failure":
                patches.enter_context(mock.patch("socket.getaddrinfo", unresolved))
            calls[contact]()
        raise AssertionError(f"{kind}: the {contact} did not fail")


def recorded_statuses(instance):
    """Every (connection, error_code) the loops report for the Room, in order."""
    statuses, report = [], instance._status_room

    def record(binding, room_id="", **fields):
        statuses.append((fields.get("connection"), fields.get("error_code")))
        return report(binding, room_id, **fields)

    instance._status_room = record
    return statuses


# The one log line of a stop (adapter._fail_closed_on_request), as captured below.
STOP_LINE = (f"{adapter.logger.name} ERROR Room room-1 request refused by the runtime before any I/O "
             "(code request_not_formed); the loop stops")


@__import__("contextlib").contextmanager
def captured_logs():
    """Every record of the root logger at DEBUG, formatted, while the block runs."""
    import logging
    records = []

    class Capture(logging.Handler):
        def emit(self, record):
            records.append(logging.Formatter("%(name)s %(levelname)s %(message)s").format(record))

    handler, root = Capture(level=logging.DEBUG), logging.getLogger()
    previous = root.level
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)
    try:
        yield records
    finally:
        root.removeHandler(handler)
        root.setLevel(previous)


def refused_request_run(api):
    """Run the watch loop whose registration makes ``api``'s real request (which
    the runtime refuses to form) until the loop ends. Returns the outcome, the
    Room double, the status entry, every log line and the status file's text."""
    with captured_logs() as records, tempfile.TemporaryDirectory() as scratch:
        path = Path(scratch) / "status.json"
        room = FakeRoom()
        room.register_errors = [lambda: api.request("POST", "/connectors"), lambda: api.request("POST", "/connectors")]
        instance, binding = watched(room, path)
        # The binding holds the refused credential, as it does live.
        binding.credential = api.credential
        with mock.patch.object(asyncio, "sleep", Waits()):
            outcome = run_watch(instance, binding, lambda: "register failed" in room.log
                                and binding.room_id not in instance._tasks, timeout=HANG_GUARD)
        return outcome, room, local_status.read(path)["rooms"]["room-1"], "\n".join(records), path.read_text()


def refused_heartbeat_run(api):
    """Run the heartbeat loop whose heartbeat makes ``api``'s real request (which
    the runtime refuses to form) until it ends or has tried three times.
    Returns the outcome, the heartbeats tried, the status entry, every log line
    and the status file's text."""
    with captured_logs() as records, tempfile.TemporaryDirectory() as scratch:
        path = Path(scratch) / "status.json"
        room, beats, outcome = FakeRoom(), [], {}

        def heartbeat(room_id, session):
            beats.append("heartbeat refused")
            return api.request("POST", f"/rooms/{room_id}/connectors/{session}/heartbeat")

        room.heartbeat = heartbeat
        instance, binding = watched(room, path)
        binding.credential = api.credential
        instance._registered_rooms.add(binding.room_id)

        async def run():
            task = asyncio.create_task(instance._heartbeat_loop(binding, 15))
            deadline = time.monotonic() + HANG_GUARD
            while not task.done() and len(beats) < 3 and time.monotonic() < deadline:
                await REAL_SLEEP(0.01)
            outcome["exited"] = task.done()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        with mock.patch.object(asyncio, "sleep", Waits()):
            asyncio.run(run())
        return outcome, beats, local_status.read(path)["rooms"]["room-1"], "\n".join(records), path.read_text()


CRASHED = []


def fresh_attempt(source):
    attempt = claimed_attempt()
    attempt["cycle"]["sourceEventId"] = source
    return attempt


def turn_kinds(**options):
    """P7c-4: the frame of each turn kind every_prompt does not build, with a
    marker that shows the kind was rendered: (text, marker) per name."""
    kept_reply = {"source_event_id": "m-42", "source_seq": 42, "epoch_id": "epoch-2",
                  "produced_at": "2026-10-05T09:52:30Z", "reason": "turn_expired", "text": "My kept earlier reply."}

    def keep(_instance, binding):
        binding.kept_reply = dict(kept_reply)

    def miss(_instance, binding):
        binding.missed_sources = [{"source_event_id": "m-42", "source_seq": 42}]

    def command_turn(name):
        source = command(46, name, arguments={"instruction": "Your view?"})
        kept = []
        text = build_message(attempt_ready(52, 1, source="cmd-46"), attempt=fresh_attempt("cmd-46"),
                             context=(START, HUMAN, PEER, source), keep_instance=kept, **options).text
        assert getattr(kept[0][0], "_cycle_command_sources", set()) == {"ready-52"}, "decided as a command-source turn"
        return text

    summary = attempt_ready(55, 1, source="term-54", phase="summary", phaseInstruction="Write the closing summary.")
    return {
        "response round": (build_message(response_ready(51), attempt=claimed_attempt(),
                                         context=(START, HUMAN, PEER, INTERJECTION), **options).text,
                           f"[Room phase instruction — server-provided, verbatim]\n{RESPONSE_INSTRUCTION}\n"),
        "closing summary": (build_message(summary, attempt=claimed_attempt(), **options).text,
                            "[Autonomous discussion phase summary, round 2,"),
        "ask cycle": (command_turn("ask"), "; its source event cmd-46 is not available to the connector]"),
        "summarize cycle": (command_turn("summarize"), "; its source event cmd-46 is not available to the connector]"),
        "kept reply offered": (build_message(attempt_ready(50, 2, source="m-43"), attempt=cycle(), prepare=keep,
                                             **options).text, "\nMy kept earlier reply.\n[/Your own earlier reply]"),
        "missed source offered": (build_message(attempt_ready(50, 2, source="m-43"), attempt=cycle(), prepare=miss,
                                                **options).text, "\nMissed earlier: #42 Owner; "),
        "discussion start as the current item": (build_message(attempt_ready(50, 1, source="start-2"), attempt=cycle(),
                                                               **options).text, "\n\nOwner: Patient and doctor\n\n"),
        # The Room's phases opening and initial_greeting in round 1, and a word
        # bound frozen in the cycle (as OpenClaw, OP-12 P7c r3 OpenClaw B2, N2).
        "opening": (build_message(attempt_ready(50, 1, source="m-43", phase="opening"), attempt=cycle(used=0),
                                  **options).text, "[Autonomous discussion phase opening, round 1,"),
        "initial greeting": (build_message(attempt_ready(50, 1, source="m-43", phase="initial_greeting"),
                                           attempt=cycle(used=0), **options).text,
                             "[Autonomous discussion phase initial_greeting, round 1,"),
        "word bound frozen in the cycle": (
            build_message(attempt_ready(50, 2, source="m-43"), attempt=cycle(maxContributionWords=120), **options).text,
            "At most 120 words per response (Room policy maxContributionWords, as frozen for this discussion cycle); "
            "there is no minimum."),
        "open message offering a kept reply": (build_message({**HUMAN, "id": "m-56", "seq": 56}, prepare=keep,
                                                             **options).text,
                                               "\nMy kept earlier reply.\n[/Your own earlier reply]"),
        # The source message older than the loaded context, found by the bounded
        # older-source scan (P1a-8, the P1a-12 fixture; OP-12 P7c r3 N3, a9).
        "source found by the older-source scan": (
            build_message(attempt_ready(1250, 1, source="msg-1020"), attempt=cycle(used=0), roster=CODING_ROSTER,
                          context=(CODING_START, coding_message(1020, "owner-1", "human_owner", CODING_OWNER_BODY),
                                   *(coding_message(1100 + 8 * index, "paula-1" if index % 2 else "member-1",
                                                    "participant_agent", f"Later note {index}.") for index in range(16))),
                          starts_at=1002, **options).text,
            f"#1020 TJE: {CODING_OWNER_BODY}"),
    }


async def command_source_turn(source, *, renewals, post_error=None):
    """P7c-1: dispatch an ask-cycle turn (source cmd-45 or m-43), let the
    renewal replace the attempt ``renewals`` times with the Room's fresh claim
    answer, then send. Returns the posts, the result, the turns the dispatch
    decided to be command-source turns, and the binding."""
    attempt = fresh_attempt(source)
    kept = []
    await asyncio.to_thread(build_message, attempt_ready(52, 1, source=source), attempt=attempt,
                            context=(START, HUMAN, PEER, ASK), keep_instance=kept)
    decided = set(getattr(kept[0][0], "_cycle_command_sources", set()))
    # The send harness holds the same turn as evt-5; the dispatch's fact for
    # ready-52 goes with it, as one process carries it from dispatch to send.
    instance, binding, api, _published, _dispatched = room_instance(attempt)
    instance._cycle_command_sources = {"evt-5"} if "ready-52" in decided else set()
    instance._cycle_response_sources["evt-5"] = source
    api.claim = lambda _n: fresh_attempt(source)  # never the object the connector holds
    api.post_error = post_error
    recording = RecordingRoom(api)
    CRASHED.append(binding)
    if renewals:
        with mock.patch.object(asyncio, "sleep", Waits()):
            instance._start_attempt_renewal(binding, "evt-5", attempt)
            for _ in range(100_000):  # a bound on loop turns, not on time
                # Claimed and applied: the stored attempt is the Room's new object.
                if api.claims >= renewals and instance._cycle_attempts.get("evt-5") is not attempt:
                    break
                await REAL_SLEEP(0)
            await instance._stop_attempt_renewal("evt-5")
        assert instance._cycle_attempts["evt-5"] is not attempt, "the renewal replaced the attempt"
    result = await instance._send_final("room-1", source_ref(), "My answer.")
    return recording, result, decided, binding


class P7cLeftoverTests(unittest.TestCase):
    """P7c items 4 and 8: tests for mutations that survived the P6 round-2 and
    P7a round-3 reviews (REVIEW-20261004-parity-P6-r2.md finding 4;
    REVIEW-20261005-parity-P7a-r3-hermes.md N1: j, q2, q3, q8)."""

    def test_the_header_check_refuses_a_credential_exactly_when_http_client_does(self):
        # OP-12 P7c r3 (OpenClaw N4, the equal here): the connector refuses a
        # credential exactly when the running http.client refuses the header it
        # would send (the stored credential is trimmed first). Every code point
        # 0x00-0xFF and a few above, alone, inside, first and last; both through
        # real requests to a loopback listener.
        import http.client
        import http.server

        class Answer(http.server.BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802 - http.server API
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"{}")

            def log_message(self, *_args):
                pass

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Answer)
        server.daemon_threads = True
        threading.Thread(target=server.serve_forever, daemon=True).start()
        port = server.server_address[1]

        def runtime(value):
            token = value.strip()
            connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
            try:
                connection.request("GET", "/x", headers={"Authorization": f"Bearer {token}"} if token else {})
                connection.getresponse().read()
                return "sent"
            except (ValueError, http.client.HTTPException):
                return "refused"
            finally:
                connection.close()

        def plugin(value):
            try:
                protocol.RoomProtocol(f"http://127.0.0.1:{port}/api", value).request("GET", "/rooms/room-1/state")
                return "sent"
            except protocol.RequestNotFormed:
                return "refused"
            except Exception as error:  # noqa: BLE001 - reported as a disagreement
                return f"failed {type(error).__name__}"

        outcomes, disagreements = {"sent": 0, "refused": 0}, []
        try:
            for point in (*range(0x100), 0x100, 0x2019, 0x3042, 0xD800, 0xFEFF, 0xFFFD, 0xFFFF):
                character = chr(point)
                for value in (character, f"a{character}b", f"{character}ab", f"ab{character}"):
                    expected, actual = runtime(value), plugin(value)
                    outcomes[expected] += 1
                    if actual != expected:
                        disagreements.append(f"U+{point:04X} in {value!r}: http.client {expected}, plugin {actual}")
        finally:
            server.shutdown()
            server.server_close()
        self.assertEqual(disagreements, [])
        self.assertTrue(outcomes["sent"] and outcomes["refused"], outcomes)

    def test_every_start_read_takes_a_string_start_as_unknown(self):
        # A start "45" is unknown (P6-2); int() would read it as 45 and fence
        # the turn as historical at the receive fence, the cycle-start fence,
        # the dispatch fence and the send path.
        string_start = {"activeEpoch": {"id": "epoch-2", "startsAtSeq": "45"}}
        instance, binding, record, _state = receive_instance(state=string_start)
        binding.cursor = binding.acknowledged_cursor = 10
        early = {**INTERJECTION, "id": "m-30", "seq": 30}
        asyncio.run(instance._consume(binding, early))
        self.assertEqual([request["sourceEventId"] for request in record.starts], ["m-30"])
        message = build_message({**HUMAN, "id": "m-30", "seq": 30}, starts_at="45", context=(HUMAN,))
        self.assertIn("Should the doctor disclose", message.text)

        async def send():
            instance, binding, api, _published, _dispatched = room_instance(cycle_attempt())
            api.room_state = lambda _room: {"headSeq": 5, "activeEpoch": {"id": "epoch-1", "startsAtSeq": "45"}}
            return await instance._send_final("room-1", source_ref(), "An answer."), api

        result, api = asyncio.run(send())
        self.assertTrue(result.success)
        self.assertEqual(len(api.posts), 1, "not superseded as stale_epoch")
        # The cycle-conflict re-read: a string start does not make the source historical.
        conflict = adapter.ProtocolError("conflict", status=409, code="cycle_conflict")

        def refuse(_request):
            raise conflict

        instance, binding, record, _state = receive_instance(state=string_start)
        binding.cursor = binding.acknowledged_cursor = 10
        with mock.patch.object(adapter, "_is_cycle_start_refusal", lambda error: error is conflict):
            instance._call = _wrap_start(instance._call, refuse)
            asyncio.run(instance._consume(binding, early))
        self.assertEqual(record.completed[-1]["terminal_status"], "skipped")

    def test_the_turn_source_memory_is_kept_per_source(self):
        # P6-5: remembered per Room and source. An entry for another source of
        # the same Room never stands in for this turn's source.
        older = [{"id": f"m-{seq}", "seq": seq, "type": "message.posted", "actorRole": "human_owner",
                  "actorId": "owner-1", "payload": {"body": f"Message {seq}."}} for seq in range(42, 251)]

        # Two turns of one connector: the first scans for m-43, the second for m-44.
        # Both lie outside the first context chunk (CONTEXT_SCAN_CHUNK = 100 before the turn).
        kept = []
        first = build_message(attempt_ready(300, 2, source="m-43"), attempt=claimed_attempt(),
                              context=(START, *older), keep_instance=kept)
        self.assertIn("#43 Owner: Message 43.", first.text)
        instance, binding = kept[0]
        captured = []
        instance.handle_message = lambda message: asyncio.sleep(0, result=captured.append(message))
        second_event = attempt_ready(301, 2, source="m-44")
        instance._cycle_attempts[second_event["id"]] = claimed_attempt()
        instance._event_epoch[second_event["id"]] = "epoch-2"
        self.assertTrue(asyncio.run(instance._dispatch_room_event(binding, second_event, "generation")))
        self.assertIn("#44 Owner: Message 44.", captured[0].text)
        self.assertNotIn("Message 43.", captured[0].text.split("[/Canonical Room context]", 1)[1])

    def test_the_topic_is_trimmed_as_guidance_is(self):
        topic = {"title": "\ufeff Heat plan \u2028", "description": "\ufeffBrief \ud800 text\u3000"}
        self.assertEqual(context_module.active_topic({"activeTopic": topic}), ("Heat plan", "Brief \ufffd text"))
        text = build_message({**HUMAN, "id": "m-56", "seq": 56}, topic=topic).text
        self.assertIn("Current discussion: Heat plan\nBrief \ufffd text\n", text)

    def test_a_skip_whose_settle_is_retried_does_not_renew(self):
        # r3 N1 j: the skip path of B1.
        attempt = cycle_attempt()
        instance, binding, api, _published, _dispatched = room_instance(attempt)
        settles = []
        complete = api.complete_discussion_attempt

        def flaky_complete(*args):
            settles.append(api.claims)
            if len(settles) <= 3:
                raise edge_error(502)
            return complete(*args)

        api.complete_discussion_attempt = flaky_complete

        def sleep(delay, result=None):
            caller = sys._getframe(1).f_code.co_name
            return REAL_SLEEP(0.05 if caller == "_complete_cycle_attempt" else 0.002, result)

        async def run():
            instance._start_attempt_renewal(binding, "evt-5", attempt)
            deadline = time.monotonic() + 10
            while api.claims < 2 and time.monotonic() < deadline:
                await REAL_SLEEP(0.005)
            return await instance._send_final("room-1", source_ref(), '{"action":"skip"}')

        with mock.patch.object(asyncio, "sleep", sleep), self.assertLogs(adapter.logger, level="INFO"):
            result = asyncio.run(run())
        self.assertTrue(result.success)
        self.assertEqual(api.posts, [])
        self.assertEqual([payload["action"] for _id, payload in api.completions], ["pass"])
        self.assertEqual(len(settles), 4)
        self.assertEqual({*settles, api.claims}, {settles[0]})

    def test_a_coded_429_after_a_run_is_retried(self):
        # r3 N1 q2: the 429 half of "a coded 5xx or 429 is retried".
        for status, code, expected in ((429, "rate_limited", True), (503, "busy", True),
                                       (409, "cycle_superseded", False), (404, "not_found", False)):
            error = adapter.ProtocolError("refused", status=status, code=code,
                                          retryable=status == 429 or status >= 500)
            self.assertEqual(adapter._uncoded_contact_failure(error), expected, (status, code))

    def test_a_redirect_with_the_rooms_json_is_not_the_rooms_answer(self):
        # r3 N1 q3 (request path) and q8 (stream route).
        body = json.dumps({"code": "credential_revoked", "message": "moved"}).encode()

        def redirect(*_args, **_kwargs):
            raise protocol.urllib.error.HTTPError("https://room.example/api/x", 302, "Found",
                                                  {"Location": "https://elsewhere.example/"}, __import__("io").BytesIO(body))

        api = protocol.RoomProtocol("https://room.example/api", "credential")
        with mock.patch.object(protocol, "_urlopen", redirect):
            for call in (lambda: api.request("GET", "/rooms/room-1/state"),
                         lambda: api.stream_events("room-1", 0, lambda _event: True)):
                with self.assertRaises(adapter.ProtocolError) as raised:
                    call()
                self.assertEqual((raised.exception.code, raised.exception.retryable, raised.exception.revoked),
                                 ("", True, False))


def _wrap_start(call, start):
    """Route POST /cycles to ``start``; every other Room call as before."""
    async def wrapped(binding, operation):
        class Proxy:
            def __init__(self, inner):
                self.inner = inner

            def __getattr__(self, name):
                if name == "start_discussion_cycle":
                    return lambda _room, request: start(request)
                return getattr(self.inner, name)

        return await call(binding, lambda api: operation(Proxy(api)))
    return wrapped


def _lease_in(seconds):
    from test_parity_p1 import lease_in
    return lease_in(seconds)


if __name__ == "__main__":
    unittest.main()
