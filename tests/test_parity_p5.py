"""Parity release P5: cycle documents (contract section P5).

The shared case carries the name listed in ``conformance.json``
``parityP5Cases``; OpenClaw implements the same name. Incident (Showroom,
2026-10-03 21:25-21:32Z): a brief attached to the message that started a
debate reached the debaters only on the turns whose attempt named that
message; on every other turn they saw the library catalog only. No test uses
a real credential or talks to a live Room.
"""
from __future__ import annotations

import asyncio
import json
import logging
import unittest
from unittest import mock

from test_delivery_lifecycle import ROOT, adapter
from test_parity_p1a import START, attempt_ready, build_message

CONFORMANCE = json.loads((ROOT / "conformance.json").read_text())
context_module = __import__(adapter.__name__.rsplit(".", 1)[0] + ".context", fromlist=["context"])

BRIEF_TEXT = "Debate brief, owner's attachment." + " Position A holds that the evidence is decisive." * 450
PEER_TEXT = "Peer appendix: the counter-study of 2025."
OPEN_TEXT = "Open Room note: minutes of the last meeting."
DOCUMENTS = "[Room-shared document context"


def manifest(artifact_id, version_id="v1", name=None):
    return {"artifactId": artifact_id, "versionId": version_id, "name": name or f"{artifact_id}.md",
            "mediaType": "text/markdown", "sha256": "c" * 64}


def artifact(artifact_id, text, version_id="v1"):
    return {"artifactId": artifact_id, "visibility": "room_shared",
            "currentVersion": {"versionId": version_id, "name": f"{artifact_id}.md", "mediaType": "text/markdown",
                               "sha256": "c" * 64, "extractionStatus": "ready", "extractedText": text}}


SOURCE = {"id": "m-src", "seq": 42, "type": "message.posted", "actorRole": "human_owner", "actorId": "owner-1",
          "payload": {"body": "Debate: is the evidence decisive? Brief attached.", "attachments": [manifest("brief")]}}
ARTIFACTS = {"brief": artifact("brief", BRIEF_TEXT), "peer": artifact("peer", PEER_TEXT),
             "open": artifact("open", OPEN_TEXT)}


def claimed(source_id="m-src"):
    """The claim response as Room server 772ca32 returns it: {attempt, cycle}."""
    return {"cycle": {"id": "cycle-1", "generation": 1, "totalTurns": 1, "sourceEventId": source_id,
                      "budgets": {"totalTurns": 16, "perAgentTurns": 8}},
            "attempt": {"id": "attempt-1", "round": 1}}


def contribution(seq, actor="peer-1", attachments=None):
    payload = {"body": f"Contribution {seq}.", "resolvedRecipientMembershipIds": ["member-1"]}
    if attachments:
        payload["attachments"] = attachments
    return {"id": f"m-{seq}", "seq": seq, "type": "message.posted", "actorRole": "participant_agent",
            "actorId": actor, "payload": payload}


# Room traffic between the source message and the debate (cycle and activity
# events, no messages), so that later turns no longer load the source with
# their recent messages and resolve it by the bounded scan.
GAP = [{"id": f"sys-{seq}", "seq": seq, "type": "discussion.cycle_started", "actorRole": "system",
        "actorId": "room_coordinator", "payload": {"cycleId": f"cycle-{seq}"}} for seq in range(43, 200)]


def debate(turns):
    """Two debaters alternating after the owner's source message (seq 42)."""
    events, previous = [START, SOURCE, *GAP], "m-src"
    for turn in range(1, turns + 1):
        ready = attempt_ready(197 + 3 * turn, turn, source=previous)
        events.append(ready)
        yield turn, ready, list(events)
        events.append(contribution(198 + 3 * turn, actor="peer-1" if turn % 2 else "member-1"))
        previous = events[-1]["id"]
        # An audience remark (a message, no attachment) between turns.
        events.append({"id": f"m-{199 + 3 * turn}", "seq": 199 + 3 * turn, "type": "message.posted",
                       "actorRole": "human_member", "actorId": "guest-1", "payload": {"body": "Noted."}})


def scans_of_cycle_sources(calls):
    real = adapter._cycle_source_message

    def counting(*args, **kwargs):
        if kwargs.get("types") == adapter.CYCLE_DOCUMENT_SOURCE_TYPES:
            calls.append(args[1])
        return real(*args, **kwargs)

    return mock.patch.object(adapter, "_cycle_source_message", side_effect=counting)


def documents_block(text):
    start = text.index(DOCUMENTS) if DOCUMENTS in text else -1
    return "" if start < 0 else text[start:text.index("[/Room-shared document context]", start)]


class ParityP5Tests(unittest.TestCase):
    def test_p5_01_every_turn_of_a_cycle_carries_the_cycle_source_attachments(self):
        # Sixteen turns: the brief is in every turn's context, exactly once,
        # also when the source message has left the loaded window (scan).
        loaded, scanned = [], []
        for turn, ready, events in debate(16):
            with self.subTest(turn=turn):
                calls = []
                with scans_of_cycle_sources(calls):
                    text = build_message(ready, attempt=claimed(), context=events, artifacts=ARTIFACTS).text
                (scanned if calls else loaded).append(turn)
                self.assertEqual(text.count(BRIEF_TEXT), 1, turn)
                self.assertEqual(text.count("Artifact/version: brief / v1"), 1, turn)
        # Both resolution paths ran: the loaded context and the bounded scan.
        self.assertEqual(loaded[0], 1)
        self.assertTrue(scanned and scanned[-1] == 16, (loaded, scanned))

        # A peer message with its own attachment: both, the cycle source first.
        events = [START, SOURCE, contribution(43, attachments=[manifest("peer")])]
        ready = attempt_ready(44, 2, source="m-43")
        text = build_message(ready, attempt=claimed(), context=[*events, ready], artifacts=ARTIFACTS).text
        block = documents_block(text)
        self.assertEqual((block.count(BRIEF_TEXT), block.count(PEER_TEXT)), (1, 1))
        self.assertLess(block.index(BRIEF_TEXT), block.index(PEER_TEXT))
        # The same exact version attached again is rendered once.
        events = [START, SOURCE, contribution(43, attachments=[manifest("brief"), manifest("peer")])]
        text = build_message(ready, attempt=claimed(), context=[*events, ready], artifacts=ARTIFACTS).text
        self.assertEqual((text.count(BRIEF_TEXT), text.count(PEER_TEXT)), (1, 1))

        # Limits unchanged: 8 attachments, 64,000 characters, the cycle
        # source's first and the turn source's cut at the limit.
        many = [manifest(f"doc-{index}") for index in range(8)]
        library = {f"doc-{index}": artifact(f"doc-{index}", f"Document {index}.") for index in range(8)}
        crowded = {**SOURCE, "payload": {**SOURCE["payload"], "attachments": many}}
        events = [START, crowded, contribution(43, attachments=[manifest("peer")])]
        text = build_message(ready, attempt=claimed(), context=[*events, ready],
                             artifacts={**ARTIFACTS, **library}).text
        self.assertEqual(documents_block(text).count("Document: "), 8)
        self.assertIn("Document 7.", text)
        self.assertNotIn(PEER_TEXT, text)
        huge = {"brief": artifact("brief", "x" * 70_000)}
        events = [START, SOURCE, contribution(43, attachments=[manifest("peer")])]
        text = build_message(ready, attempt=claimed(), context=[*events, ready], artifacts={**ARTIFACTS, **huge}).text
        rendered = documents_block(text) + "[/Room-shared document context]"
        # The bound as before P5 (its accounting leaves out one separator).
        self.assertLessEqual(len(rendered), context_module.ARTIFACT_CONTEXT_CHARACTER_LIMIT + 2)
        self.assertIn("x…", rendered)
        self.assertNotIn(PEER_TEXT, text)

        # Unresolvable cycle source (outside the scan bound, or a Room error):
        # the turn proceeds without it and one coded line says so.
        events = [START, contribution(43, attachments=[manifest("peer")])]
        with self.assertLogs(adapter.logger, logging.WARNING) as logs:
            text = build_message(ready, attempt=claimed("m-gone"), context=[*events, ready],
                                 artifacts=ARTIFACTS).text
        self.assertIn(PEER_TEXT, text)
        self.assertNotIn(BRIEF_TEXT, text)
        self.assertEqual(sum("code cycle_source_unresolved" in line for line in logs.output), 1, logs.output)
        events = [START, SOURCE, *GAP, *[contribution(seq) for seq in range(200, 220)]]
        late = attempt_ready(220, 9, source="m-219")
        with mock.patch.object(adapter, "_cycle_source_message", side_effect=RuntimeError("room down")), \
                self.assertLogs(adapter.logger, logging.WARNING) as logs:
            text = build_message(late, attempt=claimed(), context=[*events, late], artifacts=ARTIFACTS).text
        self.assertNotIn(BRIEF_TEXT, text)
        self.assertIn("Contribution 219.", text)
        self.assertTrue(any("code cycle_source_unresolved" in line and "RuntimeError" in line for line in logs.output))

        # Outside a cycle (open Room, no claimed attempt): its own attachments only.
        human = {"id": "m-221", "seq": 221, "type": "message.posted", "actorRole": "human_owner", "actorId": "owner-1",
                 "payload": {"body": "Notes attached.", "attachments": [manifest("open")]}}
        text = build_message(human, context=[START, SOURCE, human], artifacts=ARTIFACTS).text
        block = documents_block(text)
        self.assertEqual((block.count(OPEN_TEXT), block.count(BRIEF_TEXT)), (1, 0))

    def test_cycle_source_is_read_once_per_cycle(self):
        # The resolved source is remembered per cycle: a later turn of the
        # same cycle does not scan again.
        kept, scans = [], []
        events = [START, SOURCE, *GAP, *[contribution(seq) for seq in range(200, 230)]]
        first = attempt_ready(230, 13, source="m-229")
        with scans_of_cycle_sources(scans):
            build_message(first, attempt=claimed(), context=[*events, first], artifacts=ARTIFACTS, keep_instance=kept)
            instance, binding = kept[0]
            second = attempt_ready(231, 14, source="m-229")
            instance._cycle_attempts = {second["id"]: claimed()}
            instance._event_epoch[second["id"]] = "epoch-2"
            captured = []
            instance.handle_message = lambda message: asyncio.sleep(0, result=captured.append(message))
            self.assertTrue(asyncio.run(instance._dispatch_room_event(binding, second, "generation-2")))
        self.assertEqual(scans, ["m-src"])
        self.assertEqual(captured[0].text.count(BRIEF_TEXT), 1)

    def test_case_names_resolve_to_tests(self):
        cases = CONFORMANCE["parityP5Cases"]
        self.assertEqual(len(cases), 1)
        sources = "\n".join(path.read_text() for path in (ROOT / "tests").glob("test_*.py"))
        for index, (name, method) in enumerate(cases.items(), start=1):
            self.assertTrue(name.startswith(f"P5-{index:02d} "), name)
            self.assertEqual(sources.count(f"def {method}(self"), 1, method)
        self.assertEqual(CONFORMANCE["parityP5Contract"], "docs/delivery-lifecycle-contract-v1.md#p5--cycle-documents-2026-10-03")


if __name__ == "__main__":
    unittest.main()
