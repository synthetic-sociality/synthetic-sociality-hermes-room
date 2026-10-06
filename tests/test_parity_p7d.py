"""Parity P7d "Discussion documents: once in full, then on demand": contract
section P7d (conformance ``parityP7dCases``; OpenClaw implements the same
names).

The Room state's ``activeTopic.documents`` (Room server ebad3df and later)
names the documents of the discussion. No test uses a real credential or talks
to a live Room.
"""

from __future__ import annotations

import asyncio
import copy
import dataclasses
import hashlib
import json
import logging
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

from test_delivery_lifecycle import ROOT, adapter
from test_parity_p1 import cycle_attempt, room_instance, completion_event
from test_parity_p1a import HUMAN, PEER, START, attempt_ready

PACKAGE = adapter.__name__.rsplit(".", 1)[0]
# Looked up, not required: on a connector without P7d each case fails on its own.
documents = sys.modules.get(PACKAGE + ".documents") or types.SimpleNamespace()
room_tools = sys.modules[PACKAGE + ".room_tools"]
state_module = sys.modules[PACKAGE + ".state"]
origin = sys.modules[PACKAGE + ".origin_context"]
local_status = adapter.local_status
CONFORMANCE = json.loads((ROOT / "conformance.json").read_text())
TOOL = "synthetic_sociality_room_document"
HEADER = "[Room-shared document context"
FOOTER = "[/Room-shared document context]"


def generated_text(lines, count):
    """The shared text generator of the conformance vectors (same in OpenClaw)."""
    return "\n".join(f"{index:04d} {lines[index % len(lines)]}" for index in range(count))


LINES = CONFORMANCE["parityP7dTextLines"]
# The 2026-10-04 proof document had 22,709 characters.
PROOF_TEXT = generated_text(LINES, 520)[:22_709]
LARGE_TEXT = generated_text(LINES, 900)


def entry(artifact_id, version_id="v1", *, name=None, classification="room_shared", seq=42):
    return {"artifactId": artifact_id, "versionId": version_id, "name": name or f"{artifact_id}.md",
            "mimeType": "text/markdown", "size": 1, "sha256": "d" * 64, "sourceSeq": seq,
            "classification": classification}


def record(artifact_id, versions):
    """An artifact as GET /rooms/{room}/artifacts/{id} returns it."""
    items = []
    for version_id, (status, text) in versions.items():
        item = {"versionId": version_id, "extractionStatus": status}
        if text is not None:
            item["extractedText"] = text
        items.append(item)
    return {"artifactId": artifact_id, "visibility": "room_shared", "currentVersion": items[-1], "versions": items}


class Room:
    """The Room API a turn and the tool read; records every artifact read."""

    def __init__(self, records=None, library=(), *, documents_list=None, epoch_id="epoch-2", topic=True):
        self.records = dict(records or {})
        self.library = list(library)
        self.reads = []
        self.library_reads = 0
        self.documents_list = documents_list
        self.epoch_id = epoch_id
        self.topic = topic

    def state(self):
        state = {
            "title": "Showroom", "purpose": "Patient and doctor use case", "headSeq": 60,
            "activeEpoch": {"id": self.epoch_id, "startsAtSeq": 41}, "rules": [],
            "roster": [{"membershipId": "owner-1", "displayName": "Owner"},
                       {"membershipId": "peer-1", "displayName": "Peer"},
                       {"membershipId": "member-1", "displayName": "Agent"}],
        }
        if self.topic:
            topic = {"id": "topic-1", "title": "Disclosure", "status": "active", "description": None}
            if self.documents_list is not None:
                topic["documents"] = copy.deepcopy(self.documents_list)
            state["activeTopic"] = topic
        return state

    def artifact(self, _room, artifact_id):
        self.reads.append(artifact_id)
        value = self.records.get(artifact_id)
        if isinstance(value, Exception):
            raise value
        if value is None:
            raise adapter.ProtocolError("not found", status=404, code="not_found")
        return copy.deepcopy(value)

    def artifacts(self, _room):
        self.library_reads += 1
        return {"items": copy.deepcopy(self.library)}


def dispatch_turn(room, event, *, memory=None, attempt=None, context=(START, HUMAN, PEER), selection=None,
                  peek=None, keep=None):
    """The MessageEvent text the adapter hands to Hermes for ``event``, and the connector."""
    captured = []
    instance = object.__new__(adapter.SyntheticSocialityAdapter)
    instance.platform = adapter.Platform(adapter.NAME)
    instance._inflight_events, instance._event_seq, instance._event_epoch = set(), {}, {}
    instance._latest_source, instance._run_for_event, instance._epoch_handovers = {}, {}, {}
    instance._cycle_attempts = {event["id"]: attempt} if attempt else {}
    if selection is not None:
        instance._tool_selection = selection
    if peek is not None:
        # Core's own key for the Room thread, and the session id it maps to now.
        instance._event_session_key = lambda message: f"agent:main:synthetic_sociality:group:{message.source.thread_id}"
        instance._session_store = types.SimpleNamespace(
            peek_session_id=lambda key: peek if key.startswith("agent:main:synthetic_sociality:group:") else None)
    api = types.SimpleNamespace(
        room_state=lambda _room: room.state(),
        room_policy=lambda _room: {"policy": {}},
        events=lambda _room, after, _wait: {"events": [item for item in context if item["seq"] > after]},
        artifacts=room.artifacts,
        artifact=room.artifact,
    )
    instance._call = lambda _binding, operation: asyncio.sleep(0, result=operation(api))
    instance._publish = lambda *_args, **_kwargs: asyncio.sleep(0)
    instance.build_source = types.MethodType(
        lambda self, **kwargs: types.SimpleNamespace(platform=self.platform, **kwargs), instance,
    )
    instance.handle_message = lambda message: asyncio.sleep(0, result=captured.append(message))
    binding = adapter.RoomBinding("https://room.example/api", "room-1", "member-1", "credential",
                                  installation_id="inst-1", display_name="Agent")
    binding.epoch_session_routing_initialized = True
    binding.legacy_session_epoch_id = "epoch-1"
    binding.documents_given = copy.deepcopy(memory or {})
    persisted = []
    instance._persist_binding = lambda current: persisted.append(copy.deepcopy(current.documents_given)) or True
    instance._event_epoch[event["id"]] = room.epoch_id
    assert asyncio.run(instance._dispatch_room_event(binding, event, "generation"))
    if keep is not None:
        keep.append((instance, binding, persisted))
    return captured[0].text


def document_block(text):
    if HEADER not in text:
        return ""
    start = text.index(HEADER)
    return text[start:text.index(FOOTER, start) + len(FOOTER)]


def reference(name, artifact_id, version_id, size, *, given, tool=True):
    line = f"Document: {name} · artifact/version {artifact_id} / {version_id} · {size} characters"
    clauses = (["given in full on your first turn of this discussion"] if given else []) + (
        [f"read passages with {TOOL}"] if tool else [])
    return line + (" · " + "; ".join(clauses) if clauses else "")


class Discussion:
    """One agent's turns in one discussion, with the memory written after each run."""

    def __init__(self, case, room, *, selection=None, peek=None, context=(START, HUMAN, PEER)):
        self.case, self.room, self.selection, self.peek = case, room, selection, peek
        self.context = context
        self.memory = {}
        self.seq = 49

    def turn(self, *, mark=True, event=None, attempt=None, run_session=None):
        self.seq += 1
        event = event or attempt_ready(self.seq, 1, source="m-42")
        kept = []
        text = dispatch_turn(self.room, event, memory=self.memory, attempt=attempt, selection=self.selection,
                             peek=self.peek, keep=kept, context=self.context)
        instance, binding, _persisted = kept[0]
        if run_session is not None:
            adapter._on_pre_llm_call(platform="synthetic_sociality", session_id=run_session, sender_id="room-1")
        if mark:
            instance._mark_documents_given(binding, event["id"])
            self.memory = copy.deepcopy(binding.documents_given)
        return text


class ParityP7dTests(unittest.TestCase):
    def setUp(self):
        getattr(documents, "TEXT_CACHE", set()).clear()
        getattr(origin, "_SESSION_BY_ROOM", {}).clear()
        getattr(origin, "_ROOM_BY_SESSION", {}).clear()

    def test_p7d_00_cases_and_constants_are_pinned(self):
        cases = CONFORMANCE["parityP7dCases"]
        self.assertEqual(len(cases), 17)
        for name, method in cases.items():
            self.assertTrue(name.startswith("P7d-"), name)
            self.assertTrue(hasattr(self, method), method)
        constants = CONFORMANCE["parityP7dConstants"]
        self.assertEqual(constants, {
            "toolName": documents.DOCUMENT_TOOL_NAME, "fullTextLimit": documents.DOCUMENT_FULL_TEXT_LIMIT,
            "turnCharacterLimit": documents.DOCUMENT_TURN_CHARACTER_LIMIT,
            "toolResultLimit": documents.DOCUMENT_TOOL_RESULT_LIMIT,
            "queryCharacterLimit": documents.DOCUMENT_QUERY_CHARACTER_LIMIT,
            "contextLines": documents.DOCUMENT_CONTEXT_LINES, "givenLimit": documents.DOCUMENTS_GIVEN_LIMIT,
            "cacheVersions": documents.DOCUMENT_CACHE_VERSIONS, "cacheCharacters": documents.DOCUMENT_CACHE_CHARACTERS,
            "libraryLimit": documents.DOCUMENT_LIBRARY_LIMIT,
        })

    def test_p7d_01_first_turn_full_later_turns_reference_only(self):
        room = Room({"brief": record("brief", {"v1": ("ready", PROOF_TEXT)})}, documents_list=[entry("brief")])
        discussion = Discussion(self, room)
        first = discussion.turn()
        block = document_block(first)
        self.assertEqual(first.count(PROOF_TEXT.strip()), 1)
        self.assertIn("Document: brief.md\nArtifact/version: brief / v1\nMedia type: text/markdown\n"
                      f"SHA-256: {'d' * 64}\nExtraction status: ready.\nContent:\n{PROOF_TEXT.strip()}", block)
        self.assertNotIn(" characters · ", block)
        self.assertEqual(discussion.memory, {"epoch_id": "epoch-2", "host_session": "", "given": ["brief/v1"]})
        for _turn in range(2, 9):
            later = discussion.turn()
            block = document_block(later)
            self.assertNotIn(PROOF_TEXT[:200], later)
            self.assertEqual(block, "\n\n".join([
                documents.DOCUMENT_CONTEXT_HEADER,
                reference("brief.md", "brief", "v1", len(PROOF_TEXT), given=True),
                FOOTER]))
        # One Room read of the text for the whole discussion (P7d-5 cache).
        self.assertEqual(room.reads, ["brief"])

    def test_p7d_02_the_tool_returns_passages_and_coded_refusals(self):
        text = generated_text(LINES, 900)
        lines = text.split("\n")
        room = Room({
            "report": record("report", {"v1": ("ready", text)}),
            "pending": record("pending", {"v1": ("pending", None)}),
            "secret": adapter.ProtocolError("forbidden", status=403, code="forbidden"),
            "blank": record("blank", {"v1": ("ready", None)}),
            "gone": adapter.ProtocolError("not found", status=404, code="not_found"),
            "busy": adapter.ProtocolError("rate", status=429, code="rate_limited", retryable=True, retry_after=7),
            "edge": adapter.ProtocolError("proxy page", status=403, retryable=True, non_json=True),
            "down": adapter.ProtocolError("Room API unavailable", retryable=True),
            "guide": record("guide", {"v9": ("ready", "Guideline line one.\nGuideline line two.")}),
        }, library=[{"artifactId": "guide", "visibility": "room_shared",
                     "currentVersion": {"versionId": "v9", "name": "guide.pdf"}}],
           documents_list=[entry(name) for name in ("report", "pending", "secret", "blank", "gone", "busy", "edge", "down")])
        call = self.tool_caller(room)

        first = call({"artifactId": "report"})
        self.assertTrue(first["success"])
        self.assertEqual((first["name"], first["artifactId"], first["versionId"]), ("report.md", "report", "v1"))
        self.assertEqual((first["totalLines"], first["totalCharacters"]), (900, len(text)))
        self.assertLessEqual(len(first["text"]), documents.DOCUMENT_TOOL_RESULT_LIMIT)
        self.assertTrue(first["truncated"])
        returned = first["returnedLines"]
        self.assertEqual(returned[0], 1)
        self.assertEqual(first["nextFromLine"], returned[1] + 1)
        self.assertEqual(first["text"], "\n".join(f"{n}: {lines[n - 1]}" for n in range(1, returned[1] + 1)))
        self.assertEqual(first["note"], documents.DOCUMENT_RESULT_NOTE)

        ranged = call({"artifactId": "report", "fromLine": 10, "toLine": 12})
        self.assertEqual(ranged["text"], "\n".join(f"{n}: {lines[n - 1]}" for n in (10, 11, 12)))
        self.assertEqual((ranged["returnedLines"], ranged["truncated"], ranged["nextFromLine"]), ([10, 12], False, 13))
        tail = call({"artifactId": "report", "fromLine": 899})
        self.assertEqual((tail["returnedLines"], tail["truncated"], tail["nextFromLine"]), ([899, 900], False, None))
        beyond = call({"artifactId": "report", "fromLine": 5000})
        self.assertEqual((beyond["text"], beyond["returnedLines"], beyond["nextFromLine"]), ("", None, None))

        wanted = LINES[3].split()[1]
        found = call({"artifactId": "report", "query": wanted.upper()})
        matching = [n for n, line in enumerate(lines, 1) if wanted.lower() in line.lower()]
        self.assertEqual((found["query"], found["matches"]), (wanted.upper(), len(matching)))
        self.assertTrue(found["truncated"])
        self.assertLessEqual(len(found["text"]), documents.DOCUMENT_TOOL_RESULT_LIMIT)
        shown = [int(line.split(":", 1)[0]) for line in found["text"].split("\n") if line != "…"]
        self.assertEqual(shown[0], max(1, matching[0] - 3))
        self.assertEqual(found["returnedLines"], [shown[0], shown[-1]])
        self.assertIsNone(found["nextFromLine"])
        # Overlapping passages are joined; a gap is one line "…".
        self.assertNotIn("…\n…", found["text"])
        words = call({"artifactId": "report", "query": f"zz-not-there {wanted}"})
        self.assertEqual(words["matches"], len(matching))
        none = call({"artifactId": "report", "query": "zz-not-there qq"})
        self.assertEqual((none["matches"], none["text"], none["returnedLines"], none["truncated"]), (0, "", None, False))
        library = call({"artifactId": "guide"})
        self.assertEqual((library["versionId"], library["text"]), ("v9", "1: Guideline line one.\n2: Guideline line two."))

        for args in ({}, {"artifactId": ""}, {"artifactId": 7}, {"artifactId": "report", "fromLine": 0},
                     {"artifactId": "report", "fromLine": True}, {"artifactId": "report", "fromLine": 5, "toLine": 4},
                     {"artifactId": "report", "query": "x", "fromLine": 2},
                     {"artifactId": "report", "query": "x" * 201}, {"artifactId": "x" * 129},
                     {"artifactId": "report", "versionId": 3}):
            with self.subTest(arguments=args):
                self.assertEqual(call(args)["code"], "invalid_arguments")
        expected = {
            "elsewhere": ("not_in_discussion", {}), "pending": ("not_ready", {"extractionStatus": "pending"}),
            "secret": ("not_readable", {}), "blank": ("not_readable", {}), "gone": ("not_found", {}),
            "busy": ("rate_limited", {"retryAfter": 7}), "edge": ("unavailable", {}), "down": ("unavailable", {}),
        }
        for artifact_id, (code, extra) in expected.items():
            with self.subTest(refusal=artifact_id):
                answer = call({"artifactId": artifact_id})
                self.assertEqual((answer["success"], answer["code"]), (False, code))
                for key, value in extra.items():
                    self.assertEqual(answer[key], value)
                self.assertEqual(set(answer) - {"retryAfter", "extractionStatus"}, {"success", "code", "error"})
        self.assertEqual(call({"artifactId": "report", "versionId": "v7"})["code"], "not_in_discussion")

    def tool_caller(self, room, *, bindings=None, session_id=None):
        binding = adapter.RoomBinding("https://room.example/api", "room-1", "member-1", "credential")
        protocol = types.SimpleNamespace(room_state=lambda _room: room.state(), artifact=room.artifact,
                                         artifacts=room.artifacts)

        def select(requested):
            if bindings is not None:
                return bindings(requested)
            return binding, room.state(), []

        def call(args, **metadata):
            if session_id is not None:
                metadata.setdefault("session_id", session_id)
            with mock.patch.object(room_tools, "_select_room", side_effect=select), \
                    mock.patch.object(room_tools, "RoomProtocol", return_value=protocol):
                return json.loads(room_tools.room_document(args, **metadata))

        return call

    def test_p7d_03_a_large_document_goes_by_tool_from_the_start(self):
        room = Room({"long": record("long", {"v1": ("ready", LARGE_TEXT)})}, documents_list=[entry("long")])
        discussion = Discussion(self, room, selection={TOOL, "synthetic_sociality_room_history"})
        for _turn in range(3):
            text = discussion.turn()
            self.assertEqual(document_block(text), "\n\n".join([
                documents.DOCUMENT_CONTEXT_HEADER, reference("long.md", "long", "v1", len(LARGE_TEXT), given=False), FOOTER]))
            self.assertNotIn(LARGE_TEXT[:100], text)
        self.assertGreater(len(LARGE_TEXT), documents.DOCUMENT_FULL_TEXT_LIMIT)
        self.assertEqual(discussion.memory["given"], [])
        # 24,000 characters is still given in full; one more is not.
        exact = generated_text(LINES, 900)[:24_000]
        over = generated_text(LINES, 900)[:24_001]
        room = Room({"exact": record("exact", {"v1": ("ready", exact)}), "over": record("over", {"v1": ("ready", over)})},
                    documents_list=[entry("exact"), entry("over")])
        text = Discussion(self, room, selection={TOOL}).turn()
        self.assertIn(exact.strip(), text)
        self.assertIn(reference("over.md", "over", "v1", 24_001, given=False), text)
        self.assertNotIn(over, text)

    def test_p7d_04_documents_compete_for_the_turn_budget_newest_first_never_cut(self):
        texts = {name: (f"[{name}] " + generated_text(LINES, 2000))[:23_000] for name in ("d1", "d2", "d3")}
        texts["d4"] = "A short note."
        room = Room({name: record(name, {"v1": ("ready", text)}) for name, text in texts.items()},
                    documents_list=[entry(name) for name in ("d1", "d2", "d3", "d4")])
        discussion = Discussion(self, room, selection={TOOL})
        first = document_block(discussion.turn())
        for name in ("d1", "d2", "d4"):
            self.assertIn(f"Content:\n{texts[name].strip()}", first)
        self.assertNotIn("[d3]", first)
        self.assertLess(first.index("Artifact/version: d2"), first.index("Artifact/version: d4"))
        self.assertTrue(first.endswith(reference("d3.md", "d3", "v1", 23_000, given=False) + "\n\n" + FOOTER))
        self.assertEqual(discussion.memory["given"], ["d1/v1", "d2/v1", "d4/v1"])
        second = document_block(discussion.turn())
        self.assertIn(f"Content:\n{texts['d3'].strip()}", second)
        self.assertEqual(second.count("Content:"), 1)
        self.assertIn(reference("d1.md", "d1", "v1", 23_000, given=True), second)
        third = document_block(discussion.turn())
        self.assertNotIn("Content:", third)
        self.assertEqual(third.count("\nDocument: "), 4)

    def test_p7d_05_a_document_added_during_the_discussion_is_given_on_the_next_turn(self):
        room = Room({"a": record("a", {"v1": ("ready", "First document.")}),
                     "b": record("b", {"v1": ("ready", "Second document, added later.")})},
                    documents_list=[entry("a")])
        discussion = Discussion(self, room, selection={TOOL})
        self.assertIn("Content:\nFirst document.", discussion.turn())
        room.documents_list = [entry("b", seq=55), entry("a")]
        second = document_block(discussion.turn())
        self.assertIn("Content:\nSecond document, added later.", second)
        self.assertNotIn("First document.", second)
        self.assertIn(reference("a.md", "a", "v1", 15, given=True), second)
        self.assertNotIn("Content:", document_block(discussion.turn()))

    def test_p7d_06_a_new_version_is_given_again(self):
        room = Room({"a": record("a", {"v1": ("ready", "Version one."), "v2": ("ready", "Version two, revised.")})},
                    documents_list=[entry("a")])
        discussion = Discussion(self, room, selection={TOOL})
        self.assertIn("Content:\nVersion one.", discussion.turn())
        room.documents_list = [entry("a", "v2", seq=56), entry("a")]
        second = document_block(discussion.turn())
        self.assertIn("Artifact/version: a / v2\n", second)
        self.assertIn("Content:\nVersion two, revised.", second)
        self.assertIn(reference("a.md", "a", "v1", 12, given=True), second)
        self.assertEqual(discussion.memory["given"], ["a/v1", "a/v2"])
        # Without versionId the tool reads the first listed version.
        answer = self.tool_caller(room)({"artifactId": "a"})
        self.assertEqual((answer["versionId"], answer["text"]), ("v2", "1: Version two, revised."))
        self.assertEqual(self.tool_caller(room)({"artifactId": "a", "versionId": "v1"})["text"], "1: Version one.")

    def test_p7d_07_the_memory_survives_a_restart(self):
        room = Room({"brief": record("brief", {"v1": ("ready", PROOF_TEXT)})}, documents_list=[entry("brief")])
        discussion = Discussion(self, room, selection={TOOL})
        discussion.turn()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state" / "state.json"
            binding = adapter.RoomBinding("https://room.example/api", "room-1", "member-1", "credential")
            binding.documents_given = discussion.memory
            state_module.save(state_module.PluginState(bindings=[binding]), path)
            stored = json.loads(path.read_text())["bindings"][0]["documents_given"]
            self.assertEqual(stored, {"epoch_id": "epoch-2", "host_session": "", "given": ["brief/v1"]})
            loaded = state_module.load(path).binding("room-1")
            for invalid in ({"epoch_id": "epoch-2", "host_session": "", "given": ["k"] * 65},
                            {"epoch_id": "epoch-2", "host_session": "", "given": [], "names": ["brief.md"]},
                            {"epoch_id": "", "host_session": "", "given": []}, ["brief/v1"]):
                with self.subTest(invalid=invalid):
                    raw = json.loads(path.read_text())
                    raw["bindings"][0]["documents_given"] = invalid
                    broken = Path(directory) / "broken" / "state.json"
                    broken.parent.mkdir(exist_ok=True)
                    broken.write_text(json.dumps(raw))
                    with self.assertRaises(ValueError):
                        state_module.load(broken)
            # Written after the run through the connector's own persist merge.
            path = Path(directory) / "merged" / "state.json"
            fresh = adapter.RoomBinding("https://room.example/api", "room-1", "member-1", "credential")
            state_module.save(state_module.PluginState(bindings=[copy.deepcopy(fresh)]), path)
            instance = object.__new__(adapter.SyntheticSocialityAdapter)
            instance._persist_binding = types.MethodType(adapter.SyntheticSocialityAdapter._persist_binding, instance)
            instance._documents_offered = {"ready-9": {"epoch_id": "epoch-2", "host_session": "", "given": [],
                                                      "offered": ["brief/v1"]}}
            with mock.patch.object(adapter, "update", lambda mutator: state_module.update(mutator, path)):
                instance._mark_documents_given(fresh, "ready-9")
            self.assertEqual(state_module.load(path).binding("room-1").documents_given,
                             {"epoch_id": "epoch-2", "host_session": "", "given": ["brief/v1"]})
        documents.TEXT_CACHE.clear()  # a new process has no texts
        discussion.memory = copy.deepcopy(loaded.documents_given)
        after = discussion.turn()
        self.assertNotIn(PROOF_TEXT[:200], after)
        self.assertIn(reference("brief.md", "brief", "v1", len(PROOF_TEXT), given=True), after)

    def test_p7d_08_a_new_discussion_starts_the_memory_empty(self):
        room = Room({"brief": record("brief", {"v1": ("ready", "The brief.")})}, documents_list=[entry("brief")])
        discussion = Discussion(self, room, selection={TOOL})
        discussion.turn()
        self.assertNotIn("Content:", document_block(discussion.turn()))
        room.epoch_id = "epoch-3"
        self.assertIn("Content:\nThe brief.", discussion.turn())
        self.assertEqual(discussion.memory, {"epoch_id": "epoch-3", "host_session": "", "given": ["brief/v1"]})

    def test_p7d_09_a_changed_host_session_gives_the_text_again(self):
        room = Room({"brief": record("brief", {"v1": ("ready", "The brief.")})}, documents_list=[entry("brief")])
        discussion = Discussion(self, room, selection={TOOL}, peek="session-1")
        self.assertIn("Content:\nThe brief.", discussion.turn(run_session="session-1"))
        self.assertEqual(discussion.memory["host_session"], "session-1")
        self.assertNotIn("Content:", document_block(discussion.turn(run_session="session-1")))
        # The host replaced the session (reset, compression) before the turn.
        discussion.peek = "session-2"
        self.assertIn("Content:\nThe brief.", discussion.turn(run_session="session-2"))
        self.assertEqual(discussion.memory["host_session"], "session-2")
        # The host replaced it while the turn started: that turn had the
        # reference only, so the next one gives the text again.
        late = Discussion(self, room, selection={TOOL}, peek="session-1")
        late.turn(run_session="session-1")
        self.assertNotIn("Content:", document_block(late.turn(run_session="session-3")))
        self.assertEqual(late.memory, {"epoch_id": "epoch-2", "host_session": "session-3", "given": []})
        late.peek = "session-3"
        self.assertIn("Content:\nThe brief.", late.turn(run_session="session-3"))
        # Unknown sessions never reset the memory.
        unknown = Discussion(self, room, selection={TOOL})
        unknown.turn()
        self.assertNotIn("Content:", document_block(unknown.turn()))

    def test_p7d_10_the_closing_summary_turn_follows_the_same_rule(self):
        room = Room({"brief": record("brief", {"v1": ("ready", "The brief.")})}, documents_list=[entry("brief")])
        summary = attempt_ready(57, 1, source="term-56", phase="summary",
                                phaseInstruction="You are Agent in this Room. Write the closing summary.")
        attempt = {"cycle": {"id": "cycle-9", "generation": 1, "totalTurns": 0, "sourceEventId": "term-56",
                             "budgets": {"totalTurns": 1}}, "attempt": {"id": "attempt-9", "round": 1, "phase": "summary"}}
        # A summariser who did not debate: its first turn of the discussion.
        newcomer = Discussion(self, room, selection={TOOL})
        text = newcomer.turn(event=summary, attempt=attempt)
        self.assertIn("Write the closing summary.", text)
        self.assertIn("Content:\nThe brief.", text)
        # A summariser who debated: the reference line only.
        debater = Discussion(self, room, selection={TOOL})
        debater.turn()
        text = debater.turn(event={**summary, "id": "ready-58", "seq": 58}, attempt=attempt)
        self.assertNotIn("Content:", document_block(text))
        self.assertIn(reference("brief.md", "brief", "v1", 10, given=True), text)

    def test_p7d_11_without_the_tool_a_large_document_gives_its_head_once(self):
        small = "A short brief."
        room = Room({"small": record("small", {"v1": ("ready", small)}),
                     "long": record("long", {"v1": ("ready", LARGE_TEXT)})},
                    documents_list=[entry("small"), entry("long")])
        head = LARGE_TEXT[:24_000].strip()
        for label, selection, tool in (("unavailable", {"synthetic_sociality_room_history"}, False),
                                       ("unknown", None, True)):
            with self.subTest(tool=label):
                documents.TEXT_CACHE.clear()
                discussion = Discussion(self, room, selection=selection)
                first = document_block(discussion.turn())
                self.assertIn(f"Content (cut: the first 24000 of {len(LARGE_TEXT)} characters):\n{head}", first)
                self.assertNotIn(LARGE_TEXT[24_000:24_100], first)
                self.assertTrue(first.endswith(
                    reference("long.md", "long", "v1", len(LARGE_TEXT), given=False, tool=tool) + "\n\n" + FOOTER))
                self.assertEqual(discussion.memory["given"], ["small/v1", "long/v1"])
                second = document_block(discussion.turn())
                self.assertNotIn("Content", second)
                self.assertEqual(second, "\n\n".join([
                    documents.DOCUMENT_CONTEXT_HEADER,
                    reference("small.md", "small", "v1", len(small), given=True, tool=tool) + "\n"
                    + reference("long.md", "long", "v1", len(LARGE_TEXT), given=False, tool=tool), FOOTER]))
        self.assertEqual(reference("x", "a", "v", 3, given=False, tool=False), "Document: x · artifact/version a / v · 3 characters")
        # The status names the tool's availability, nothing else (P7d-6).
        with tempfile.TemporaryDirectory() as directory:
            status = local_status.LocalStatus(Path(directory) / "status.json", plugin="hermes",
                                              version=adapter.CONNECTOR_VERSION, host_label="host")
            for selection, value in (({TOOL}, "available"), (set(), "unavailable"), (None, "unknown")):
                instance = object.__new__(adapter.SyntheticSocialityAdapter)
                instance._tool_selection = selection
                status.set_document_tool(instance._document_tool_state())
                self.assertEqual(local_status.read(Path(directory) / "status.json")["documentTool"], value)

    def test_p7d_12_a_server_without_the_list_keeps_the_p5_rule(self):
        brief = {"brief": {"artifactId": "brief", "visibility": "room_shared",
                           "currentVersion": {"versionId": "v1", "name": "brief.md", "extractionStatus": "ready",
                                              "extractedText": "The P5 brief."}}}
        source = {"id": "m-42", "seq": 42, "type": "message.posted", "actorRole": "human_owner", "actorId": "owner-1",
                  "payload": {"body": "Brief attached.", "attachments": [
                      {"artifactId": "brief", "versionId": "v1", "name": "brief.md", "mediaType": "text/markdown",
                       "sha256": "c" * 64}]}}
        attempt = {"cycle": {"id": "cycle-1", "generation": 1, "totalTurns": 1, "sourceEventId": "m-42",
                             "budgets": {"totalTurns": 4}}, "attempt": {"id": "attempt-1", "round": 1}}
        for label, topic, listed in (("no activeTopic", False, None), ("no documents field", True, None)):
            with self.subTest(server=label):
                room = Room(brief, documents_list=listed, topic=topic)
                discussion = Discussion(self, room, selection={TOOL}, context=(START, source, PEER))
                for _turn in range(3):
                    text = discussion.turn(attempt=attempt, event=attempt_ready(discussion.seq + 1, 1, source="m-42"))
                    self.assertEqual(text.count("Content:\nThe P5 brief."), 1)
                    self.assertNotIn(" characters · ", text)
                self.assertEqual(discussion.memory, {})

    def test_p7d_13_a_pending_extraction_and_a_failed_run_mark_nothing(self):
        room = Room({"scan": record("scan", {"v1": ("pending", None)})}, documents_list=[entry("scan")])
        discussion = Discussion(self, room, selection={TOOL})
        first = document_block(discussion.turn())
        self.assertIn("Extraction status: pending.\nContent is not yet available to this membership.", first)
        self.assertEqual(discussion.memory["given"], [])
        room.records["scan"] = record("scan", {"v1": ("ready", "Scanned text.")})
        self.assertIn("Content:\nScanned text.", discussion.turn())
        self.assertEqual(room.reads, ["scan", "scan"])
        self.assertEqual(discussion.memory["given"], ["scan/v1"])
        # The mark follows the host run: a failed run marks nothing.
        for outcome, expected in (("failure", {}), ("success", {"epoch_id": "epoch-2", "host_session": "",
                                                                  "given": ["brief/v1"]})):
            with self.subTest(outcome=outcome):
                async def run():
                    instance, binding, _api, _published, _dispatched = room_instance(cycle_attempt())
                    persisted = []
                    instance._persist_binding = lambda current: persisted.append(copy.deepcopy(current.documents_given))
                    instance._documents_offered = {"evt-5": {"epoch_id": "epoch-2", "host_session": "", "given": [],
                                                             "offered": ["brief/v1"]}}
                    await instance.on_processing_complete(completion_event(binding), outcome)
                    return binding, persisted

                binding, persisted = asyncio.run(run())
                self.assertEqual(binding.documents_given, expected)
                self.assertTrue(all(item == expected for item in persisted), persisted)
                if expected:
                    self.assertTrue(persisted)

    def test_p7d_14_nothing_of_a_document_in_status_or_logs(self):
        secret_name = "Diagnosis-Maier-2026.md"
        text = "Confidential finding: stage II." + " Detail line." * 50
        room = Room({"doc": record("doc", {"v1": ("ready", text)}),
                     "long": record("long", {"v1": ("ready", LARGE_TEXT)})},
                    documents_list=[entry("doc", name=secret_name), entry("long", name=secret_name)])
        with self.assertLogs(level=logging.DEBUG) as captured:
            discussion = Discussion(self, room, selection={TOOL})
            discussion.turn()
            discussion.turn()
            self.tool_caller(room)({"artifactId": "doc", "query": "stage"})
            self.tool_caller(room)({"artifactId": "nowhere"})
        logged = "\n".join(captured.output)
        for fragment in (secret_name, "Confidential", "stage II", LARGE_TEXT[:40]):
            self.assertNotIn(fragment, logged)
        self.assertIn("documents for ready-50: full=1 head=0 reference=1 status=0 left_out=0 tool=available", logged)
        self.assertIn("document read: artifact=doc version=- code=ok", logged)
        self.assertIn("document read: artifact=nowhere version=- code=not_in_discussion", logged)
        self.assertEqual(set(discussion.memory), {"epoch_id", "host_session", "given"})
        self.assertNotIn(secret_name, json.dumps(discussion.memory))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "status.json"
            status = local_status.LocalStatus(path, plugin="hermes", version=adapter.CONNECTOR_VERSION, host_label="host")
            status.set_document_tool("available")
            status.room("room-1", "member-1", connection="connected")
            written = path.read_text()
            for fragment in (secret_name, "Confidential", "doc/v1", "brief"):
                self.assertNotIn(fragment, written)
            self.assertEqual(json.loads(written)["documentTool"], "available")

    def test_p7d_15_the_shared_vectors_render_identically(self):
        vector = CONFORMANCE["parityP7dRenderVector"]
        for case in vector["cases"]:
            with self.subTest(case=case["name"]):
                texts = {key: generated_text(LINES, spec["lines"])[spec.get("from", 0):spec.get("from", 0) + spec["characters"]]
                         if "lines" in spec else spec.get("text") for key, spec in vector["texts"].items()}

                def read(artifact_id, version_id, texts=texts):
                    spec = vector["texts"][f"{artifact_id}/{version_id}"]
                    if "error" in spec:
                        raise documents.DocumentReadError(spec["error"])
                    return spec.get("status", "ready"), texts[f"{artifact_id}/{version_id}"]

                listed = documents.discussion_documents({"activeTopic": {"documents": case["documents"]}})
                rendered, offered, _counts = documents.discussion_document_context(
                    listed, read, given=case["given"], tool=case["tool"], catalog=case.get("catalog", ""))
                self.assertEqual([line for line in rendered.split("\n") if " characters" in line and line.startswith("Document: ")],
                                 case["referenceLines"])
                self.assertEqual(offered, case["offered"])
                self.assertEqual((len(rendered), hashlib.sha256(rendered.encode()).hexdigest()),
                                 (case["characters"], case["sha256"]))
        tool_vector = CONFORMANCE["parityP7dToolVector"]
        for document in tool_vector["documents"]:
            text = generated_text(LINES, document["lines"]) if "lines" in document else "\n".join(
                item if isinstance(item, str) else item["repeat"] * item["times"] for item in document["textLines"])
            for case in document["calls"]:
                with self.subTest(tool=case["name"]):
                    arguments, refused = documents.tool_arguments(case["arguments"])
                    if refused is not None:
                        self.assertEqual(refused, case["expected"])
                        continue
                    answer = documents.document_passages(document["name"], "doc", "v1", text, arguments)
                    expected = dict(case["expected"])
                    digest = expected.pop("textSha256")
                    size = expected.pop("textCharacters")
                    body = answer.pop("text")
                    self.assertEqual(answer, expected)
                    self.assertEqual((len(body), hashlib.sha256(body.encode()).hexdigest()), (size, digest))

    def test_p7d_16_only_listed_or_catalog_versions_are_read(self):
        long_line = "Evidence " * 1200
        room = Room({
            "shared": record("shared", {"v1": ("ready", "Shared text.\n" + long_line)}),
            "hidden": record("hidden", {"v1": ("ready", "Owner-only text.")}),
            "meta": record("meta", {"v1": ("ready", "Metadata-only text.")}),
            "denied": adapter.ProtocolError("forbidden", status=403, code="forbidden"),
            "empty": record("empty", {"v1": ("ready", None)}),
            "other": record("other", {"v1": ("ready", "Another discussion's text.")}),
        }, library=[{"artifactId": "shared", "visibility": "room_shared",
                     "currentVersion": {"versionId": "v1", "name": "shared.md", "mediaType": "text/markdown",
                                        "sha256": "e" * 64, "extractionStatus": "ready"}}]
          + [{"artifactId": f"lib-{index}", "visibility": "room_shared",
              "currentVersion": {"versionId": "v1", "name": f"lib-{index}.pdf"}} for index in range(32)],
           documents_list=[entry("shared"), entry("hidden", classification="human_only"),
                           entry("meta", classification="metadata_only"), entry("denied"), entry("empty")])
        text = Discussion(self, room, selection={TOOL}).turn()
        block = document_block(text)
        self.assertEqual(room.reads, ["shared", "denied", "empty"])
        for absent in ("hidden", "meta", "denied", "empty", "Owner-only", "Metadata-only"):
            self.assertNotIn(absent, block)
        # The library catalog leaves out every listed exact version.
        self.assertNotIn("shared.md · artifact/version shared / v1", text)
        self.assertIn("lib-0.pdf · artifact/version lib-0 / v1", text)
        room.reads.clear()
        call = self.tool_caller(room)
        for artifact_id in ("other", "hidden", "meta", "lib-31"):
            with self.subTest(outside=artifact_id):
                self.assertEqual(call({"artifactId": artifact_id})["code"], "not_in_discussion")
        self.assertEqual(room.reads, [])
        self.assertEqual(call({"artifactId": "lib-30"})["code"], "not_found")
        # The bound: a line that does not fit alone is cut and ends in "…".
        cut = call({"artifactId": "shared", "fromLine": 2})
        self.assertEqual(len(cut["text"]), documents.DOCUMENT_TOOL_RESULT_LIMIT)
        self.assertTrue(cut["text"].startswith("2: Evidence ") and cut["text"].endswith("…"))
        self.assertEqual((cut["returnedLines"], cut["truncated"], cut["nextFromLine"]), ([2, 2], True, None))
        both = call({"artifactId": "shared"})
        self.assertEqual((both["text"], both["truncated"], both["nextFromLine"]), ("1: Shared text.", True, 2))
        # Inside a Room turn the tool reads the turn's own Room.
        origin.remember_room_session("session-7", "room-1")
        asked = []

        def bindings(requested):
            asked.append(requested)
            return adapter.RoomBinding("https://room.example/api", "room-1", "member-1", "credential"), room.state(), []

        self.tool_caller(room, bindings=bindings, session_id="session-7")({"artifactId": "shared"})
        self.tool_caller(room, bindings=bindings)({"artifactId": "shared", "room": "room-9"})
        self.assertEqual(asked, ["room-1", "room-9"])
        several = self.tool_caller(room, bindings=lambda _requested: (None, None, [{"roomId": "a"}, {"roomId": "b"}]))
        self.assertEqual(several({"artifactId": "shared"}), {"success": False, "selectionRequired": True,
                                                               "rooms": [{"roomId": "a"}, {"roomId": "b"}]})

    def test_p7d_17_the_tool_is_registered_and_declared(self):
        context = mock.MagicMock()
        adapter.register(context)
        tools = {call.kwargs["name"]: call.kwargs for call in context.register_tool.call_args_list}
        self.assertEqual(tools[TOOL]["toolset"], "synthetic_sociality_room")
        self.assertIs(tools[TOOL]["schema"], room_tools.ROOM_DOCUMENT_SCHEMA)
        self.assertIs(tools[TOOL]["handler"], room_tools.room_document)
        schema = room_tools.ROOM_DOCUMENT_SCHEMA["parameters"]
        self.assertEqual(sorted(schema["properties"]), ["artifactId", "fromLine", "query", "room", "toLine", "versionId"])
        self.assertEqual(schema["required"], ["artifactId"])
        self.assertIn("transform_tool_result", [call.args[0] for call in context.register_hook.call_args_list])
        manifest = (ROOT / "plugin.yaml").read_text()
        self.assertIn(f"  - {TOOL}\n", manifest)
        self.assertIn("  - transform_tool_result\n", manifest)
        without_version = "".join(line for line in manifest.splitlines(keepends=True) if not line.startswith("version:"))
        self.assertEqual(hashlib.sha256(without_version.encode()).hexdigest(), CONFORMANCE["parityP7dHermesManifestSha256"])
        # The Room session a turn runs in, as core reports it to pre_llm_call.
        adapter._on_pre_llm_call(platform="synthetic_sociality", session_id="session-4", sender_id="room-4")
        adapter._on_pre_llm_call(platform="telegram", session_id="session-5", sender_id="room-5")
        self.assertEqual((origin.room_for_session("session-4"), origin.host_session_for_room("room-4")),
                         ("room-4", "session-4"))
        self.assertEqual(origin.room_for_session("session-5"), "")


if __name__ == "__main__":
    unittest.main()
