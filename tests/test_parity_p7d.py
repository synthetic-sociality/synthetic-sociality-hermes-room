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
import ast
import http.server
import logging
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import threading
import types
import unittest
from pathlib import Path
from unittest import mock

from test_delivery_lifecycle import ROOT, adapter, configured_instance, lifecycle_binding
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
    clauses = (["given in full earlier in this discussion"] if given else []) + (
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
        getattr(documents, "TEXT_SIZES", set()).clear()
        getattr(origin, "_SESSION_BY_ROOM", {}).clear()
        getattr(origin, "_ROOM_BY_SESSION", {}).clear()

    def test_p7d_00_cases_and_constants_are_pinned(self):
        cases = CONFORMANCE["parityP7dCases"]
        self.assertEqual(len(cases), 29)
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
            "sizeVersions": getattr(documents, "DOCUMENT_SIZE_VERSIONS", None),
            "idForm": getattr(getattr(documents, "ROOM_ID_FORM", None), "pattern", None) and
            f"^{documents.ROOM_ID_FORM.pattern}$",
            "givenClause": documents.GIVEN_CLAUSE,
            "roomMismatchError": getattr(documents, "ROOM_MISMATCH_ERROR", None),
            "toolDescription": room_tools.ROOM_DOCUMENT_SCHEMA["description"],
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
        # Ids not in the Room's id form are logged as "invalid" (P7d-7, P7d-19).
        self.assertIn("document read: artifact=invalid version=- code=ok", logged)
        self.assertIn("document read: artifact=invalid version=- code=not_in_discussion", logged)
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

    def two_rooms(self):
        """Rooms A and B of one agent, each recording every Room call."""
        rooms = {}
        for key, room_id, title in (("a", "rooma0000000001", "Alpha"), ("b", "roomb0000000002", "Beta")):
            calls = []
            room = Room({"doc" + key: record("doc" + key, {"v1": ("ready", f"Room {title} text.")})},
                        documents_list=[entry("doc" + key)])
            state = dict(room.state(), title=title)

            def recorded(name, value, calls=calls):
                def method(*args, **_kwargs):
                    calls.append(name)
                    return value(*args) if callable(value) else copy.deepcopy(value)
                return method

            protocol = types.SimpleNamespace(
                room_state=recorded("room_state", state), artifacts=recorded("artifacts", room.artifacts),
                artifact=recorded("artifact", room.artifact), events=recorded("events", {"events": [], "hasMore": False}),
                epochs=recorded("epochs", {"epochs": []}),
            )
            binding = adapter.RoomBinding("https://room.example/api", room_id, "member-" + key, "credential-" + key)
            rooms[key] = types.SimpleNamespace(id=room_id, title=title, calls=calls, protocol=protocol, binding=binding)
        bindings = [rooms["a"].binding, rooms["b"].binding]
        by_credential = {room.binding.credential: room.protocol for room in rooms.values()}
        patches = (mock.patch.object(room_tools, "_enabled_bindings", return_value=bindings),
                   mock.patch.object(room_tools, "RoomProtocol", side_effect=lambda _url, credential: by_credential[credential]))
        return rooms, patches

    def test_p7d_18_a_room_turn_acts_only_for_its_own_room(self):
        rooms, patches = self.two_rooms()
        origin.remember_room_session("session-a", rooms["a"].id)
        tools = {"document": room_tools.room_document, "history": room_tools.room_history,
                 "context": room_tools.room_context}
        with patches[0], patches[1]:
            for name, tool in tools.items():
                for other in (rooms["b"].id, rooms["b"].title, "beta"):
                    with self.subTest(tool=name, room=other):
                        answer = json.loads(tool({"artifactId": "docb", "room": other}, session_id="session-a"))
                        self.assertEqual(answer, {"success": False, "code": "room_mismatch",
                                                  "error": documents.ROOM_MISMATCH_ERROR})
                        self.assertEqual(rooms["b"].calls, [], "nothing of Room B is read")
            # The turn's own Room, by id, by title or without the argument.
            for own in (rooms["a"].id, "alpha", None):
                with self.subTest(own=own):
                    args = {"artifactId": "doca"} if own is None else {"artifactId": "doca", "room": own}
                    answer = json.loads(room_tools.room_document(args, session_id="session-a"))
                    self.assertEqual((answer["success"], answer["artifactId"]), (True, "doca"), answer)
                    self.assertFalse(json.loads(room_tools.room_history(
                        {key: value for key, value in args.items() if key == "room"}, session_id="session-a")).get("code"))
            self.assertEqual(rooms["b"].calls, [])
            # Room B's document named without the argument: not of this discussion.
            self.assertEqual(json.loads(room_tools.room_document({"artifactId": "docb"}, session_id="session-a"))["code"],
                             "not_in_discussion")
            self.assertEqual(rooms["b"].calls, [])
            # Outside a Room turn the argument selects among the agent's own Rooms.
            outside = json.loads(room_tools.room_document({"artifactId": "docb", "room": rooms["b"].id}, session_id="cli-1"))
            self.assertEqual((outside["success"], outside["text"]), (True, "1: Room Beta text."))
            self.assertIn("artifact", rooms["b"].calls)
            several = json.loads(room_tools.room_document({"artifactId": "doca"}, session_id="cli-1"))
            self.assertTrue(several["selectionRequired"])

    def test_p7d_19_a_log_line_carries_a_caller_supplied_id_only_in_the_room_id_form(self):
        rooms, patches = self.two_rooms()
        origin.remember_room_session("session-a", rooms["a"].id)
        # A document name, a sentence, a string with a line break, a carriage return.
        typed = ["Befund-Zeisig-Kardiologie.md", "Unique finding: violet heron syndrome stage IX.", "x\nINFO forged line",
                 "doca\r"]
        with patches[0], patches[1], self.assertLogs(room_tools.logger, level=logging.DEBUG) as captured:
            for value in typed:
                room_tools.room_document({"artifactId": value}, session_id="session-a")
                room_tools.room_document({"artifactId": "doca", "versionId": value}, session_id="session-a")
                room_tools.room_document({"artifactId": "doca", "room": value}, session_id="session-a")
                room_tools.room_history({"room": value, "query": value}, session_id="session-a")
            room_tools.room_document({"artifactId": "abcde12345fghij", "versionId": "v0000000000000a"}, session_id="session-a")
        messages = [record.getMessage() for record in captured.records]
        for message in messages:
            self.assertNotIn("\n", message)
            self.assertNotIn("\r", message)
            for value in typed:
                self.assertNotIn(value.strip(), message)
            for fragment in ("Befund", "violet", "forged", "Beta"):
                self.assertNotIn(fragment, message)
        self.assertIn(f"Room {rooms['a'].id} document read: artifact=invalid version=- code=not_in_discussion", messages)
        self.assertIn(f"Room {rooms['a'].id} document read: artifact=invalid version=invalid code=not_in_discussion", messages)
        self.assertIn(f"Room {rooms['a'].id} document refused: code=room_mismatch", messages)
        self.assertIn(f"Room {rooms['a'].id} history refused: code=room_mismatch", messages)
        self.assertIn(f"Room {rooms['a'].id} document read: artifact=abcde12345fghij version=v0000000000000a "
                      "code=not_in_discussion", messages)
        self.assertEqual(documents.loggable_id("abcde12345fghij"), "abcde12345fghij")
        for value in ("abcde12345fghij\n", "ABCDE12345FGHIJ", "abcde12345fghi", "abcde12345fghijk", "abcde-2345fghij", 15, None):
            self.assertEqual(documents.loggable_id(value), "invalid", value)

    def test_p7d_20_the_release_archive_installs_every_runtime_file_it_ships(self):
        # Review B1 (1.0.84 at be23f2f): install.sh did not copy documents.py.
        # The archive is built by the packager from a commit of this tree, its
        # own install.sh installs it into a scratch HERMES_HOME (no network,
        # hermes not on PATH), and what the signed manifest lists is compared.
        with tempfile.TemporaryDirectory() as directory:
            scratch = Path(directory)
            source = scratch / "source"
            shutil.copytree(ROOT, source, ignore=shutil.ignore_patterns(".git", "__pycache__"))
            git_env = {"PATH": "/usr/bin:/bin", "HOME": str(scratch), "GIT_CONFIG_NOSYSTEM": "1",
                       "GIT_AUTHOR_NAME": "test", "GIT_AUTHOR_EMAIL": "test@example.invalid",
                       "GIT_COMMITTER_NAME": "test", "GIT_COMMITTER_EMAIL": "test@example.invalid"}
            for command in (["init", "-q"], ["add", "-A"], ["commit", "-q", "-m", "release"]):
                subprocess.run(["git", "-C", str(source), *command], env=git_env, check=True, capture_output=True)
            built = subprocess.run([sys.executable, str(source / "tools" / "package-release.py"), "--output-dir",
                                    str(scratch / "out")], env=git_env, check=True, capture_output=True, text=True)
            release = json.loads(built.stdout)
            manifest = json.loads(Path(release["manifest"]).read_text())
            with tarfile.open(release["archive"]) as archive:
                archive.extractall(scratch / "extracted", filter="data")
            unpacked = scratch / "extracted" / Path(release["archive"]).name.removesuffix(".tar.gz")
            hermes_home = scratch / "hermes-home"
            installed = subprocess.run(["sh", str(unpacked / "install.sh")], capture_output=True, text=True,
                                       env={"PATH": "/usr/bin:/bin", "HOME": str(scratch), "HERMES_HOME": str(hermes_home)})
            self.assertEqual(installed.returncode, 0, installed.stderr)
            plugin = hermes_home / "plugins" / "synthetic-sociality-room"
            # (a) Every runtime file the manifest lists (all but tests/, docs/
            # and tools/, which no installer copies) is installed and byte-equal,
            # and nothing else is.
            runtime = {name: digest for name, digest in manifest["files"].items()
                       if not name.startswith(("tests/", "docs/", "tools/"))}
            self.assertIn("documents.py", runtime)
            on_disk = {path.relative_to(plugin).as_posix() for path in plugin.rglob("*") if path.is_file()}
            self.assertEqual(on_disk, set(runtime))
            for name, digest in runtime.items():
                self.assertEqual(hashlib.sha256((plugin / name).read_bytes()).hexdigest(), digest, name)
            # (b) Every module the installed package imports resolves, and the
            # installed plugin imports.
            for path in plugin.glob("*.py"):
                for node in ast.walk(ast.parse(path.read_text(), str(path))):
                    if isinstance(node, ast.ImportFrom) and node.level:
                        targets = [node.module] if node.module else [alias.name for alias in node.names]
                        for target in targets:
                            self.assertTrue((plugin / f"{target.split('.')[0]}.py").is_file(), f"{path.name}: .{target}")
            probe = ("import importlib.util, sys\n"
                     "spec = importlib.util.spec_from_file_location('ssr_installed', sys.argv[1] + '/__init__.py', "
                     "submodule_search_locations=[sys.argv[1]])\n"
                     "module = importlib.util.module_from_spec(spec); sys.modules['ssr_installed'] = module\n"
                     "spec.loader.exec_module(module)\n"
                     "print(callable(getattr(module, 'register', None)), sorted(name for name in sys.modules "
                     "if name.startswith('ssr_installed.')))\n")
            loaded = subprocess.run([sys.executable, "-c", probe, str(plugin)], capture_output=True, text=True,
                                    env={**os.environ, "HOME": str(scratch), "HERMES_HOME": str(hermes_home)})
            self.assertEqual(loaded.returncode, 0, loaded.stderr[-2000:])
            self.assertTrue(loaded.stdout.startswith("True "), loaded.stdout)
            self.assertIn("ssr_installed.documents", loaded.stdout)

    def test_p7d_21_a_version_already_given_is_not_read_again(self):
        # Review N3: more listed documents than the text cache holds.
        names = [f"doc{index:02d}" for index in range(12)]
        room = Room({name: record(name, {"v1": ("ready", f"{name} text. " * 300)}) for name in names},
                    documents_list=[entry(name) for name in names])
        discussion = Discussion(self, room, selection={TOOL})
        first = discussion.turn()
        self.assertEqual(first.count("\nContent:\n"), 12)
        self.assertEqual(sorted(room.reads), names)
        for _turn in range(3):
            later = document_block(discussion.turn())
            self.assertEqual(later.count(" · given in full earlier in this discussion"), 12)
        self.assertEqual(len(room.reads), 12, "a given version is named without a Room read")
        # A process that holds no count (a restart) reads each version once.
        documents.TEXT_SIZES.clear()
        documents.TEXT_CACHE.clear()
        discussion.turn()
        discussion.turn()
        self.assertEqual(len(room.reads), 24)
        # A large document while the tool is available: read once, then by count.
        room = Room({"long": record("long", {"v1": ("ready", LARGE_TEXT)})}, documents_list=[entry("long")])
        documents.TEXT_CACHE.clear()
        large = Discussion(self, room, selection={TOOL})
        for _turn in range(3):
            self.assertIn(reference("long.md", "long", "v1", len(LARGE_TEXT), given=False), large.turn())
        self.assertEqual(room.reads, ["long"])
        # The count memory is bounded.
        sizes = documents.DocumentSizes(versions=3)
        for index in range(5):
            sizes.put(("scope", f"a{index}", "v1"), index)
        self.assertEqual([sizes.get(("scope", f"a{index}", "v1")) for index in range(5)], [None, None, 2, 3, 4])

    def test_p7d_22_bounds_of_memory_library_cache_result_and_head(self):
        # Review deferred 1: more than 64 given keys drop the oldest.
        keys = [f"a{index}/v1" for index in range(70)]
        memory = documents.given_memory("epoch-2", keys[:60], keys[60:], "s")
        self.assertEqual(memory["given"], keys[6:])
        # Deferred 2: a library entry of class human_only is refused, no read.
        room = Room({"private": record("private", {"v1": ("ready", "Owner only.")}),
                     "open": record("open", {"v1": ("ready", "Open text.")})},
                    library=[{"artifactId": "private", "visibility": "human_only",
                              "currentVersion": {"versionId": "v1", "name": "private.md"}},
                             {"artifactId": "open", "visibility": "restricted",
                              "currentVersion": {"versionId": "v1", "name": "open.md"}}],
                    documents_list=[])
        call = self.tool_caller(room)
        self.assertEqual(call({"artifactId": "private"})["code"], "not_in_discussion")
        self.assertEqual(room.reads, [])
        self.assertTrue(call({"artifactId": "open"})["success"])
        # Deferred 3: the same artifact id in two Rooms never shares a cache entry.
        fetched = []
        for room_id in ("rooma0000000001", "roomb0000000002"):
            def fetch(wanted, room_id=room_id):
                fetched.append(room_id)
                return record(wanted, {"v1": ("ready", f"Text of {room_id}.")})
            self.assertEqual(documents.read_version(("https://room.example/api", room_id), fetch, "same", "v1"),
                             ("ready", f"Text of {room_id}."))
        self.assertEqual(fetched, ["rooma0000000001", "roomb0000000002"])
        # Deferred 4: a result that fills the bound exactly is not cut.
        line = "x" * (documents.DOCUMENT_TOOL_RESULT_LIMIT - len("1: "))
        exact = documents.document_passages("exact.md", "exact", "v1", line + "\nsecond", {"fromLine": 1, "toLine": 1})
        self.assertEqual((len(exact["text"]), exact["truncated"], exact["text"][-1]),
                         (documents.DOCUMENT_TOOL_RESULT_LIMIT, False, "x"))
        over = documents.document_passages("over.md", "over", "v1", line + "x", {})
        self.assertEqual((len(over["text"]), over["truncated"], over["text"][-1]),
                         (documents.DOCUMENT_TOOL_RESULT_LIMIT, True, "…"))
        # Deferred 5: no head of a large document when less than 24,000
        # characters of the turn's budget remain.
        texts = {"one": "a" * 20_001, "two": "b" * 20_001, "big": "c" * 30_000}
        listed = documents.discussion_documents({"activeTopic": {"documents": [entry(key) for key in texts]}})
        rendered, offered, counts = documents.discussion_document_context(
            listed, lambda artifact_id, _version: ("ready", texts[artifact_id]), given=[], tool="unknown")
        self.assertEqual((offered, counts["full"], counts["head"]), (["one/v1", "two/v1"], 2, 0))
        self.assertNotIn("c" * 100, rendered)
        self.assertIn(reference("big.md", "big", "v1", 30_000, given=False), rendered)
        texts["two"] = "b" * 16_000
        rendered, offered, counts = documents.discussion_document_context(
            listed, lambda artifact_id, _version: ("ready", texts[artifact_id]), given=[], tool="unknown")
        self.assertEqual((offered, counts["head"]), (["one/v1", "two/v1", "big/v1"], 1))
        # OpenClaw review deferred 1: heads are charged to the budget.
        heads = {key: key * 30_000 for key in ("a", "b", "c")}
        many = documents.discussion_documents({"activeTopic": {"documents": [entry(key) for key in heads]}})
        _rendered, offered, counts = documents.discussion_document_context(
            many, lambda artifact_id, _version: ("ready", heads[artifact_id]), given=[], tool="unknown")
        self.assertEqual((offered, counts["head"]), (["a/v1", "b/v1"], 2))

    def test_p7d_23_two_agents_in_one_room_each_get_a_document_once(self):
        # OpenClaw review B2. A Hermes profile is one agent with its own
        # process, state file and session record, so two agents never share
        # the memory; here two connectors' turns interleave in one process.
        room = Room({"brief": record("brief", {"v1": ("ready", PROOF_TEXT)})}, documents_list=[entry("brief")])
        first, second = Discussion(self, room), Discussion(self, room)
        texts = {id(first): [], id(second): []}
        for _turn in range(8):
            for discussion in (first, second):
                texts[id(discussion)].append(discussion.turn())
        for turns in texts.values():
            self.assertEqual(sum(text.count(PROOF_TEXT.strip()) for text in turns), 1)
            self.assertIn(PROOF_TEXT.strip(), turns[0])

    def test_p7d_24_a_session_changed_unseen_after_a_restart_gives_the_documents_again(self):
        # OpenClaw review B3, Hermes review N1: the memory records session S1;
        # after a restart the turn is built without a known session and the
        # run reveals S2.
        room = Room({"brief": record("brief", {"v1": ("ready", PROOF_TEXT)})}, documents_list=[entry("brief")])
        discussion = Discussion(self, room)
        self.assertIn(PROOF_TEXT.strip(), discussion.turn(run_session="S1"))
        self.assertEqual(discussion.memory["host_session"], "S1")
        origin._SESSION_BY_ROOM.clear()
        origin._ROOM_BY_SESSION.clear()
        # The turn built before the change was seen: references (P7d-5).
        self.assertNotIn(PROOF_TEXT[:200], discussion.turn(run_session="S2"))
        self.assertEqual(discussion.memory, {"epoch_id": "epoch-2", "host_session": "S2", "given": []})
        # The next turn is a first turn again, then references.
        self.assertIn(PROOF_TEXT.strip(), discussion.turn())
        self.assertNotIn(PROOF_TEXT[:200], discussion.turn())
        self.assertEqual(discussion.memory["given"], ["brief/v1"])

    def test_p7d_25_a_run_that_ended_in_the_operational_fallback_marks_nothing(self):
        # OpenClaw review B4: the fallback is settled as fail although core
        # reports the delivery itself as done.
        cases = {
            "typed outcome": ("Provider unavailable.", {"operational_outcome": {
                "layer": "provider", "code": "server_error", "retryable": True}}),
            "generic fallback": (sorted(adapter._GATEWAY_OPERATIONAL_ERRORS)[0], {}),
            "model reply": ("A contribution of the model.", None),
        }
        for label, (content, metadata) in cases.items():
            with self.subTest(label):
                binding = lifecycle_binding()
                binding.delivery_intents.clear()
                binding.delivery_lifecycle.clear()
                binding.documents_given = {}
                cycle_attempt = {"cycle": {"id": "cycle-1", "generation": 3},
                                 "attempt": {"id": "attempt-1", "membershipId": "member-1"}}
                instance = configured_instance(binding, cycle_attempt, [])
                instance._documents_offered = {"evt-5": {"epoch_id": "epoch-1", "host_session": "", "recorded_session": "",
                                                         "given": [], "offered": ["brief/v1"]}}

                class API:
                    def room_state(self, _room_id):
                        return {"headSeq": 5, "activeEpoch": {"id": "epoch-1", "startsAtSeq": 1}}

                    def post_message(self, *_args, **_kwargs):
                        return {"event": {"id": "posted-6", "seq": 6, "ts": "2026-08-17T00:00:00Z"}}

                    def complete_discussion_attempt(self, *_args):
                        return {"state": "completed"}

                api = API()
                instance._call = lambda _binding, operation, api=api: asyncio.sleep(0, result=operation(api))
                instance._publish = lambda *_args, **_kwargs: asyncio.sleep(0)
                if metadata is not None:
                    result = asyncio.run(instance.send(binding.room_id, content,
                                                       reply_to=adapter._dispatch_source_ref("evt-5", "generation-1"),
                                                       metadata={"notify": True, **metadata}))
                    self.assertTrue(result.success, getattr(result, "error", None))
                # Core then reports the turn; the mark runs on success.
                instance._mark_documents_given(binding, "evt-5")
                expected = {} if metadata is not None else {"epoch_id": "epoch-1", "host_session": "", "given": ["brief/v1"]}
                self.assertEqual(binding.documents_given, expected)

    def test_p7d_26_an_overlong_id_never_reaches_the_state_and_a_failed_library_listing_does_not_fail_the_turn(self):
        long_id = "a" * 129
        room = Room({"brief": record("brief", {"v1": ("ready", "Brief text.")}),
                     long_id: record(long_id, {"v1": ("ready", "Long id text.")})},
                    documents_list=[entry(long_id), entry("brief", "v" * 129), entry("brief")])
        self.assertEqual([item["artifactId"] + "/" + item["versionId"]
                          for item in documents.discussion_documents(room.state())], ["brief/v1"])
        discussion = Discussion(self, room)
        text = discussion.turn()
        self.assertNotIn("Long id text.", text)
        self.assertEqual(room.reads, ["brief"])
        self.assertEqual(discussion.memory["given"], ["brief/v1"])
        self.assertTrue(documents.valid_given_memory(discussion.memory))
        # A memory that could not be loaded again is never written.
        kept = []
        dispatch_turn(room, attempt_ready(70, 1, source="m-42"), keep=kept)
        instance, binding, persisted = kept[0]
        # A key the state loader would refuse (over 2 x 128 + 1 characters).
        instance._documents_offered["ready-70"]["offered"] = [f"{'a' * 300}/v1"]
        with self.assertLogs(level=logging.WARNING):
            instance._mark_documents_given(binding, "ready-70")
        self.assertEqual(persisted, [])
        # Review N5: the library listing failing leaves the documents in place.
        failing = Room({"brief": record("brief", {"v1": ("ready", "Brief text.")})}, documents_list=[entry("brief")])
        failing.artifacts = lambda _room: (_ for _ in ()).throw(adapter.ProtocolError("library down", retryable=True))
        with self.assertLogs(level=logging.WARNING) as captured:
            block = document_block(Discussion(self, failing).turn())
        self.assertIn("Content:\nBrief text.", block)
        self.assertIn("document library unavailable: ProtocolError", "\n".join(captured.output))

    def test_p7d_27_the_tools_find_every_room_the_connector_runs_and_each_agent_sees_only_its_own(self):
        # OpenClaw review r2 B1, the Hermes rule: a profile's Rooms are the
        # bindings of its own state file, $HERMES_HOME/synthetic-sociality-room/
        # state.json, where the profile's home puts it; the connector
        # (adapter state_path()) and the tools (room_tools load()) read that one
        # file. Real state files of two profiles outside ~/.hermes, the
        # registered handlers and hook, a loopback Room double.
        requests = []
        rooms = {"rooma0000000001": ("Alpha", "doca00000000001"), "roomb0000000002": ("Beta", "docb00000000002"),
                 "roomc0000000003": ("Gamma", "docc00000000003")}

        class Answer(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                path = self.path.split("?")[0].split("/")
                room_id = path[3] if len(path) > 3 else ""
                requests.append((room_id, self.headers.get("Authorization", "").removeprefix("Bearer ")))
                title, doc = rooms.get(room_id, ("", ""))
                version = {"versionId": "v00000000000001", "name": f"{title}.md", "extractionStatus": "ready",
                           "extractedText": f"Text of {title}."}
                rest = path[4:]
                if not title:
                    status, body = 404, {"code": "not_found"}
                elif rest == ["state"]:
                    status, body = 200, dict(Room({}, documents_list=[dict(entry(doc, "v00000000000001"), name=f"{title}.md")],
                                                  epoch_id="epoch-z").state(), title=title)
                elif rest == ["artifacts"]:
                    status, body = 200, {"items": []}
                elif rest == ["artifacts", doc]:
                    status, body = 200, {"artifactId": doc, "visibility": "room_shared", "currentVersion": version,
                                         "versions": [version]}
                elif rest == ["events"]:
                    status, body = 200, {"events": [
                        {"id": "e-1", "seq": 1, "type": "discussion.started", "actorRole": "human_owner", "actorId": "o",
                         "payload": {"epoch": {"id": "epoch-z", "ordinal": 1, "startsAtSeq": 1}}},
                        {"id": "e-2", "seq": 2, "type": "message.posted", "actorRole": "human_owner", "actorId": "o",
                         "payload": {"body": f"Message in {title}.", "epochId": "epoch-z"}}], "hasMore": False, "headSeq": 3}
                else:
                    status, body = 404, {"code": "not_found"}
                data = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *_args):
                pass

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Answer)
        server.daemon_threads = True
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        base = f"http://127.0.0.1:{server.server_address[1]}/api"
        context = mock.MagicMock()
        adapter.register(context)
        handlers = {call.kwargs["name"]: call.kwargs["handler"] for call in context.register_tool.call_args_list}
        hooks = {call.args[0]: call.args[1] for call in context.register_hook.call_args_list}
        document, history = handlers[TOOL], handlers["synthetic_sociality_room_history"]
        with tempfile.TemporaryDirectory() as directory:
            profiles = {"zurie": Path(directory) / "profiles" / "zurie", "aura": Path(directory) / "var" / "aura-home"}
            owned = {"zurie": [("rooma0000000001", "cred-za"), ("roomb0000000002", "cred-zb")],
                     "aura": [("roomc0000000003", "cred-ac"), ("rooma0000000001", "cred-aa")]}
            for agent, home in profiles.items():
                path = home / "synthetic-sociality-room" / "state.json"
                bindings = [adapter.RoomBinding(base, room_id, f"m-{credential}", credential) for room_id, credential in owned[agent]]
                state_module.save(state_module.PluginState(bindings=bindings), path)

            def as_profile(agent):
                return mock.patch.dict(os.environ, {"HERMES_HOME": str(profiles[agent])})

            def call(handler, args, session_id):
                before = len(requests)
                return json.loads(handler(args, session_id=session_id)), requests[before:]

            with as_profile("zurie"):
                # The file the connector runs is the file the tools read.
                self.assertEqual(state_module.state_path(), profiles["zurie"] / "synthetic-sociality-room" / "state.json")
                hooks["pre_llm_call"](platform="synthetic_sociality", session_id="session-za", sender_id="rooma0000000001")
                answer, sent = call(document, {"artifactId": "doca00000000001"}, "session-za")
                self.assertEqual(answer.get("text"), "1: Text of Alpha.", answer)
                self.assertEqual(set(sent), {("rooma0000000001", "cred-za")})
                answer, sent = call(history, {}, "session-za")
                self.assertEqual([item["body"] for item in answer["messages"]], ["Message in Alpha."], answer)
                self.assertEqual(set(sent), {("rooma0000000001", "cred-za")})
                answer, sent = call(document, {"artifactId": "docb00000000002", "room": "roomb0000000002"}, "session-za")
                # The turn's own Room may be read before the refusal; nothing of
                # the other Room is requested.
                self.assertEqual(answer["code"], "room_mismatch")
                self.assertTrue(set(sent) <= {("rooma0000000001", "cred-za")}, sent)
                answer, sent = call(history, {}, "cli-zurie")
                self.assertEqual(sorted(item["roomId"] for item in answer["rooms"]), ["rooma0000000001", "roomb0000000002"])
                answer, sent = call(document, {"artifactId": "docc00000000003", "room": "roomc0000000003"}, "cli-zurie")
                self.assertFalse(answer["success"])
                self.assertNotIn("roomc0000000003", [room for room, _credential in requests])
            with as_profile("aura"):
                self.assertEqual(state_module.state_path(), profiles["aura"] / "synthetic-sociality-room" / "state.json")
                hooks["pre_llm_call"](platform="synthetic_sociality", session_id="session-aa", sender_id="rooma0000000001")
                answer, sent = call(document, {"artifactId": "doca00000000001"}, "session-aa")
                self.assertEqual(answer.get("text"), "1: Text of Alpha.", answer)
                self.assertEqual(set(sent), {("rooma0000000001", "cred-aa")})
                answer, sent = call(history, {}, "cli-aura")
                self.assertEqual(sorted(item["roomId"] for item in answer["rooms"]), ["rooma0000000001", "roomc0000000003"])
                answer, sent = call(document, {"artifactId": "docb00000000002", "room": "roomb0000000002"}, "cli-aura")
                self.assertFalse(answer["success"])
            # Every credential reached only its own Room; nothing of another
            # profile was read.
            self.assertEqual({credential: room for room, credential in requests},
                             {"cred-za": "rooma0000000001", "cred-aa": "rooma0000000001", "cred-zb": "roomb0000000002",
                              "cred-ac": "roomc0000000003"})

    def test_p7d_28_a_recorded_host_session_is_never_overwritten_by_a_run_whose_session_is_unknown(self):
        # OpenClaw review r2 N1, the same sequence in Hermes: a restart, a turn
        # whose run shows no session, a host reset, then the run shows S2.
        # B: core knows no session at the second turn and S2 at the third; C:
        # core knows none at all, only the runs show S1 and S2.
        for label, peek_after_restart, peek_after_reset in (("B", None, "S2"), ("C", None, None)):
            with self.subTest(label):
                documents.TEXT_CACHE.clear()
                origin._SESSION_BY_ROOM.clear()
                origin._ROOM_BY_SESSION.clear()
                room = Room({"brief": record("brief", {"v1": ("ready", PROOF_TEXT)})}, documents_list=[entry("brief")])
                discussion = Discussion(self, room, peek="S1" if label == "B" else None)
                self.assertIn(PROOF_TEXT.strip(), discussion.turn(run_session="S1"))
                origin._SESSION_BY_ROOM.clear()
                origin._ROOM_BY_SESSION.clear()
                discussion.peek = peek_after_restart
                self.assertNotIn(PROOF_TEXT[:200], discussion.turn())
                self.assertEqual(discussion.memory, {"epoch_id": "epoch-2", "host_session": "S1", "given": ["brief/v1"]})
                discussion.peek = peek_after_reset
                third = discussion.turn(run_session="S2")
                if peek_after_reset:
                    # Known before the turn: a first turn again.
                    self.assertIn(PROOF_TEXT.strip(), third)
                else:
                    # Seen only in the run: references, then the next turn gives it.
                    self.assertNotIn(PROOF_TEXT[:200], third)
                    self.assertEqual(discussion.memory, {"epoch_id": "epoch-2", "host_session": "S2", "given": []})
                    self.assertIn(PROOF_TEXT.strip(), discussion.turn(run_session="S2"))
                self.assertNotIn(PROOF_TEXT[:200], discussion.turn(run_session="S2"))
                self.assertEqual(discussion.memory, {"epoch_id": "epoch-2", "host_session": "S2", "given": ["brief/v1"]})

    def test_p7d_29_a_document_tool_deferred_behind_the_hosts_tool_search_is_available(self):
        # Canary 2026-10-08: core 7cadfaa66 defers plugin tools behind its Tool
        # Search bridge; the collapsed list names only the bridge tools.
        bridge = ["tool_search", "tool_describe", "tool_call"]
        catalog = ["terminal", "web_extract", TOOL, "synthetic_sociality_room_history"]
        calls = []

        def current_core(enabled_toolsets=None, disabled_toolsets=None, quiet_mode=False,
                         skip_tool_search_assembly=False):
            calls.append(skip_tool_search_assembly)
            names = catalog if skip_tool_search_assembly else ["terminal", "web_extract", *bridge]
            return [{"function": {"name": name}} for name in names]

        def older_core(enabled_toolsets=None, disabled_toolsets=None, quiet_mode=False):
            calls.append(None)
            return [{"function": {"name": name}} for name in older_names]

        def resolve(definitions):
            run = types.ModuleType("gateway.run")
            run._load_gateway_config = lambda: {"platform_toolsets": {adapter.NAME: ["synthetic_sociality_room"]}}
            tools_config = types.ModuleType("hermes_cli.tools_config")
            tools_config._get_platform_tools = lambda config, platform: set(config["platform_toolsets"][platform])
            core = types.ModuleType("model_tools")
            core.get_tool_definitions = definitions
            hermes_cli = sys.modules.get("hermes_cli") or types.ModuleType("hermes_cli")
            with mock.patch.dict(sys.modules, {"gateway.run": run, "hermes_cli": hermes_cli,
                                               "hermes_cli.tools_config": tools_config, "model_tools": core}):
                instance = object.__new__(adapter.SyntheticSocialityAdapter)
                instance._tool_selection = adapter._room_turn_tool_selection()
                return instance

        instance = resolve(current_core)
        self.assertEqual(calls, [True])
        self.assertEqual(instance._tool_selection, set(catalog))
        self.assertEqual(instance._document_tool_state(), "available")
        # Web reading keeps its value: core tools are never deferred.
        self.assertEqual(sorted(name for name in local_status.WEB_READ_TOOL_NAMES if name in instance._tool_selection),
                         ["web_extract"])
        # Available: a document over 24,000 characters goes by reference, naming the tool.
        room = Room({"long": record("long", {"v1": ("ready", LARGE_TEXT)})}, documents_list=[entry("long")])
        discussion = Discussion(self, room, selection=instance._tool_selection)
        for _turn in range(2):
            text = discussion.turn()
            self.assertEqual(document_block(text), "\n\n".join([
                documents.DOCUMENT_CONTEXT_HEADER, reference("long.md", "long", "v1", len(LARGE_TEXT), given=False),
                FOOTER]))
            self.assertNotIn("Content (cut:", text)
        # An older core without the parameter: the call as before, with its answer.
        for older_names, expected in ((catalog, "available"), (["terminal"], "unavailable")):
            with self.subTest(older_core=expected):
                calls.clear()
                instance = resolve(older_core)
                self.assertEqual(calls, [None])
                self.assertEqual(instance._tool_selection, set(older_names))
                self.assertEqual(instance._document_tool_state(), expected)


if __name__ == "__main__":
    unittest.main()
