"""Shared cases L1-15 and L1-16: P4 additions after the Room server release
joint-772ca32 (contract L1-12 and L1-13). OpenClaw implements the same names
in this release. No test uses a real credential or talks to a live Room.
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from test_delivery_lifecycle import adapter
from test_parity_p1 import cycle_attempt, room_instance, source_ref
from test_parity_p1a import DENY_PATTERN, START, attempt_ready, build_message, cycle
from test_parity_p2a import ROSTER, consume_instance, message

state_module = __import__(adapter.__name__.rsplit(".", 1)[0] + ".state", fromlist=["state"])

REPLY = "A reply that ran past the Room's word cap."
BYTE_CEILING = "message exceeds byte limit"  # the Room's text at 772ca32


def too_large(message_text=BYTE_CEILING):
    return adapter.ProtocolError(message_text, status=413, code="message_too_large", retryable=False)


class L1RoomServerTests(unittest.TestCase):
    def test_l1_15_message_too_large_is_terminal(self):
        for mode, attempt in (("open", None), ("coordinated", cycle_attempt())):
            with self.subTest(mode=mode):
                async def run(mode=mode, attempt=attempt):
                    instance, binding, api, published, _dispatched = room_instance(attempt, mode=mode)
                    api.post_error = lambda _n: too_large()
                    room_state = api.room_state
                    api.room_state = lambda room: {**room_state(room), "serverLimits": {"messageMaxBytes": 16}}
                    result = await instance._send_final("room-1", source_ref(), REPLY)
                    return instance, binding, api, published, result

                instance, binding, api, published, result = asyncio.run(run())
                self.assertTrue(result.success)
                terminal = instance._terminal_results[source_ref()]
                self.assertEqual((terminal["status"], terminal["reason"]), ("failed", "message_too_large"))
                # Never a quarantine: acknowledged through the normal path.
                self.assertNotEqual(binding.inbox.get("5"), "quarantined")
                self.assertEqual(binding.acknowledged_cursor, 5)
                self.assertIn(("evt-5", "terminal", "failed"), published)
                self.assertEqual(len(api.posts), 1)
                if attempt is None:
                    self.assertEqual(api.completions, [])
                else:
                    self.assertEqual(len(api.completions), 1)
                    payload = api.completions[0][1]
                    self.assertEqual(payload["action"], "fail")
                    self.assertEqual({key: payload["executionFailure"][key] for key in ("layer", "code")},
                                     {"layer": "runtime", "code": "response_too_large"})
                    key = adapter._attempt_authority_key(binding, "evt-5", attempt)
                    self.assertEqual(binding.delivery_authority[key]["state"], "superseded")
                # Kept under P2c-2 with the Room's byte ceiling, never a word count.
                kept = binding.kept_reply
                self.assertEqual((kept["reason"], kept["text"]), ("message_too_large (limit 16 bytes)", REPLY))
                offer = adapter._kept_reply_text(kept, None, {})
                self.assertIn("\nNot posted: message_too_large (limit 16 bytes)\n", offer)
                self.assertNotIn("words", offer.replace(REPLY, ""))
                self.assertEqual(DENY_PATTERN.findall(offer.replace(REPLY, "")), [])
        # The ceiling is named only when the body exceeds it (a cycle byte
        # budget below the ceiling, or an unknown ceiling, names only the code).
        self.assertEqual(adapter.too_large_kept_reason("x" * 20000, 16384), "message_too_large (limit 16384 bytes)")
        self.assertEqual(adapter.too_large_kept_reason("é" * 9000, 16384), "message_too_large (limit 16384 bytes)")
        self.assertEqual(adapter.too_large_kept_reason("x" * 900, 16384), "message_too_large")
        self.assertEqual(adapter.too_large_kept_reason("x" * 20000, 0), "message_too_large")
        self.assertEqual(adapter.post_refusal_outcome("message_too_large", False), "too_large")
        # OP-12 P4 r1 H2: a realistic reply over the Room's default ceiling is kept.
        long_reply = "word " * 3999 + "word"  # 19999 characters, over 16384 bytes

        async def long_run():
            instance, binding, api, _published, _dispatched = room_instance(None, mode="open")
            api.post_error = lambda _n: too_large()
            room_state = api.room_state
            api.room_state = lambda room: {**room_state(room), "serverLimits": {"messageMaxBytes": 16384}}
            await instance._send_final("room-1", source_ref(), long_reply)
            return binding

        kept = asyncio.run(long_run()).kept_reply
        self.assertEqual((kept["reason"], kept["text"]), ("message_too_large (limit 16384 bytes)", long_reply))

    def test_l1_16_missed_source_named_by_the_room(self):
        mention = message(2964, "owner-1", "human_owner", "@Zurie your view?", resolvedRecipientMembershipIds=["member-1"])
        named = message(2966, "owner-1", "human_owner", "And this one?", resolvedRecipientMembershipIds=["member-1"])
        newer = [message(seq, "paula-1", "participant_agent", f"Note {seq}.") for seq in (2967, 2968)]
        paula = message(2969, "paula-1", "participant_agent", "Zurie, over to you.", resolvedRecipientMembershipIds=["member-1"])
        page = (START, mention, named, *newer, paula)
        prepared = []

        def with_sources(entries):
            def prepare(instance, binding):
                binding.missed_sources = [dict(item) for item in entries]
                instance._persist_binding = lambda _current: True
                prepared.append(binding)
            return prepare

        local = [{"source_event_id": "msg-2964", "source_seq": 2964}]
        # The Room's field replaces the local record, which is cleared.
        text = build_message(attempt_ready(2970, 1, source="msg-2969", missedSourceEventId="msg-2966"),
                             attempt=cycle(used=0), roster=ROSTER, context=page, starts_at=2900,
                             prepare=with_sources(local)).text
        fact = "Missed earlier: #2966 TJE, 09:49 UTC; 3 newer messages since."
        self.assertEqual(text.count("Missed earlier"), 1, text)
        self.assertIn(f"\n{fact}\n", text)
        self.assertNotIn("#2964", text.split("Missed earlier")[1])
        marked = [{"source_event_id": "msg-2966", "source_seq": 0, "offered": True}]
        self.assertEqual(prepared[-1].missed_sources, marked)
        # Offered once: a later event repeating the field offers nothing.
        again = build_message(attempt_ready(2971, 1, source="msg-2969", missedSourceEventId="msg-2966"),
                              attempt=cycle(used=0), roster=ROSTER, context=page, starts_at=2900,
                              prepare=with_sources(marked)).text
        self.assertNotIn("Missed earlier", again)
        self.assertEqual(prepared[-1].missed_sources, marked)
        # An attempt-ready event without the field ends that memory.
        build_message(attempt_ready(2972, 1, source="msg-2969"), attempt=cycle(used=0), roster=ROSTER,
                      context=page, starts_at=2900, prepare=with_sources(marked))
        self.assertEqual(prepared[-1].missed_sources, [])
        # Without the field the local record is offered as before (P2a-3).
        plain = build_message(attempt_ready(2973, 1, source="msg-2969"), attempt=cycle(used=0), roster=ROSTER,
                              context=page, starts_at=2900, prepare=with_sources(local)).text
        self.assertIn("Missed earlier: #2964", plain)
        # The epoch floor: a named source before the active discussion is not offered.
        earlier = message(2900, "owner-1", "human_owner", "Before the restart.")
        restart = {**START, "id": "start-2950", "seq": 2950,
                   "payload": {"epoch": {**START["payload"]["epoch"], "startsAtSeq": 2950}}}
        late = message(2999, "paula-1", "participant_agent", "Zurie, your turn.", resolvedRecipientMembershipIds=["member-1"])
        floor = build_message(attempt_ready(3000, 1, source="msg-2999", missedSourceEventId="msg-2900"),
                              attempt=cycle(used=0), roster=ROSTER, context=(earlier, restart, late), starts_at=2950,
                              prepare=with_sources([])).text
        self.assertNotIn("Missed earlier", floor)
        # A field equal to the attempt's own source is no missed source.
        self.assertEqual(adapter._server_missed_source({"sourceEventId": "msg-1", "missedSourceEventId": "msg-1"}), "")
        self.assertEqual(adapter._server_missed_source({"sourceEventId": "msg-1", "missedSourceEventId": "msg-0"}), "msg-0")
        # The mark is durable and validated with the state.
        stored = adapter.RoomBinding("https://room.example/api", "room-1", "member-1", "credential", missed_sources=marked)
        self.assertEqual(adapter.RoomBinding.from_dict(dict(stored.__dict__)).missed_sources, marked)
        with self.assertRaises(ValueError):
            adapter.RoomBinding.from_dict({**stored.__dict__, "missed_sources": [{**marked[0], "offered": "yes"}]})
        self.assertEqual(DENY_PATTERN.findall(fact), [])
        # OP-12 P4 r1 item 2: a failed read leaves the Room's source unmarked.
        direct = object.__new__(adapter.SyntheticSocialityAdapter)
        binding = adapter.RoomBinding("https://room.example/api", "room-1", "member-1", "credential")

        async def unavailable(_binding, _operation):
            raise adapter.ProtocolError("unavailable", code="network", retryable=True)

        direct._call = unavailable
        with self.assertLogs(adapter.logger, level="WARNING"):
            result = asyncio.run(direct._missed_source_facts(binding, {"roster": ROSTER}, 2970, [named], 2900, set(),
                                                             server_missed=named["id"]))
        self.assertEqual(result, ([], [], ""))
        # OP-12 P4 r1 item 5: the epoch floor also holds for a source on the current page.
        before = message(2900, "owner-1", "human_owner", "Before the restart.")
        result = asyncio.run(direct._missed_source_facts(binding, {"roster": ROSTER}, 3000, [before], 2950, set(),
                                                         server_missed=before["id"]))
        self.assertEqual(result, ([], [before["id"]], before["id"]))
        binding.missed_sources = [{"source_event_id": before["id"], "source_seq": 0}]
        result = asyncio.run(direct._missed_source_facts(binding, {"roster": ROSTER}, 3000, [before], 2950, set()))
        self.assertEqual(result, ([], [before["id"]], ""))

    def test_l1_19_a_room_named_source_is_marked_only_after_its_offer(self):
        # OP-12 P4 r1 (Hermes H1, OpenClaw item 2): a transient failure while
        # offering leaves the Room's source unmarked, so a later turn offers it.
        import types
        named = message(2966, "owner-1", "human_owner", "And this one?", resolvedRecipientMembershipIds=["member-1"])
        newer = [message(seq, "paula-1", "participant_agent", f"Note {seq}.") for seq in (2967, 2968, 2969)]
        binding = adapter.RoomBinding("https://room.example/api", "room-1", "member-1", "credential")
        instance = object.__new__(adapter.SyntheticSocialityAdapter)
        instance._persist_binding = lambda _binding: True

        async def unavailable(_binding, _operation):
            raise adapter.ProtocolError("unavailable", code="network", retryable=True)

        instance._call = unavailable
        with self.assertLogs(adapter.logger, level="WARNING"):
            facts, offered, mark = asyncio.run(instance._missed_source_facts(
                binding, {"roster": ROSTER}, 2970, [named], 2900, set(), server_missed=named["id"]))
        self.assertEqual((facts, offered, mark), ([], [], ""))
        instance._clear_missed_sources(binding, offered, mark=mark, attempt_ready=True)
        self.assertEqual(binding.missed_sources, [])
        # The next turn, with the Room still naming it: offered, then marked.
        api = types.SimpleNamespace(events=lambda _room, after, _wait: {
            "events": [item for item in (named, *newer) if item["seq"] > after]})
        instance._call = lambda _binding, operation: asyncio.sleep(0, result=operation(api))
        facts, offered, mark = asyncio.run(instance._missed_source_facts(
            binding, {"roster": ROSTER}, 2970, [named], 2900, set(), server_missed=named["id"]))
        self.assertEqual(len(facts), 1)
        self.assertTrue(facts[0].startswith("Missed earlier: #2966 "), facts)
        instance._clear_missed_sources(binding, offered, mark=mark, attempt_ready=True)
        self.assertEqual(binding.missed_sources, [{"source_event_id": named["id"], "source_seq": 0, "offered": True}])

    def test_l1_17_unknown_tool_selection(self):
        # A core without parse_config_string_list (Hermes core c896c09c) or a
        # failing resolution reports no selection; it never raises.
        with mock.patch.dict(sys.modules, {"agent.skill_utils": None}):
            self.assertIsNone(adapter._core_tool_selection()[0])
        broken = type(sys)("model_tools")
        broken.get_tool_definitions = mock.Mock(side_effect=RuntimeError("core changed"))
        with mock.patch.dict(sys.modules, {"model_tools": broken}):
            self.assertIsNone(adapter._core_tool_selection()[0])
        with tempfile.TemporaryDirectory() as scratch:
            home = Path(scratch) / ".hermes"
            store = home / "synthetic-sociality-room"
            store.mkdir(parents=True)
            instance = object.__new__(adapter.SyntheticSocialityAdapter)
            instance._state_file = store / "state.json"
            secret_isolation = adapter.secret_isolation
            for backend, expected in (("local", ["host_shell_backend", "tool_selection_unavailable"]),
                                      ("docker", ["tool_selection_unavailable"])):
                with self.subTest(backend=backend), \
                        mock.patch.dict(os.environ, {"HERMES_HOME": str(home), "HOME": scratch, "TERMINAL_ENV": backend,
                                                     "TERMINAL_DOCKER_VOLUMES": "[]"}), \
                        mock.patch.object(adapter, "_core_tool_selection", lambda: (None, [])), \
                        mock.patch.object(secret_isolation, "_sandbox_mount_sources", lambda: []):
                    reasons = instance._secret_store_reasons()
                    self.assertEqual(reasons, expected)
                    # Reported, not fatal: the status says reachable with the code.
                    status = adapter.local_status.LocalStatus(store / "status.json", plugin="hermes", version="x", host_label="h")
                    status.set_secret_store("reachable" if reasons else "isolated", reasons)
                    written = adapter.local_status.read(store / "status.json")
                    self.assertEqual((written["secretStore"], written["secretStoreReasons"]), ("reachable", expected))
            # The enforcement switch: unreadable counts as on; readable non-true is off.
            class Hostile:
                @property
                def extra(self):
                    raise RuntimeError("config changed")

            switch = object.__new__(adapter.SyntheticSocialityAdapter)
            switch.config = Hostile()
            self.assertIs(switch._secret_isolation_enforced(), True)
            for extra, expected in (({}, False), ({"enforceSecretIsolation": "true"}, False),
                                    ({"enforceSecretIsolation": True}, True)):
                switch.config = type("Config", (), {"extra": extra})()
                self.assertIs(switch._secret_isolation_enforced(), expected, extra)
            # A readable selection adds no such code.
            with mock.patch.dict(os.environ, {"HERMES_HOME": str(home), "HOME": scratch, "TERMINAL_ENV": "docker",
                                              "TERMINAL_DOCKER_VOLUMES": "[]"}), \
                    mock.patch.object(adapter, "_core_tool_selection", lambda: ({"read_file"}, [])), \
                    mock.patch.object(secret_isolation, "_sandbox_mount_sources", lambda: []):
                self.assertEqual(instance._secret_store_reasons(), [])

    def test_l1_18_too_large_refusal_persists_through_the_real_writer(self):
        # OP-12 P4 r1 item 1, as a universal real-writer probe: the real
        # _persist_binding and state file (no snapshot stub), then a reload.
        import types
        for mode, attempt in (("open", None), ("coordinated", cycle_attempt())):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as scratch:
                path = Path(scratch) / ".hermes" / "synthetic-sociality-room" / "state.json"

                async def run(mode=mode, attempt=attempt):
                    instance, binding, api, _published, _dispatched = room_instance(attempt, mode=mode)
                    api.post_error = lambda _n: too_large()
                    room_state = api.room_state
                    api.room_state = lambda room: {**room_state(room), "serverLimits": {"messageMaxBytes": 16}}
                    state_module.save(adapter.PluginState(bindings=[binding]), path)
                    instance._state_file = path
                    instance._renewing_rooms = set()
                    instance._persist_binding = types.MethodType(adapter.SyntheticSocialityAdapter._persist_binding, instance)
                    with state_module.state_path_scope(path):
                        result = await instance._send_final("room-1", source_ref(), REPLY)
                    return api, result

                api, result = asyncio.run(run())
                self.assertTrue(result.success)
                reloaded = state_module.load(path).bindings[0]  # validated by the real loader
                self.assertEqual(reloaded.acknowledged_cursor, 5)
                self.assertNotEqual(reloaded.inbox.get("5"), "quarantined")
                self.assertEqual((reloaded.kept_reply["reason"], reloaded.kept_reply["text"]),
                                 ("message_too_large (limit 16 bytes)", REPLY))
                self.assertEqual(len(api.posts), 1)

    # Owner request 2026-10-03 (contract L1-16 to L1-18).
    def test_l1_22_a_coordinated_discussion_start_is_not_a_trigger(self):
        start = {**START, "seq": 2964}
        instance, binding, starts, completed, _persisted = consume_instance(mode="coordinated")
        asyncio.run(instance._consume(binding, start))
        self.assertEqual(starts, [])
        self.assertEqual((completed[-1]["terminal_status"], completed[-1]["reason"]),
                         ("ignored", "discussion_start_not_a_trigger"))
        # Open Rooms are unchanged: Hermes starts a cycle from a human start.
        instance, binding, starts, completed, _persisted = consume_instance(mode="open")
        asyncio.run(instance._consume(binding, start))
        self.assertEqual(len(starts), 1)
        # The topic reaches the agent with the first human message.
        text = build_message({**message(2965, "owner-1", "human_owner", "Should the doctor disclose it?")},
                             context=(START, message(2965, "owner-1", "human_owner", "Should the doctor disclose it?")),
                             starts_at=41, topic={"title": "Disclosure of a diagnosis"}).text
        self.assertIn("Current discussion: Disclosure of a diagnosis", text)

    def test_l1_23_every_human_role_is_a_cycle_source(self):
        for role in ("human", "human_owner", "human_member", "human_admin", "agent_owner"):
            self.assertTrue(adapter._is_human_cycle_source({"type": "message.posted", "actorRole": role}), role)
        for role in ("participant_agent", "room_master", "system", ""):
            self.assertFalse(adapter._is_human_cycle_source({"type": "message.posted", "actorRole": role}), role)
        # A Showroom Guest's message starts a cycle in a coordinated Room.
        guest = message(2964, "guest-1", "human_member", "@Agent your view?", resolvedRecipientMembershipIds=["member-1"])
        instance, binding, starts, completed, _persisted = consume_instance(mode="coordinated")
        asyncio.run(instance._consume(binding, guest))
        self.assertEqual(len(starts), 1)
        self.assertEqual(starts[0]["sourceEventId"], guest["id"])

    def test_l1_24_an_unknown_cycle_start_refusal_is_skipped(self):
        mention = message(2964, "owner-1", "human_owner", "@Agent your view?", resolvedRecipientMembershipIds=["member-1"])
        for code, status in (("source_not_eligible", 409), ("a_code_no_list_names", 422)):
            with self.subTest(code=code):
                instance, binding, starts, completed, _persisted = consume_instance(
                    start=lambda _r, code=code, status=status: (_ for _ in ()).throw(
                        adapter.ProtocolError("refused", status=status, code=code, retryable=False)))
                asyncio.run(instance._consume(binding, mention))
                self.assertEqual(len(starts), 1)
                self.assertEqual((completed[-1]["terminal_status"], completed[-1]["reason"]), ("skipped", code))
        # Retryable, 429 and session codes keep the retry path (raised, not skipped).
        for code, status, retryable in (("a_code_no_list_names", 422, True), ("a_code_no_list_names", 429, False),
                                        ("busy", 409, False), ("credential_renewal_pending", 409, False)):
            with self.subTest(code=code, status=status, retryable=retryable):
                error = adapter.ProtocolError("refused", status=status, code=code, retryable=retryable)
                self.assertFalse(adapter._is_cycle_start_refusal(error))


if __name__ == "__main__":
    unittest.main()
