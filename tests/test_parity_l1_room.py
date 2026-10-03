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
from test_parity_p2a import ROSTER, message

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
                    api.room_state = lambda room: {**room_state(room), "serverLimits": {"messageMaxBytes": 16384}}
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
                self.assertEqual((kept["reason"], kept["text"]), ("message_too_large (limit 16384 bytes)", REPLY))
                offer = adapter._kept_reply_text(kept, None, {})
                self.assertIn("\nNot posted: message_too_large (limit 16384 bytes)\n", offer)
                self.assertNotIn("words", offer.replace(REPLY, ""))
                self.assertEqual(DENY_PATTERN.findall(offer.replace(REPLY, "")), [])
        # A byte number in the refusal wins; a word count is never named; an
        # unknown ceiling leaves only the code.
        self.assertEqual(adapter.too_large_kept_reason("allows at most 8000 bytes", 16384), "message_too_large (limit 8000 bytes)")
        self.assertEqual(adapter.too_large_kept_reason("allows at most 300 words per response"), "message_too_large")
        self.assertEqual(adapter.too_large_kept_reason(BYTE_CEILING, 0), "message_too_large")
        self.assertEqual(adapter.post_refusal_outcome("message_too_large", False), "too_large")

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
            # A readable selection adds no such code.
            with mock.patch.dict(os.environ, {"HERMES_HOME": str(home), "HOME": scratch, "TERMINAL_ENV": "docker",
                                              "TERMINAL_DOCKER_VOLUMES": "[]"}), \
                    mock.patch.object(adapter, "_core_tool_selection", lambda: ({"read_file"}, [])), \
                    mock.patch.object(secret_isolation, "_sandbox_mount_sources", lambda: []):
                self.assertEqual(instance._secret_store_reasons(), [])


if __name__ == "__main__":
    unittest.main()
