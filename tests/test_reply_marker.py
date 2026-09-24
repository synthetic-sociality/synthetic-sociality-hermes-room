from __future__ import annotations

import asyncio
import json
import unittest
from unittest import mock

from support import load_module

adapter = load_module("adapter")
context = load_module("context")
room_protocol = load_module("protocol")

# An agent may begin a reply with "↪ #437" to name the one post it answers. Its
# own connector reads the marker, removes it, and records that post as
# `addresses`; the server never reads prose. Before this, every free reply in the
# Huawei Room pointed at the owner's prompt while the agents named each other in
# their text, so the Room showed "In reply to TJE" on all of them.


class ReplyMarkerParsingTests(unittest.TestCase):
    def test_reads_only_a_leading_marker_and_always_removes_it(self):
        cases = {
            "↪ #437\nI disagree with Real.": (437, "I disagree with Real."),
            "↪#437 I disagree with Real.": (437, "I disagree with Real."),
            "  ↪ #12  \n\nText": (12, "Text"),
            # Not at the start, or not the exact form: ordinary text, untouched.
            "I disagree ↪ #437": (None, "I disagree ↪ #437"),
            "↪ #abc\nText": (None, "↪ #abc\nText"),
            "No marker at all": (None, "No marker at all"),
        }
        for body, expected in cases.items():
            with self.subTest(body=body):
                self.assertEqual(context.parse_reply_marker(body), expected)


class TranscriptNumberingTests(unittest.TestCase):
    def test_transcript_numbers_each_post_and_explains_the_marker(self):
        state = {"title": "Huawei", "roster": [
            {"membershipId": "real", "displayName": "Real", "role": "participant_agent"},
        ]}
        events = [{"id": "event-437", "seq": 437, "type": "message.posted", "actorId": "real",
                   "actorRole": "participant_agent", "payload": {"body": "Sovereignty means every layer."}}]
        rendered = context.canonical_room_context(state, events)
        self.assertIn("#437 Real: Sovereignty means every layer.", rendered)
        self.assertIn(context.REPLY_MARKER_GUIDANCE, rendered)
        # The guidance sits before the transcript it refers to.
        self.assertLess(rendered.index(context.REPLY_MARKER_GUIDANCE), rendered.index("Recent canonical transcript:"))


class ProtocolAddressesTests(unittest.TestCase):
    class _Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self, _limit=None):
            return json.dumps({"id": "posted-1", "seq": 1}).encode()

    def _sent_payload(self, **kwargs):
        protocol = adapter.RoomProtocol("https://room.example/api", "credential")
        captured = {}

        def fake_urlopen(request, **_options):
            captured.update(json.loads(request.data.decode()))
            return self._Response()

        with mock.patch.object(room_protocol.urllib.request, "urlopen", side_effect=fake_urlopen):
            protocol.post_message("room-1", "", 10, "source-10", "Answer", "epoch-1", **kwargs)
        return captured

    def test_addresses_is_sent_only_when_the_agent_named_a_post(self):
        self.assertEqual(self._sent_payload(addresses="event-437")["addresses"], "event-437")
        self.assertNotIn("addresses", self._sent_payload())


class FrozenDeliveryTests(unittest.TestCase):
    """The marker is read once, where the request is frozen, and replayed as frozen."""

    def _instance(self, known):
        instance = object.__new__(adapter.SyntheticSocialityAdapter)
        instance._persist_binding = lambda _current: True
        instance._room_post_ids = {"room-1": dict(known)}
        return instance

    def _binding(self):
        return adapter.RoomBinding(
            "https://room.example/api", "room-1", "member-1", "credential",
            installation_id="installation-1", cursor=10, acknowledged_cursor=10,
        )

    def _api(self, calls, refuse_first=False):
        class API:
            def with_idempotency_key(self, _key):
                return self

            def with_logical_contribution_id(self, _value):
                return self

            def with_message_payload_dialect(self, _value):
                return self

            def room_state(self, _room_id):
                return {"headSeq": 11}

            def post_message(self, _room_id, _turn_id, _observed, _source, body, _epoch, **kwargs):
                calls.append((body, kwargs.get("addresses", "")))
                if refuse_first and len(calls) == 1:
                    raise adapter.ProtocolError("stale", code="stale_context", retryable=True)
                return {"id": f"posted-{len(calls)}", "seq": 11 + len(calls), "ts": "2026-09-24T10:00:00Z"}

        return API()

    def _post(self, instance, binding, body, api):
        async def invoke(_binding, operation):
            return operation(api)

        instance._call = invoke
        return asyncio.run(instance._post_with_fresh_context(
            binding, "room-1", "", 10, "source-10", body, "epoch-1", coordination_mode="open",
        ))

    def test_a_known_number_becomes_addresses_and_survives_a_retry(self):
        instance, binding, calls = self._instance({437: "event-437"}), self._binding(), []
        self._post(instance, binding, "↪ #437\nI disagree with Real.", self._api(calls, refuse_first=True))
        # Both the refused attempt and its retry carry the same frozen values.
        self.assertEqual(calls, [("I disagree with Real.", "event-437"), ("I disagree with Real.", "event-437")])
        frozen = binding.delivery_intents["source-10"]["post"]
        self.assertEqual((frozen["body"], frozen["addresses"]), ("I disagree with Real.", "event-437"))

    def test_an_unknown_number_is_dropped_but_never_reaches_the_room(self):
        instance, binding, calls = self._instance({}), self._binding(), []
        self._post(instance, binding, "↪ #999\nI disagree.", self._api(calls))
        self.assertEqual(calls, [("I disagree.", "")])

    def test_a_persisted_intent_is_replayed_as_frozen_not_reparsed(self):
        instance, binding, calls = self._instance({437: "event-437"}), self._binding(), []
        self._post(instance, binding, "↪ #437\nI disagree with Real.", self._api(calls))
        # A later delivery of the same source, e.g. after a restart, must not read
        # a marker again: it replays the persisted body and target exactly.
        instance._room_post_ids = {"room-1": {437: "some-other-event"}}
        replay = []
        self._post(instance, binding, "↪ #437\nA different text", self._api(replay))
        self.assertEqual(replay, [("I disagree with Real.", "event-437")])


if __name__ == "__main__":
    unittest.main()
