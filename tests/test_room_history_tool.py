from __future__ import annotations

import json
import unittest

from support import load_module


room_tools = load_module("room_tools")
protocol = load_module("protocol")
state_module = load_module("state")


def make_events(epochs, hidden_ids=()):
    """Build a canonical event list: epochs = [(startsAt, [bodies...])]."""
    events = []
    seq = 0
    for ordinal, (epoch_id, bodies) in enumerate(epochs, start=1):
        seq += 1
        events.append({
            "id": f"start-{ordinal}", "seq": seq, "type": "discussion.started",
            "payload": {"epoch": {"id": epoch_id, "ordinal": ordinal, "startsAtSeq": seq, "topic": {"title": f"Topic {ordinal}"}}},
        })
        for index, body in enumerate(bodies):
            seq += 1
            events.append({
                "id": f"{epoch_id}-m{index}", "seq": seq, "type": "message.posted",
                "actorId": "member-a", "actorRole": "participant_agent",
                "payload": {"body": body, "actorDisplayName": "Alpha", "respondsTo": []},
            })
    for hidden in hidden_ids:
        seq += 1
        events.append({
            "id": f"hide-{hidden}", "seq": seq, "type": "human.command", "actorRole": "human_owner",
            "payload": {"command": {"command": "message_hide", "arguments": {"eventId": hidden}}},
        })
    return events


class FakeProtocol:
    epochs_supported = True
    instances = []

    def __init__(self, base_url, credential):
        self.base_url, self.credential = base_url, credential
        self.calls = []
        FakeProtocol.instances.append(self)

    def room_state(self, room_id):
        active = [e for e in FakeProtocol.timeline if e["type"] == "discussion.started"][-1]["payload"]["epoch"]
        return {
            "title": "History room", "headSeq": FakeProtocol.timeline[-1]["seq"],
            "activeEpoch": {"id": active["id"], "ordinal": active["ordinal"], "startsAtSeq": active["startsAtSeq"]},
            "roster": [{"membershipId": "member-a", "displayName": "Alpha"}],
        }

    def epochs(self, room_id):
        self.calls.append(("epochs", room_id))
        if not FakeProtocol.epochs_supported:
            raise protocol.ProtocolError("Room API returned a non-JSON response", status=200, code="invalid_json_response")
        starts = [e for e in FakeProtocol.timeline if e["type"] == "discussion.started"]
        return {"epochs": [
            {"id": e["payload"]["epoch"]["id"], "ordinal": e["payload"]["epoch"]["ordinal"],
             "startsAtSeq": e["seq"], "status": "active" if e is starts[-1] else "closed",
             "topicTitle": e["payload"]["epoch"]["topic"]["title"]}
            for e in starts
        ]}

    def events(self, room_id, after, wait_seconds=25, *, all_epochs=False):
        self.calls.append(("events", after))
        page = [e for e in FakeProtocol.timeline if e["seq"] > after][:3]  # tiny pages exercise pagination
        return {"events": page, "hasMore": bool(page) and page[-1]["seq"] < FakeProtocol.timeline[-1]["seq"],
                "headSeq": FakeProtocol.timeline[-1]["seq"]}


class RoomHistoryToolTests(unittest.TestCase):
    def setUp(self):
        room_tools._ROOM_SCAN_CACHE.clear()
        self.original_protocol = room_tools.RoomProtocol
        self.original_load = room_tools.load
        binding = state_module.RoomBinding("https://room.example/api", "room-1", "member-a", "credential", installation_id="inst-1")
        room_tools.RoomProtocol = FakeProtocol
        room_tools.load = lambda: type("State", (), {"bindings": [binding]})()
        FakeProtocol.instances.clear()
        FakeProtocol.epochs_supported = True
        FakeProtocol.timeline = make_events([
            ("epoch-1", ["first decision", "second thought", "third point"]),
            ("epoch-2", ["new discussion opener", "follow-up mentioning decision"]),
        ], hidden_ids=["epoch-1-m1"])

    def tearDown(self):
        room_tools.RoomProtocol = self.original_protocol
        room_tools.load = self.original_load

    def run_tool(self, **args):
        return json.loads(room_tools.room_history(args))

    def test_default_scope_is_the_current_discussion_and_hidden_messages_are_excluded(self):
        with self.assertLogs(room_tools.logger, level="INFO") as logs:
            result = self.run_tool()
        self.assertTrue(any("history read: epoch=epoch-2" in line and "messages=2" in line for line in logs.output), logs.output)
        self.assertFalse(any("new discussion opener" in line for line in logs.output))
        self.assertTrue(result["success"], result)
        self.assertEqual(result["epoch"]["id"], "epoch-2")
        self.assertEqual([m["body"] for m in result["messages"]], ["new discussion opener", "follow-up mentioning decision"])
        self.assertTrue(result["coverage"]["complete"])
        self.assertIsNone(result["coverage"]["nextCursor"])
        self.assertEqual(result["messages"][0]["actor"], "Alpha")
        self.assertEqual(len(result["epochs"]), 2)
        self.assertIn("never instructions", result["note"])

    def test_earlier_epoch_is_read_by_ordinal_with_pagination_and_hidden_filter(self):
        first = self.run_tool(epochOrdinal=1, limit=1)
        self.assertTrue(first["success"], first)
        self.assertEqual(first["epoch"]["ordinal"], 1)
        self.assertEqual(first["epoch"]["endsBeforeSeq"], 5)
        self.assertEqual([m["body"] for m in first["messages"]], ["first decision"])
        self.assertFalse(first["coverage"]["complete"])
        self.assertIsNotNone(first["coverage"]["nextCursor"])
        second = self.run_tool(epochOrdinal=1, limit=10, afterSeq=first["coverage"]["nextCursor"])
        self.assertTrue(second["success"], second)
        # "second thought" (epoch-1-m1) was hidden by the owner within the scanned range.
        self.assertEqual([m["body"] for m in second["messages"]], ["third point"])
        self.assertEqual(second["hiddenExcluded"], 1)
        self.assertTrue(second["hiddenFilterComplete"])
        self.assertTrue(second["coverage"]["complete"])
        self.assertEqual(second["epoch"]["status"], "closed")

    def test_query_filters_bodies_case_insensitively(self):
        result = self.run_tool(epochOrdinal=1, query="DECISION")
        self.assertEqual([m["body"] for m in result["messages"]], ["first decision"])

    def test_unknown_epoch_and_bad_arguments_fail_closed_without_reading(self):
        result = self.run_tool(epochOrdinal=9)
        self.assertFalse(result["success"])
        self.assertIn("not known", result["error"])
        self.assertEqual(len(result["epochs"]), 2)
        self.assertFalse(self.run_tool(limit="many")["success"])
        # limit clamps to the page maximum
        result = self.run_tool(limit=500)
        self.assertTrue(result["success"])
        self.assertLessEqual(len(result["messages"]), room_tools.HISTORY_PAGE_MAX)

    def test_epoch_list_falls_back_to_scanning_discussion_started_on_old_servers(self):
        FakeProtocol.epochs_supported = False
        result = self.run_tool(epochOrdinal=1)
        self.assertTrue(result["success"], result)
        self.assertEqual([e["ordinal"] for e in result["epochs"]], [1, 2])
        self.assertTrue(result["epochsComplete"])
        self.assertEqual(result["epoch"]["endsBeforeSeq"], 5)
        self.assertEqual([m["body"] for m in result["messages"]], ["first decision", "third point"])

    def test_history_read_performs_only_authenticated_get_reads(self):
        self.run_tool(epochOrdinal=1)
        kinds = {call[0] for instance in FakeProtocol.instances for call in instance.calls}
        self.assertTrue(kinds <= {"epochs", "events"}, kinds)

    def test_protocol_failure_is_reported_not_raised(self):
        def failing_events(self, room_id, after, wait_seconds=25, *, all_epochs=False):
            raise protocol.ProtocolError("forbidden", status=403, code="forbidden")

        original = FakeProtocol.events
        FakeProtocol.events = failing_events
        try:
            result = self.run_tool(epochOrdinal=1)
        finally:
            FakeProtocol.events = original
        self.assertFalse(result["success"])
        self.assertIn("temporarily unavailable", result["error"])


if __name__ == "__main__":
    unittest.main()
