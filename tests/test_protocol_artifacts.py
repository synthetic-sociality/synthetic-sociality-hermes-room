from __future__ import annotations

import unittest

from support import load_module


protocol = load_module("protocol")


class ArtifactProtocolTests(unittest.TestCase):
    def test_artifact_reads_use_authorized_quoted_room_routes(self):
        class Capture(protocol.RoomProtocol):
            def request(self, method, path, payload=None, *, credential=None):
                self.captured = (method, path, payload, credential)
                return {"artifactId": "artifact/1"}

        client = Capture("https://room.example/api", "credential")
        client.artifact("room one", "artifact/1")
        self.assertEqual(client.captured, (
            "GET", "/rooms/room%20one/artifacts/artifact%2F1", None, None,
        ))
        client.artifacts("room one")
        self.assertEqual(client.captured, (
            "GET", "/rooms/room%20one/artifacts", None, None,
        ))


if __name__ == "__main__":
    unittest.main()
