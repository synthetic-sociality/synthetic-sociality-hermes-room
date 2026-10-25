"""P11b: the connector's product User-Agent on every Room request (contract P11b-1, shared cases P11b-2).

Offline: the real request path of ``protocol.RoomProtocol`` with ``_urlopen`` patched to capture the formed
request for each request kind P11b-1 names, and one loopback listener that records the header ``urllib`` actually
sends on the photo download. The cases cover the request kinds the connector sends today; a new request kind is added
here with its code (P11b-2), and future code is held to P11b-1 by review, not by a static scan. No Room, edge or
provider contact."""

from __future__ import annotations

import ast
import http.server
import io
import json
import re
import threading
import unittest
import urllib.request
from pathlib import Path
from unittest import mock

from support import load_module


ROOT = Path(__file__).resolve().parents[1]
protocol = load_module("protocol")
CONFORMANCE = json.loads((ROOT / "conformance.json").read_text(encoding="utf-8"))
CONTRACT = (ROOT / "docs" / "delivery-lifecycle-contract-v1.md").read_text(encoding="utf-8")
PRODUCT = "synthetic-sociality-hermes/1.0"
CASES = [
    "every Room request carries the connector user agent",
    "photo download carries the connector user agent on the wire",
]
PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 8


class _Response(io.BytesIO):
    def __init__(self, body: bytes, content_type: str):
        super().__init__(body)
        self.status = 200
        self.headers = {"Content-Type": content_type}


def _anchor(heading: str) -> str:
    return re.sub(r"[^\w\- ]", "", heading.strip().lower()).replace(" ", "-")


def _dotted(expr) -> str:
    parts = []
    while isinstance(expr, ast.Attribute):
        parts.append(expr.attr)
        expr = expr.value
    if not isinstance(expr, ast.Name):
        return ""
    parts.append(expr.id)
    return ".".join(reversed(parts))


def _product_literals() -> list[tuple[str, int]]:
    """Every string constant equal to the product agent in the shipped modules, as (module, line)."""
    found = []
    for path in sorted(ROOT.glob("*.py")):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"), path.name)):
            if isinstance(node, ast.Constant) and node.value == PRODUCT:
                found.append((path.name, node.lineno))
    return found


class ParityP11bConformanceTests(unittest.TestCase):
    def test_conformance_names_the_clause_and_the_shared_cases(self):
        self.assertEqual(CONFORMANCE["parityP11bCases"], CASES)
        document, anchor = CONFORMANCE["parityP11bContract"].split("#", 1)
        self.assertEqual(document, "docs/delivery-lifecycle-contract-v1.md")
        headings = [line[3:] for line in CONTRACT.split("\n") if line.startswith("## ")]
        self.assertIn(anchor, {_anchor(heading) for heading in headings})
        self.assertIn("\nP11b-1 Product User-Agent.", CONTRACT)
        self.assertIn("\nP11b-2 Shared conformance.", CONTRACT)
        self.assertIn(f"`{PRODUCT}`", CONTRACT)


class ParityP11bTests(unittest.TestCase):
    def test_every_room_request_carries_the_connector_user_agent(self):
        self.assertEqual(protocol.ROOM_USER_AGENT, PRODUCT)
        # One shared constant: the product string exists once in the shipped modules, as the value of ROOM_USER_AGENT.
        literals = _product_literals()
        self.assertEqual(len(literals), 1, literals)
        assigned = [node for node in ast.walk(ast.parse((ROOT / "protocol.py").read_text(encoding="utf-8")))
                    if isinstance(node, ast.Assign) and [_dotted(target) for target in node.targets] == ["ROOM_USER_AGENT"]]
        self.assertEqual([(literal.lineno, literal.value.value) for literal in assigned], [(literals[0][1], PRODUCT)])
        self.assertEqual(literals[0][0], "protocol.py")
        api = protocol.RoomProtocol("https://room.example.test/api", "synthetic-credential")
        stream = b'event: ready\ndata: {"ready": true}\n\n'
        kinds = {
            "json read": (lambda: api.artifact("room-1", "artifact-1"), b"{}", "application/json"),
            "json write": (lambda: api.start_discussion_cycle("room-1", {"topic": "synthetic"}), b"{}", "application/json"),
            "multipart upload": (lambda: api.create_artifact("room-1", b"# note\n", "note.md", "text/markdown", "Note"),
                                 b"{}", "application/json"),
            "event stream": (lambda: api.stream_events("room-1", 0, lambda _event: True), stream, "text/event-stream"),
            "photo download": (lambda: api.download_artifact("room-1", "photo-1", "version-1", "image/png"), PNG, "image/png"),
        }
        for kind, (call, body, content_type) in kinds.items():
            with self.subTest(kind=kind):
                with mock.patch.object(protocol, "_urlopen", return_value=_Response(body, content_type)) as opened:
                    call()
                self.assertEqual(opened.call_count, 1)
                request = opened.call_args.args[0]
                # urllib keeps header names capitalized ("User-agent"); the opener adds its default only when absent.
                self.assertTrue(request.has_header("User-agent"))
                self.assertEqual(request.get_header("User-agent"), PRODUCT)

    def test_photo_download_carries_the_connector_user_agent_on_the_wire(self):
        seen = []

        class Download(http.server.BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802 - http.server API
                seen.append((self.path, self.headers.get_all("User-Agent")))
                self.send_response(200)
                self.send_header("Content-Type", "image/png")
                self.send_header("Content-Length", str(len(PNG)))
                self.end_headers()
                self.wfile.write(PNG)

            def log_message(self, *_args):
                pass

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Download)
        server.daemon_threads = True
        threading.Thread(target=server.serve_forever, daemon=True).start()
        base = f"http://127.0.0.1:{server.server_address[1]}/api"
        try:
            api = protocol.RoomProtocol(base, "synthetic-credential")
            self.assertEqual(api.download_artifact("room-1", "photo-1", "version-1", "image/png"), (PNG, "image/png"))
            # Negative control: the same opener sends the library default for a request formed without the header
            # (the 1.0.90 download), so the listener does see what an edge would refuse.
            with protocol._urlopen(urllib.request.Request(base + "/control", method="GET"), timeout=10) as response:
                response.read()
        finally:
            server.shutdown()
            server.server_close()
        self.assertEqual(seen[0], ("/api/rooms/room-1/artifacts/photo-1/versions/version-1/download", [PRODUCT]))
        self.assertEqual(seen[1][0], "/api/control")
        self.assertEqual(len(seen[1][1]), 1)
        self.assertTrue(seen[1][1][0].startswith("Python-urllib/"), seen[1][1])
        self.assertEqual(len(seen), 2)


if __name__ == "__main__":
    unittest.main()
