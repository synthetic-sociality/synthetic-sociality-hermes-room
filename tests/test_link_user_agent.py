"""Hermes 1.0.109: the device link sends its own User-Agent (contract P11b-3).

Live defect, 16 Nov 2026: the pairing confirmation went out with urllib's
default agent, the site's bot protection in front of the Harness answered 403
``error code: 1010``, and the connector reported ``link_pairing_unavailable``
until the token expired. A fake origin on loopback stands in for that edge: it
refuses a library default and accepts the connector's own agent. No real
service, token or key is used.
"""

from __future__ import annotations

import asyncio
import http.server
import json
import threading
import unittest
from unittest import mock

from test_parity_l1 import (
    DEVICE, FAKE_PAIRING_SECRET, SEED, Harness, LinkTestCase, _iso, adapter, cli, confirmed, link, make_token,
    run_cli, until,
)

CLOUDFLARE_1010 = b"error code: 1010"


class _EdgeHandler(http.server.BaseHTTPRequestHandler):
    """The pairing route behind a bot-protection edge."""

    def do_POST(self):  # noqa: N802 - http.server API
        server = self.server
        length = int(self.headers.get("Content-Length") or 0)
        self.rfile.read(length)
        agent = self.headers.get("User-Agent") or ""
        server.agents.append(agent)
        if server.mode == "edge" and (not agent or agent.startswith("Python-urllib")):
            self._send(403, CLOUDFLARE_1010, "text/plain; charset=UTF-8", cloudflare=True)
            return
        if server.mode == "always-blocked":
            self._send(403, CLOUDFLARE_1010, "text/plain; charset=UTF-8", cloudflare=True)
            return
        if server.mode == "cloudflare-html":
            self._send(403, b"<html><title>Attention Required!</title></html>", "text/html", cloudflare=True)
            return
        if server.mode == "harness-403":
            self._send(403, json.dumps({"error": "forbidden"}).encode(), "application/json; charset=utf-8")
            return
        status, body = server.answer
        self._send(status, json.dumps(body).encode(), "application/json; charset=utf-8")

    def _send(self, status: int, payload: bytes, content_type: str, *, cloudflare: bool = False) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        if cloudflare:
            self.send_header("Server", "cloudflare")
            self.send_header("CF-RAY", "8f00000000000000-FRA")
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *_args):
        pass


class edge_server:
    def __init__(self, mode: str, answer: tuple[int, dict] = (500, {})) -> None:
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _EdgeHandler)
        self.server.mode, self.server.answer, self.server.agents = mode, answer, []

    def __enter__(self):
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        return self.server

    def __exit__(self, *_exc) -> None:
        self.server.shutdown()
        self.server.server_close()


def audience_of(server) -> str:
    return f"http://127.0.0.1:{server.server_port}/tokenwerk/room/v1/local-devices/connect"


class LinkUserAgentTests(LinkTestCase):
    def pending(self, audience: str) -> dict:
        record = link.pending_record({"deviceId": DEVICE, "audience": audience, "pairingSecret": FAKE_PAIRING_SECRET,
                                      "expiresAt": _iso(300)}, label="Test · Hermes test", seed=SEED)
        link.write_link(record, link.pending_path(self.record_path))
        return record

    def test_the_link_agent_names_the_link_and_the_release(self):
        self.assertEqual(link.link_user_agent(), f"hermes-room-link/{adapter.CONNECTOR_VERSION}")
        self.assertEqual(link.link_user_agent("9.9.9"), "hermes-room-link/9.9.9")

    def test_the_confirmation_passes_an_edge_that_refuses_the_library_agent(self):
        with edge_server("edge") as server:
            record = self.pending(audience_of(server))
            server.answer = confirmed(record["fingerprintSha256"])
            self.assertEqual(link.confirm_pending(path=self.record_path), ("active", ""))
        self.assertEqual(server.agents, [f"hermes-room-link/{adapter.CONNECTOR_VERSION}"])
        self.assertEqual(link.read_link(self.record_path)["state"], "active")

    def test_a_bot_protection_refusal_has_its_own_reason(self):
        for mode in ("always-blocked", "cloudflare-html"):
            with self.subTest(mode), edge_server(mode) as server:
                self.pending(audience_of(server))
                self.assertEqual(link.confirm_pending(path=self.record_path),
                                 ("pending", "link_blocked_by_site_protection"))
                # Still pending (retried until the token expires), never refused.
                self.assertTrue(link.pending_path(self.record_path).exists())
        # The Harness's own 403 keeps its mapping.
        with edge_server("harness-403") as server:
            self.pending(audience_of(server))
            self.assertEqual(link.confirm_pending(path=self.record_path), ("pending", "link_pairing_unavailable"))

    def test_the_cli_names_the_site_bot_protection(self):
        with edge_server("always-blocked") as server, mock.patch.object(cli, "LINK_CONFIRMATION_WAIT_SECONDS", 0.5):
            code, out, err, _prompt = run_cli("link", token=make_token(audience_of(server)))
        self.assertEqual(code, 1)
        self.assertIn("link_blocked_by_site_protection", err)
        self.assertIn("bot protection", err)
        self.assertNotIn("link_pairing_unavailable", out + err)


class EdgeHarness(Harness):
    """The device endpoint behind an edge that refuses every upgrade (403 1010)."""

    def _process(self, connection, request):
        self.requests.append(request)
        response = connection.respond(403, "error code: 1010")
        response.headers["Server"] = "cloudflare"
        return response


class LinkSocketUserAgentTests(LinkTestCase):
    async def test_the_device_socket_sends_the_link_agent(self):
        async with Harness() as harness:
            self.active_record(harness.audience)
            await self.running(self.service())
            await harness.accept()
            await until(lambda: self.status_link()["state"] == "connected")
        self.assertEqual(harness.requests[0].headers["User-Agent"], f"hermes-room-link/{adapter.CONNECTOR_VERSION}")

    async def test_a_bot_protection_refusal_of_the_socket_has_its_own_reason(self):
        async with EdgeHarness() as harness:
            self.active_record(harness.audience)
            await self.running(self.service())
            await until(lambda: self.status_link()["code"] == "link_blocked_by_site_protection")
            self.assertIn(self.status_link()["state"], {"reconnecting", "connecting"})


class IdempotentConfirmationTests(LinkTestCase):
    """A repeated confirmation after a lost 201: the server already holds the
    device active with this key and answers 404 (the challenge is spent)."""

    async def test_a_device_the_server_already_holds_active_becomes_active_locally(self):
        async with Harness() as harness:
            with edge_server("open", (404, {"error": "local device unavailable"})) as server:
                record = link.pending_record({"deviceId": DEVICE, "audience": harness.audience,
                                              "pairingSecret": FAKE_PAIRING_SECRET, "expiresAt": _iso(300)},
                                             label="Test · Hermes test", seed=SEED)
                link.write_link(record, link.pending_path(self.record_path))
                original = link.send_confirmation

                def to_edge(pending, **kwargs):
                    with mock.patch.object(link, "derive_urls",
                                           lambda *_a: (None, f"http://127.0.0.1:{server.server_port}/c")):
                        return original(pending, **kwargs)

                with mock.patch.object(link, "send_confirmation", to_edge):
                    attempt = asyncio.create_task(asyncio.to_thread(link.confirm_pending, path=self.record_path))
                    await harness.accept()  # the server knows this key: authentication-complete
                    self.assertEqual(await attempt, ("active", ""))
        stored = link.read_link(self.record_path)
        self.assertEqual(stored["state"], "active")
        self.assertEqual(stored["privateKeyBase64"], record["privateKeyBase64"])
        self.assertNotIn("pairingSecret", stored)
        self.assertFalse(link.pending_path(self.record_path).exists())

    async def test_a_genuinely_refused_pairing_stays_refused(self):
        async with Harness() as harness:
            harness.refuse = (404, "no_route")  # the server does not know this device
            with edge_server("open", (404, {"error": "local device unavailable"})) as server:
                link.write_link(link.pending_record({"deviceId": DEVICE, "audience": harness.audience,
                                                     "pairingSecret": FAKE_PAIRING_SECRET, "expiresAt": _iso(300)},
                                                    label="Test · Hermes test", seed=SEED),
                                link.pending_path(self.record_path))
                original = link.send_confirmation

                def to_edge(pending, **kwargs):
                    with mock.patch.object(link, "derive_urls",
                                           lambda *_a: (None, f"http://127.0.0.1:{server.server_port}/c")):
                        return original(pending, **kwargs)

                with mock.patch.object(link, "send_confirmation", to_edge):
                    result = await asyncio.to_thread(link.confirm_pending, path=self.record_path)
        self.assertEqual(result, ("refused", "link_pairing_refused"))
        self.assertEqual(link.read_link(self.record_path)["state"], "refused")


if __name__ == "__main__":
    unittest.main()
