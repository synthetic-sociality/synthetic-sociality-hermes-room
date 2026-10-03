"""Shared conformance cases for the device link L1 (Hermes translation L3).

Delivery contract section "L1 — Device link"; case names are the keys of
``conformance.json`` ``parityL1Cases`` (OpenClaw lists the same names as
planned until P5). A fake Harness (WebSocket and HTTP on loopback) and a fake
local Gateway stand in for the server and the dashboard. No test uses a real
token, key or credential, and no test talks to a live service.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import http.server
import io
import json
import logging
import os
import stat
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from websockets.asyncio.server import serve

from test_delivery_lifecycle import ROOT, adapter

CONFORMANCE = json.loads((ROOT / "conformance.json").read_text())
link = adapter.device_link
cli = adapter.cli
local_status = adapter.local_status
secret_isolation = adapter.secret_isolation

# The shared vector's seed: a published test key, not a credential.
SEED = bytes(range(32))
DEVICE = "0f3c2b1a-9d8e-4f7a-8b6c-5d4e3f2a1b0c"
FAKE_PAIRING_SECRET = "PairingSecretForTestsOnly_0123456789abcdefg"
FAKE_DASHBOARD_TOKEN = "dashboard-session-token-for-tests-only-0123456789"
READY = '{"jsonrpc": "2.0", "method": "event", "params": {"type": "gateway.ready", "payload": {"replay_epoch": "é1"}}}'
CAPABILITIES = '{"jsonrpc":"2.0","id":1,"method":"client.capabilities","params":{"server_requests":true}}'
FAST_LIMITS = {"maxDataBytes": 2048, "openTimeoutMs": 3000, "maxChannels": 2, "maxFrameBytes": 8192, "keepaliveMs": 25_000}


def _iso(delta_seconds: float) -> str:
    moment = datetime.now(timezone.utc) + timedelta(seconds=delta_seconds)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def make_token(audience: str, *, drop: tuple[str, ...] = (), **overrides) -> str:
    payload = {"v": 1, "audience": audience, "deviceId": DEVICE, "pairingSecret": FAKE_PAIRING_SECRET,
               "expiresAt": _iso(300)}
    payload.update(overrides)
    for key in drop:
        payload.pop(key, None)
    encoded = base64.urlsafe_b64encode(json.dumps(payload, separators=(",", ":")).encode()).decode().rstrip("=")
    return link.TOKEN_PREFIX + encoded


def public_key(seed: bytes = SEED) -> Ed25519PublicKey:
    return Ed25519PublicKey.from_public_bytes(link.public_spki_der(seed)[len(link.SPKI_PREFIX):])


async def until(predicate, timeout: float = 8.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not reached in time")
        await asyncio.sleep(0.02)


async def fast_sleep(_seconds: float) -> None:
    await asyncio.sleep(0.01)


class Harness:
    """A fake Harness device endpoint on loopback."""

    def __init__(self) -> None:
        self.connections: asyncio.Queue = asyncio.Queue()
        self.requests: list = []
        self.refuse: tuple[int, str] | None = None
        self.frames: dict[int, list[int]] = {}

    async def __aenter__(self) -> "Harness":
        self.server = await serve(self._handler, "127.0.0.1", 0, process_request=self._process,
                                  max_size=None, compression=None, ping_interval=None)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *_exc) -> None:
        self.server.close()
        await self.server.wait_closed()

    @property
    def audience(self) -> str:
        return f"http://127.0.0.1:{self.port}/tokenwerk/room/v1/local-devices/connect"

    def _process(self, connection, request):
        self.requests.append(request)
        if self.refuse is not None:
            status, error = self.refuse
            response = connection.respond(status, json.dumps({"error": error}))
            response.headers["Content-Type"] = "application/json; charset=utf-8"
            return response
        return None

    async def _handler(self, ws) -> None:
        # Record the opcode of every frame the device sends (pings included).
        opcodes = self.frames.setdefault(id(ws), [])
        original = ws.protocol.recv_frame

        def recv_frame(frame):
            opcodes.append(int(frame.opcode))
            return original(frame)

        ws.protocol.recv_frame = recv_frame
        await self.connections.put(ws)
        await ws.wait_closed()

    async def accept(self, *, limits=None, audience=None, timeout: float = 8.0):
        ws = await asyncio.wait_for(self.connections.get(), timeout)
        proof = await self.handshake(ws, limits=limits, audience=audience)
        return ws, proof

    async def handshake(self, ws, *, limits=None, audience=None):
        challenge = {"version": 1, "handshakeId": "6f1d2c3b-4a59-4e68-9d7c-0b1a2c3d4e5f",
                     "connectionId": "1a2b3c4d-5e6f-4a0b-8c1d-2e3f4a5b6c7d", "deviceId": DEVICE,
                     "audience": audience or self.audience, "nonce": "N" * 43, "expiresAt": _iso(15),
                     "type": "authentication-challenge"}
        await ws.send(json.dumps(challenge))
        proof = json.loads(await asyncio.wait_for(ws.recv(), 8))
        signed = json.dumps({key: challenge[key] for key in (
            "version", "handshakeId", "connectionId", "deviceId", "audience", "nonce", "expiresAt")},
            separators=(",", ":"), ensure_ascii=False).encode()
        public_key().verify(base64.b64decode(proof["signatureBase64"]), signed)
        complete = {"version": 1, "type": "authentication-complete"}
        if limits is not None:
            complete["limits"] = limits
        await ws.send(json.dumps(complete))
        return proof


async def next_frame(ws, timeout: float = 8.0) -> dict:
    return json.loads(await asyncio.wait_for(ws.recv(), timeout))


async def no_frame(ws, wait: float = 0.4) -> None:
    with contextlib.suppress(asyncio.TimeoutError):
        frame = await asyncio.wait_for(ws.recv(), wait)
        raise AssertionError(f"unexpected frame {frame[:200]}")


class Gateway:
    """A fake local Hermes Gateway (/api/ws) that greets on open."""

    def __init__(self, *, hold_ready: bool = False, token: str = FAKE_DASHBOARD_TOKEN) -> None:
        self.token = token
        self.release = asyncio.Event()
        if not hold_ready:
            self.release.set()
        self.received: list[str] = []
        self.sockets: list = []
        self.closed = 0
        self.page = f'<html><script>window.__HERMES_SESSION_TOKEN__="{token}";</script></html>'

    async def __aenter__(self) -> "Gateway":
        self.server = await serve(self._handler, "127.0.0.1", 0, process_request=self._process,
                                  max_size=None, compression=None, ping_interval=None)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *_exc) -> None:
        self.release.set()  # a held greeting never blocks the shutdown
        self.server.close()
        await self.server.wait_closed()

    def _process(self, connection, request):
        if request.path == "/":
            response = connection.respond(200, self.page)
            response.headers["Content-Type"] = "text/html; charset=utf-8"
            return response
        if request.path != f"/api/ws?token={self.token}" or "Origin" in request.headers:
            return connection.respond(403, "")
        return None

    async def _handler(self, ws) -> None:
        self.sockets.append(ws)
        try:
            await self.release.wait()
            await ws.send(READY)
            async for message in ws:
                self.received.append(message)
                if message == CAPABILITIES:
                    await ws.send('{"jsonrpc":"2.0","id":1,"result":{"ok":"ü"}}')
        finally:
            self.closed += 1

    def opener(self):
        return lambda max_size: link.open_local_gateway(
            "", max_size, resolver=lambda _url: ("127.0.0.1", self.port, self.token))


class _HttpHandler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):  # noqa: N802 - http.server API
        server = self.server
        length = int(self.headers.get("Content-Length") or 0)
        server.requests.append({"path": self.path, "headers": dict(self.headers.items()), "body": self.rfile.read(length)})
        status, body = server.responses.pop(0) if server.responses else (500, {})
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *_args):
        pass


@contextlib.contextmanager
def confirmation_server(*responses):
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _HttpHandler)
    server.requests, server.responses = [], list(responses)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()


def confirmed(fingerprint: str) -> tuple[int, dict]:
    return 201, {"deviceId": DEVICE, "kind": "hermes", "deviceLabel": "x", "fingerprintSha256": fingerprint,
                 "state": "active", "confirmedAt": "2026-10-03T12:00:00.000Z"}


def run_cli(*argv, token: str | None = None):
    parser = __import__("argparse").ArgumentParser()
    cli.setup_cli(parser)
    out, err = io.StringIO(), io.StringIO()
    prompt = mock.Mock(return_value=token or "")
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err), \
            mock.patch.object(cli.getpass, "getpass", prompt), \
            mock.patch("builtins.input", side_effect=AssertionError("input() must not read a link token")), \
            mock.patch("time.sleep", lambda _s: None):
        code = cli.dispatch(parser.parse_args(list(argv)))
    return code, out.getvalue(), err.getvalue(), prompt


class LinkTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.scratch = Path(scratch.name)
        self.home = self.scratch / ".hermes"
        self.store = self.home / "synthetic-sociality-room"
        patches = [
            mock.patch.dict(os.environ, {"HERMES_HOME": str(self.home), "HOME": str(self.scratch)}),
            mock.patch.object(link, "_ALLOW_LOOPBACK_HTTP", True),
        ]
        try:
            import gateway  # Hermes core

            # No host rendezvous record unless a test provides one.
            patches.append(mock.patch.object(gateway, "host_rendezvous", None, create=True))
        except ImportError:
            pass
        for patcher in patches:
            patcher.start()
            self.addCleanup(patcher.stop)
        self.record_path = self.store / link.LINK_FILE
        self.status_path = self.store / "status.json"

    def active_record(self, audience: str, seed: bytes = SEED) -> dict:
        record = link.pending_record({"deviceId": DEVICE, "audience": audience, "pairingSecret": FAKE_PAIRING_SECRET,
                                      "expiresAt": _iso(300)}, label="Test · Hermes test", seed=seed)
        record = {key: value for key, value in record.items() if key not in {"pairingSecret", "expiresAt"}}
        record["state"] = "active"
        link.write_link(record, self.record_path)
        return record

    def service(self, gateway: Gateway | None = None, **kwargs) -> "link.LinkService":
        status = local_status.LocalStatus(self.status_path, plugin="hermes", version=adapter.CONNECTOR_VERSION,
                                          host_label="test-host")
        return link.LinkService(
            plugin_version=adapter.CONNECTOR_VERSION, status=status, record_path=self.record_path,
            local_opener=gateway.opener() if gateway is not None else None, sleep=kwargs.pop("sleep", fast_sleep),
            **kwargs,
        )

    async def running(self, service):
        task = asyncio.create_task(service.run())
        self.addAsyncCleanup(self._stop, task)
        return task

    @staticmethod
    async def _stop(task) -> None:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    def status_link(self) -> dict:
        try:
            return json.loads(self.status_path.read_text())["link"]
        except (OSError, ValueError, KeyError):
            return {"state": "", "code": "", "channels": -1, "limits": "", "lastChannelErrorCode": ""}


class L1ConformanceTests(unittest.TestCase):
    def test_case_names_resolve_to_tests(self):
        cases = CONFORMANCE["parityL1Cases"]
        self.assertEqual(len(cases), 14)
        sources = "\n".join(path.read_text() for path in (ROOT / "tests").glob("test_*.py"))
        for index, (name, method) in enumerate(cases.items(), start=1):
            self.assertTrue(name.startswith(f"L1-{index:02d} "), name)
            self.assertEqual(sources.count(f"def {method}(self"), 1, method)
        self.assertEqual(CONFORMANCE["parityL1Contract"], "docs/delivery-lifecycle-contract-v1.md#l1--device-link-2026-10-03")
        contract = (ROOT / "docs" / "delivery-lifecycle-contract-v1.md").read_text()
        section = contract[contract.index("\n## L1 — Device link (2026-10-03)\n"):]
        for file, digest in CONFORMANCE["parityL1Specs"].items():
            self.assertIn(f"| `{file}` | `{digest}` |", section)


class L1TokenAndSignatureTests(LinkTestCase):
    def test_l1_01_token_parsing(self):
        audience = "https://rooms.tokenwerk.eu/tokenwerk/room/v1/local-devices/connect"
        fields = link.parse_token("  " + make_token(audience, extra="ignored", kind="hermes") + "\n")
        self.assertEqual(fields["deviceId"], DEVICE)
        self.assertEqual(set(fields), {"audience", "deviceId", "pairingSecret", "expiresAt"})
        self.assertEqual(link.derive_urls(audience, DEVICE), (
            f"wss://rooms.tokenwerk.eu/tokenwerk/room/v1/local-devices/connect?deviceId={DEVICE}",
            "https://rooms.tokenwerk.eu/tokenwerk/room/v1/local-devices/pairing-confirmations",
        ))
        refused = {
            "version 2": (make_token(audience, v=2), "link_token_unsupported"),
            "version as string": (make_token(audience, v="1"), "link_token_unsupported"),
            "version as boolean": (make_token(audience, v=True), "link_token_unsupported"),
            "no version": (make_token(audience, drop=("v",)), "link_token_unsupported"),
            "too long": (make_token(audience) + "A" * link.MAX_TOKEN_LENGTH, "link_token_invalid"),
            "padding": (make_token(audience) + "=", "link_token_invalid"),
            "wrong prefix": ("twh2." + make_token(audience)[5:], "link_token_invalid"),
            "uppercase deviceId": (make_token(audience, deviceId=DEVICE.upper()), "link_token_invalid"),
            "short secret": (make_token(audience, pairingSecret="abc"), "link_token_invalid"),
            "expired beyond skew": (make_token(audience, expiresAt=_iso(-61)), "link_pairing_expired"),
            "audience with query": (make_token(audience + "?x=1"), "link_token_invalid"),
            "audience not connect": (make_token(audience.replace("/connect", "/other")), "link_token_invalid"),
        }
        for name in ("audience", "deviceId", "pairingSecret", "expiresAt"):
            refused[f"missing {name}"] = (make_token(audience, drop=(name,)), "link_token_invalid")
        for label, (token, code) in refused.items():
            with self.subTest(label):
                with self.assertRaises(link.LinkError) as caught:
                    link.parse_token(token)
                self.assertEqual(caught.exception.code, code)
                self.assertNotIn(FAKE_PAIRING_SECRET, str(caught.exception))
        # Within the 60 s skew the server decides.
        self.assertEqual(link.parse_token(make_token(audience, expiresAt=_iso(-50)))["deviceId"], DEVICE)
        # https only; plain http only to loopback in a test build.
        with mock.patch.object(link, "_ALLOW_LOOPBACK_HTTP", False), self.assertRaises(link.LinkError):
            link.parse_token(make_token("http://127.0.0.1:9/tokenwerk/room/v1/local-devices/connect"))
        with self.assertRaises(link.LinkError):
            link.parse_token(make_token("http://rooms.tokenwerk.eu/tokenwerk/room/v1/local-devices/connect"))
        # The token is read at a hidden prompt, never from the command line.
        token = make_token(audience)
        code, out, err, prompt = run_cli("link", token)
        self.assertEqual(code, 1)
        prompt.assert_not_called()
        self.assertIn("hidden prompt", err)
        self.assertNotIn(token, out + err)
        self.assertFalse(self.record_path.exists())
        code, out, err, prompt = run_cli("link", token="twh1.not-a-token")
        self.assertEqual(code, 1)
        prompt.assert_called_once()
        self.assertIn("link_token_invalid", err)

    def test_l1_02_pairing_confirmation(self):
        with confirmation_server() as server:
            audience = f"http://127.0.0.1:{server.server_port}/tokenwerk/room/v1/local-devices/connect"
            token = make_token(audience)
            seed_holder = {}
            real_seed = link.new_private_seed

            def remembered_seed():
                seed_holder["seed"] = real_seed()
                return seed_holder["seed"]

            with mock.patch.object(link, "new_private_seed", remembered_seed), \
                    mock.patch.object(link, "device_label", return_value="Mac128 · Hermes claude"):
                server.responses.append((502, {"error": "staging Room is unavailable"}))
                server.responses.append((403, {"error": "no_route"}))
                server.responses.append(None)  # replaced below once the key is known
                server.responses[-1] = confirmed("")

                def fingerprint_response():
                    server.responses[-1] = confirmed(link.fingerprint(seed_holder["seed"]))

                original = link.write_link

                def write_then_answer(record, path=None):
                    result = original(record, path)
                    if record.get("state") == "pending":
                        fingerprint_response()
                    return result

                with mock.patch.object(link, "write_link", write_then_answer):
                    code, out, err, prompt = run_cli("link", token=token)
            self.assertEqual(code, 0, err)
            prompt.assert_called_once()
            self.assertEqual(len(server.requests), 3)  # two retried failures, then 201
            request = server.requests[-1]
            self.assertEqual(request["path"], "/tokenwerk/room/v1/local-devices/pairing-confirmations")
            headers = {key.lower(): value for key, value in request["headers"].items()}
            self.assertEqual(headers["content-type"], "application/json")
            self.assertNotIn("cookie", headers)
            self.assertNotIn("authorization", headers)
            body = request["body"]
            self.assertLessEqual(len(body), link.CONFIRMATION_BODY_LIMIT)
            self.assertEqual(body.count(b'"deviceId"'), 1)
            sent = json.loads(body)
            self.assertEqual(list(sent), ["version", "deviceId", "pairingSecret", "deviceLabel", "publicKeySpkiBase64", "signatureBase64"])
            self.assertEqual(sent["deviceLabel"], "Mac128 · Hermes claude")
            self.assertEqual(base64.b64decode(sent["publicKeySpkiBase64"])[:12], link.SPKI_PREFIX)
            public_key(seed_holder["seed"]).verify(base64.b64decode(sent["signatureBase64"]), link.pairing_proof_bytes(
                DEVICE, FAKE_PAIRING_SECRET, "Mac128 · Hermes claude", sent["publicKeySpkiBase64"]))
            record = link.read_link(self.record_path)
            self.assertEqual(record["state"], "active")
            self.assertNotIn("pairingSecret", record)
            self.assertIn(record["fingerprintSha256"], out)
            self.assertNotIn(FAKE_PAIRING_SECRET, out + err)
            self.assertNotIn(record["privateKeyBase64"], out + err)
        # 404: the token is spent; no retry, the pending secret leaves the store.
        with confirmation_server((404, {"error": "local device unavailable"})) as server:
            audience = f"http://127.0.0.1:{server.server_port}/tokenwerk/room/v1/local-devices/connect"
            code, out, err, _prompt = run_cli("link", token=make_token(audience))
            self.assertEqual(code, 1)
            self.assertEqual(len(server.requests), 1)
            self.assertIn("link_pairing_refused", err)
            record = link.read_link(self.record_path)
            self.assertEqual(record["state"], "refused")
            self.assertNotIn("pairingSecret", record)

    def test_l1_03_signature_vector(self):
        vector = CONFORMANCE["parityL1SignatureVector"]
        seed = bytes.fromhex(vector["seedHex"])
        self.assertEqual(seed, SEED)
        self.assertEqual(link.public_spki_base64(seed), vector["publicKeySpkiBase64"])
        self.assertEqual(link.fingerprint(seed), vector["fingerprintSha256"])
        pairing = vector["pairing"]
        pairing_bytes = link.pairing_proof_bytes(pairing["deviceId"], pairing["pairingSecret"], pairing["deviceLabel"],
                                                 pairing["publicKeySpkiBase64"])
        handshake_bytes = link.handshake_proof_bytes(dict(vector["handshake"], type="authentication-challenge"))
        self.assertEqual(hashlib.sha256(pairing_bytes).hexdigest(), vector["pairingInputSha256"])
        self.assertEqual(hashlib.sha256(handshake_bytes).hexdigest(), vector["handshakeInputSha256"])
        self.assertEqual(link.sign(seed, pairing_bytes), vector["pairingSignatureBase64"])
        self.assertEqual(link.sign(seed, handshake_bytes), vector["handshakeSignatureBase64"])
        # JSON.stringify: no spaces, literal UTF-8, no escaping of / < > &.
        self.assertIn("Mac128 · Hermes claude </&>".encode(), pairing_bytes)
        self.assertTrue(pairing_bytes.startswith(b'{"version":1,"deviceId":'))
        self.assertTrue(handshake_bytes.startswith(b'{"version":1,"handshakeId":'))
        self.assertNotIn(b'"type"', handshake_bytes)


class L1SocketTests(LinkTestCase):
    async def test_l1_04_signed_handshake(self):
        async with Harness() as harness:
            self.active_record(harness.audience)
            await self.running(self.service())
            ws, proof = await harness.accept()
            self.assertEqual(list(proof), ["version", "handshakeId", "connectionId", "deviceId", "audience", "nonce",
                                           "expiresAt", "signatureBase64", "pluginVersion"])
            self.assertEqual(proof["pluginVersion"], adapter.CONNECTOR_VERSION)
            request = harness.requests[0]
            self.assertEqual(request.path, f"/tokenwerk/room/v1/local-devices/connect?deviceId={DEVICE}")
            self.assertNotIn("Cookie", request.headers)
            self.assertNotIn("Authorization", request.headers)
            await until(lambda: self.status_link()["state"] == "connected")
            await ws.close()
            # A foreign audience is never answered.
            ws = await asyncio.wait_for(harness.connections.get(), 8)
            challenge = {"version": 1, "handshakeId": "6f1d2c3b-4a59-4e68-9d7c-0b1a2c3d4e5f",
                         "connectionId": "1a2b3c4d-5e6f-4a0b-8c1d-2e3f4a5b6c7d", "deviceId": DEVICE,
                         "audience": "https://elsewhere.example/tokenwerk/room/v1/local-devices/connect",
                         "nonce": "N" * 43, "expiresAt": _iso(15), "type": "authentication-challenge"}
            await ws.send(json.dumps(challenge))
            await ws.wait_closed()
            self.assertEqual(harness.frames[id(ws)].count(1), 0)  # no text frame (proof) was sent
            await until(lambda: self.status_link()["code"] == "link_audience_mismatch")

    async def test_l1_05_limits(self):
        self.assertEqual(link.read_limits({"version": 1, "type": "authentication-complete"}),
                         (link.FALLBACK_LIMITS, "fallback"))
        self.assertEqual(link.FALLBACK_LIMITS, {"maxFrameBytes": 6291456, "maxDataBytes": 3145728, "openTimeoutMs": 15000,
                                                "maxChannels": 4, "keepaliveMs": 25000})
        limits, source = link.read_limits({"limits": {"maxChannels": 99, "keepaliveMs": "x", "maxDataBytes": 4096,
                                                      "openTimeoutMs": True, "unknown": 1}})
        self.assertEqual(source, "advertised")
        self.assertEqual(limits, dict(link.FALLBACK_LIMITS, maxDataBytes=4096))
        self.assertGreaterEqual(link.CLIENT_MAX_MESSAGE_BYTES, 6291456 + 1024)
        async with Harness() as harness, Gateway() as gateway:
            self.active_record(harness.audience)
            await self.running(self.service(gateway))
            # Without limits: the fallbacks, and a server message above 6 MiB is accepted.
            ws, _ = await harness.accept()
            await until(lambda: self.status_link()["state"] == "connected")
            self.assertEqual(self.status_link()["limits"], "fallback")
            channel = "11111111-1111-4111-8111-111111111111"
            await ws.send(json.dumps({"version": 1, "type": "hermes-open", "channelId": channel}))
            self.assertEqual((await next_frame(ws))["type"], "hermes-opened")
            self.assertEqual((await next_frame(ws))["data"], READY)
            big = "x" * (link.FALLBACK_LIMITS["maxDataBytes"] + 1)
            padding = "p" * (6291456 + 512 - len(big))
            await ws.send(json.dumps({"version": 1, "type": "hermes-frame", "channelId": channel, "data": big, "pad": padding}))
            self.assertEqual(await next_frame(ws), {"version": 1, "type": "hermes-close", "channelId": channel})
            self.assertEqual(self.status_link()["lastChannelErrorCode"], "channel_data_too_large")
            await ws.close()
            # With limits: advertised values govern data size and the channel cap.
            ws, _ = await harness.accept(limits=dict(FAST_LIMITS, maxChannels=1))
            await until(lambda: self.status_link()["limits"] == "advertised")
            first, second = "22222222-2222-4222-8222-222222222222", "33333333-3333-4333-8333-333333333333"
            await ws.send(json.dumps({"version": 1, "type": "hermes-open", "channelId": first}))
            self.assertEqual((await next_frame(ws))["type"], "hermes-opened")
            self.assertEqual((await next_frame(ws))["data"], READY)
            await ws.send(json.dumps({"version": 1, "type": "hermes-open", "channelId": second}))
            self.assertEqual(await next_frame(ws), {"version": 1, "type": "hermes-open-failed", "channelId": second})
            await ws.send(json.dumps({"version": 1, "type": "hermes-frame", "channelId": first, "data": "y" * 3000}))
            self.assertEqual(await next_frame(ws), {"version": 1, "type": "hermes-close", "channelId": first})

    async def test_l1_06_channel_bridge(self):
        async with Harness() as harness, Gateway(hold_ready=True) as gateway:
            self.active_record(harness.audience)
            await self.running(self.service(gateway))
            ws, _ = await harness.accept(limits=FAST_LIMITS)
            channel = "44444444-4444-4444-8444-444444444444"
            await ws.send(json.dumps({"version": 1, "type": "hermes-open", "channelId": channel}))
            # hermes-opened as soon as the Gateway is open, before gateway.ready.
            opened = await asyncio.wait_for(ws.recv(), 3)
            self.assertEqual(opened, '{"version":1,"type":"hermes-opened","channelId":"' + channel + '"}')
            self.assertEqual(len(gateway.sockets), 1)
            self.assertEqual(gateway.received, [])
            gateway.release.set()
            ready = await next_frame(ws)
            self.assertEqual(list(ready), ["version", "type", "channelId", "data"])
            self.assertEqual(ready["data"], READY)  # verbatim, spacing and non-ASCII kept
            await ws.send(json.dumps({"version": 1, "type": "hermes-frame", "channelId": channel, "data": CAPABILITIES}))
            reply = await next_frame(ws)
            self.assertEqual(reply["data"], '{"jsonrpc":"2.0","id":1,"result":{"ok":"ü"}}')
            self.assertEqual(gateway.received, [CAPABILITIES])
            self.assertEqual(self.status_link()["channels"], 1)
            # The server ends the channel: the Gateway connection closes, no echo.
            await ws.send(json.dumps({"version": 1, "type": "hermes-close", "channelId": channel}))
            await until(lambda: gateway.closed == 1)
            await no_frame(ws)
            await until(lambda: self.status_link()["channels"] == 0)
            # The Gateway ends a channel: hermes-close to the server.
            other = "55555555-5555-4555-8555-555555555555"
            await ws.send(json.dumps({"version": 1, "type": "hermes-open", "channelId": other}))
            self.assertEqual((await next_frame(ws))["type"], "hermes-opened")
            self.assertEqual((await next_frame(ws))["data"], READY)
            await gateway.sockets[-1].close()
            self.assertEqual(await next_frame(ws), {"version": 1, "type": "hermes-close", "channelId": other})
            # The device socket closes: every Gateway connection of it closes.
            third = "66666666-6666-4666-8666-666666666666"
            await ws.send(json.dumps({"version": 1, "type": "hermes-open", "channelId": third}))
            self.assertEqual((await next_frame(ws))["type"], "hermes-opened")
            await ws.close()
            await until(lambda: gateway.closed == 3)

    async def test_l1_07_oversize_and_cap(self):
        async with Harness() as harness, Gateway() as gateway:
            self.active_record(harness.audience)
            await self.running(self.service(gateway))
            ws, _ = await harness.accept(limits=FAST_LIMITS)
            first, second, third = ("77777777-7777-4777-8777-777777777777", "88888888-8888-4888-8888-888888888888",
                                    "99999999-9999-4999-8999-999999999999")
            for channel in (first, second):
                await ws.send(json.dumps({"version": 1, "type": "hermes-open", "channelId": channel}))
                self.assertEqual((await next_frame(ws))["type"], "hermes-opened")
                self.assertEqual((await next_frame(ws))["data"], READY)
            # Above the cap: refused, the others unaffected.
            await ws.send(json.dumps({"version": 1, "type": "hermes-open", "channelId": third}))
            self.assertEqual(await next_frame(ws), {"version": 1, "type": "hermes-open-failed", "channelId": third})
            self.assertEqual(self.status_link()["lastChannelErrorCode"], "channel_cap")
            # Oversize data from the server ends only that channel.
            await ws.send(json.dumps({"version": 1, "type": "hermes-frame", "channelId": first, "data": "z" * 2049}))
            self.assertEqual(await next_frame(ws), {"version": 1, "type": "hermes-close", "channelId": first})
            # Oversize data from the Gateway ends only that channel.
            await ws.send(json.dumps({"version": 1, "type": "hermes-frame", "channelId": second, "data": CAPABILITIES}))
            self.assertEqual((await next_frame(ws))["channelId"], second)
            big_reply = json.dumps({"jsonrpc": "2.0", "id": 2, "result": "w" * 4000})
            await gateway.sockets[-1].send(big_reply)
            self.assertEqual(await next_frame(ws), {"version": 1, "type": "hermes-close", "channelId": second})
            self.assertEqual(self.status_link()["lastChannelErrorCode"], "channel_data_too_large")
            # The socket itself is still serving.
            await ws.send(json.dumps({"version": 1, "type": "hermes-open", "channelId": third}))
            self.assertEqual((await next_frame(ws))["type"], "hermes-opened")
            self.assertEqual(self.status_link()["state"], "connected")

    async def test_l1_08_unknown_frames(self):
        async with Harness() as harness, Gateway() as gateway:
            self.active_record(harness.audience)
            await self.running(self.service(gateway))
            ws, _ = await harness.accept(limits=FAST_LIMITS)
            unknown = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
            await ws.send(json.dumps({"version": 1, "type": "hermes-frame", "channelId": unknown, "data": "{}"}))
            self.assertEqual(await next_frame(ws), {"version": 1, "type": "hermes-close", "channelId": unknown})
            self.assertEqual(self.status_link()["lastChannelErrorCode"], "channel_unknown")
            channel = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
            await ws.send(json.dumps({"version": 1, "type": "hermes-open", "channelId": channel}))
            self.assertEqual((await next_frame(ws))["type"], "hermes-opened")
            self.assertEqual((await next_frame(ws))["data"], READY)
            await ws.send(json.dumps({"version": 1, "type": "hermes-open", "channelId": channel}))
            self.assertEqual(await next_frame(ws), {"version": 1, "type": "hermes-close", "channelId": channel})
            self.assertEqual(self.status_link()["lastChannelErrorCode"], "channel_duplicate")
            await until(lambda: gateway.closed == 1)
            # An unknown type closes the socket as a protocol error.
            await ws.send(json.dumps({"version": 1, "type": "hermes-ping", "channelId": channel}))
            await ws.wait_closed()
            self.assertEqual((ws.close_code, ws.close_reason), (1008, "protocol-error"))
            await until(lambda: self.status_link()["code"] == "link_protocol_error")
            # So does a version other than 1, after the reconnect.
            ws, _ = await harness.accept(limits=FAST_LIMITS)
            await ws.send(json.dumps({"version": 2, "type": "hermes-open", "channelId": channel}))
            await ws.wait_closed()
            self.assertEqual((ws.close_code, ws.close_reason), (1008, "protocol-error"))

    async def test_l1_09_stop_states(self):
        for reason, code in (("revoked", "link_revoked"), ("replaced", "link_replaced"),
                             ("authentication-failed", "link_revoked")):
            with self.subTest(reason):
                self.status_path.unlink(missing_ok=True)
                async with Harness() as harness:
                    record = self.active_record(harness.audience)
                    task = await self.running(self.service())
                    if reason == "authentication-failed":
                        ws = await asyncio.wait_for(harness.connections.get(), 8)
                    else:
                        ws, _ = await harness.accept()
                    await ws.close(1008, reason)
                    await until(lambda: self.status_link()["state"] == "stopped")
                    self.assertEqual(self.status_link()["code"], code)
                    await asyncio.sleep(0.5)
                    self.assertTrue(harness.connections.empty(), "a stopped link must not reconnect")
                    self.assertEqual(len(harness.requests), 1)
                    # The key is kept.
                    self.assertEqual(link.read_link(self.record_path)["privateKeyBase64"], record["privateKeyBase64"])
                    await self._stop(task)
        # Pending: confirmation retried until 201, then the link connects.
        async with Harness() as harness:
            self.status_path.unlink(missing_ok=True)
            with confirmation_server() as server:
                audience = harness.audience
                pending = link.pending_record({"deviceId": DEVICE, "audience": audience, "pairingSecret": FAKE_PAIRING_SECRET,
                                               "expiresAt": _iso(300)}, label="Test · Hermes test", seed=SEED)
                link.write_link(pending, self.record_path)
                server.responses += [(502, {}), (502, {}), confirmed(pending["fingerprintSha256"])]
                original = link.send_confirmation

                def to_test_server(record, **kwargs):
                    with mock.patch.object(link, "derive_urls", lambda *_a: (None, f"http://127.0.0.1:{server.server_port}/c")):
                        return original(record, **kwargs)

                with mock.patch.object(link, "send_confirmation", to_test_server):
                    task = await self.running(self.service())
                    ws, _ = await harness.accept()
                self.assertEqual(len(server.requests), 3)
                await until(lambda: self.status_link()["state"] == "connected")
                self.assertNotIn("pairingSecret", link.read_link(self.record_path))
                await self._stop(task)
        # Pending past the token's expiry: stopped, the secret removed.
        expired = link.pending_record({"deviceId": DEVICE, "audience": audience, "pairingSecret": FAKE_PAIRING_SECRET,
                                       "expiresAt": _iso(-1)}, label="Test · Hermes test", seed=SEED)
        link.write_link(expired, self.record_path)
        self.status_path.unlink(missing_ok=True)
        task = await self.running(self.service())
        await until(lambda: self.status_link()["code"] == "link_pairing_expired")
        self.assertEqual(self.status_link()["state"], "stopped")
        self.assertNotIn("pairingSecret", link.read_link(self.record_path))
        await self._stop(task)

    async def test_l1_10_reconnect_and_keepalive(self):
        for attempt in range(12):
            low = link.backoff_delay(attempt, rng=lambda a, b: a)
            high = link.backoff_delay(attempt, rng=lambda a, b: b)
            self.assertLessEqual(high, link.BACKOFF_MAX_SECONDS)
            self.assertAlmostEqual(low * 2, high)
        self.assertEqual(link.backoff_delay(0, rng=lambda a, b: b), 1.0)
        self.assertEqual(link.backoff_delay(20, rng=lambda a, b: b), 30.0)
        calls = []
        async with Harness() as harness:
            self.active_record(harness.audience)
            service = self.service(rng=lambda a, b: calls.append((a, b)) or b)
            await self.running(service)
            for _ in range(3):
                ws = await asyncio.wait_for(harness.connections.get(), 8)
                ws.transport.abort()  # no close frame: 1006
            await harness.accept()
            self.assertEqual(calls[:3], [(0.5, 1.0), (1.0, 2.0), (2.0, 4.0)])  # bounded, jittered, growing
            await until(lambda: self.status_link()["state"] == "connected")
            self.assertEqual(service.delays[:3], [1.0, 2.0, 4.0])
        # The device's own ping, and an idle close after twice the keepalive.
        async with Harness() as harness:
            self.active_record(harness.audience)
            await self.running(self.service())
            ws, _ = await harness.accept(limits=dict(FAST_LIMITS, keepaliveMs=1000))
            await until(lambda: 0x9 in harness.frames[id(ws)], timeout=4)  # a ping from the device
            ws.transport.pause_reading()  # no pong from now on
            await until(lambda: self.status_link()["code"] == "link_keepalive_timeout", timeout=8)

    async def test_l1_13_status_codes(self):
        table = {
            (1008, "revoked"): ("link_revoked", "stop"),
            (1008, "authentication-failed"): ("link_revoked", "stop"),
            (1008, "replaced"): ("link_replaced", "stop"),
            (1008, "protocol-error"): ("link_protocol_error", "reconnect"),
            (1008, "anything"): ("link_protocol_error", "reconnect"),
            (1009, ""): ("link_frame_too_large", "reconnect"),
            (1006, ""): ("link_connection_lost", "reconnect"),
            (1000, ""): ("link_connection_lost", "reconnect"),
            (None, ""): ("link_connection_lost", "reconnect"),
        }
        for (close, reason), expected in table.items():
            self.assertEqual(link.close_outcome(close, reason), expected, (close, reason))
        # Upgrade refused by the dispatcher: route refused, reconnect.
        async with Harness() as harness:
            harness.refuse = (403, "no_route")
            self.active_record(harness.audience)
            await self.running(self.service())
            await until(lambda: self.status_link()["code"] == "link_route_refused")
            # Not a stop: the next attempt follows (connecting again by now).
            self.assertIn(self.status_link()["state"], {"reconnecting", "connecting"})
            await until(lambda: len(harness.requests) >= 2)
        # Only codes, an identifier, numbers and times; unknown values are normalised.
        status = local_status.LocalStatus(self.status_path, plugin="hermes", version="x", host_label="h")
        status.set_link({"state": "bogus", "code": "has a space", "deviceId": DEVICE, "channels": -3,
                         "limits": "other", "lastConnectedAt": 5, "lastChannelErrorCode": "x" * 200})
        written = self.status_link()
        self.assertEqual(set(written), {"state", "code", "deviceId", "channels", "limits", "lastConnectedAt",
                                        "lastChannelErrorCode", "updatedAt"})
        self.assertEqual((written["state"], written["code"], written["channels"], written["limits"],
                          written["lastConnectedAt"], written["lastChannelErrorCode"]),
                         ("stopped", "unknown_error", 0, "fallback", None, "unknown_error"))
        self.assertEqual(stat.S_IMODE(self.status_path.stat().st_mode), 0o600)


class L1LocalTests(LinkTestCase):
    async def test_l1_11_unlink(self):
        async with Harness() as harness:
            record = self.active_record(harness.audience)
            await self.running(self.service())
            ws, _ = await harness.accept()
            await until(lambda: self.status_link()["state"] == "connected")
            code, out, err, _prompt = run_cli("unlink")
            self.assertEqual(code, 0, err)
            self.assertFalse(self.record_path.exists())
            self.assertIn(DEVICE, out)
            self.assertIn("Revoke this device in the Harness", out)
            self.assertNotIn(record["privateKeyBase64"], out + err)
            # The running service drops the socket and reports not linked.
            await ws.wait_closed()
            await until(lambda: self.status_link()["state"] == "not_linked")
            self.assertTrue(harness.connections.empty())
            code, out, _err, _prompt = run_cli("unlink")
            self.assertEqual((code, out.strip()), (0, "Not linked; nothing to delete."))
        # Linking again replaces the local key and asks to revoke the old device.
        with confirmation_server() as server:
            audience = f"http://127.0.0.1:{server.server_port}/tokenwerk/room/v1/local-devices/connect"
            old = self.active_record(audience)
            old["deviceId"] = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
            link.write_link(old, self.record_path)
            original = link.write_link

            def answer(record, path=None):
                result = original(record, path)
                if record.get("state") == "pending":
                    server.responses.append(confirmed(record["fingerprintSha256"]))
                return result

            with mock.patch.object(link, "write_link", answer):
                code, out, err, _prompt = run_cli("link", token=make_token(audience))
            self.assertEqual(code, 0, err)
            self.assertIn("Revoke that device in the Harness", out)
            self.assertIn("cccccccc-cccc-4ccc-8ccc-cccccccccccc", out)
            self.assertNotEqual(link.read_link(self.record_path)["privateKeyBase64"], old["privateKeyBase64"])
            code, out, _err, _prompt = run_cli("link", "status")
            self.assertEqual(code, 0)
            self.assertIn("Link:        active", out)
            self.assertIn(DEVICE, out)
            self.assertNotIn(link.read_link(self.record_path)["privateKeyBase64"], out)

    async def test_l1_12_secret_custody(self):
        with confirmation_server() as server:
            audience = f"http://127.0.0.1:{server.server_port}/tokenwerk/room/v1/local-devices/connect"
            server.responses.append((502, {}))
            with mock.patch.object(cli, "LINK_CONFIRMATION_WAIT_SECONDS", 0):
                code, _out, _err, _prompt = run_cli("link", token=make_token(audience))
            self.assertEqual(code, 1)  # still pending: the secret is in the store
        record = link.read_link(self.record_path)
        self.assertEqual(record["state"], "pending")
        secrets_in_play = [record["privateKeyBase64"], FAKE_PAIRING_SECRET]
        # The record is inside the fenced store, 0600 in a 0700 directory.
        self.assertEqual(link.link_path().parent, self.store)
        self.assertEqual(self.record_path.parent, self.store)
        self.assertEqual(stat.S_IMODE(self.record_path.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(self.store.stat().st_mode), 0o700)
        for path in self.scratch.rglob("*"):
            if path.is_file() and path.parent != self.store:
                text = path.read_text(errors="replace")
                for value in secrets_in_play:
                    self.assertNotIn(value, text, str(path))
        # The agent's tools cannot reach it.
        with mock.patch.dict(os.environ, {"TERMINAL_CWD": str(self.scratch)}), self.assertLogs(adapter.logger, level="WARNING"):
            for tool, args in {"read_file": {"path": str(self.record_path)},
                               "search_files": {"path": str(self.home), "pattern": "privateKey"},
                               "terminal": {"command": f"cat {self.record_path}"},
                               "terminal ($HERMES_HOME)": {"command": "cat $HERMES_HOME/synthetic-sociality-room/link.json"}}.items():
                verdict = adapter._on_pre_tool_call(tool_name=tool.split(" ")[0], args=args)
                self.assertEqual(verdict["action"], "block", tool)
        # The local Gateway credential is remembered when it is resolved, and
        # never reaches a log line, even with the root logger at DEBUG.
        records: list[logging.LogRecord] = []
        capture = logging.Handler(logging.DEBUG)
        capture.emit = records.append
        root = logging.getLogger()
        previous_level = root.level
        root.addHandler(capture)
        root.setLevel(logging.DEBUG)
        try:
            async with Gateway() as gateway:
                await asyncio.to_thread(link.resolve_local_gateway, f"http://127.0.0.1:{gateway.port}")
                local = await link.open_local_gateway(f"http://127.0.0.1:{gateway.port}", 3145728)
                await local.close()
        finally:
            root.removeHandler(capture)
            root.setLevel(previous_level)
        # The fake dashboard's own server log ("< GET /api/ws?token=…") stands
        # for the dashboard's access log (report §1.3); this process's client
        # side must log nothing with the token.
        client_records = [item for item in records if not item.name.startswith("websockets.server")]
        self.assertTrue(client_records)
        for log_record in client_records:
            self.assertNotIn(FAKE_DASHBOARD_TOKEN, log_record.getMessage())
        secrets_in_play.append(FAKE_DASHBOARD_TOKEN)
        # Removed from tool results and log lines; absent from environment and status.
        leaked = "result: " + " ".join(secrets_in_play)
        scrubbed = adapter._on_transform_tool_result(result=leaked)
        log_record = logging.LogRecord("x", logging.WARNING, __file__, 1, leaked, (), None)
        adapter._SecretLogFilter().filter(log_record)
        for value in secrets_in_play:
            self.assertNotIn(value, scrubbed)
            self.assertNotIn(value, log_record.getMessage())
            self.assertNotIn(value, "\n".join(os.environ.values()))
        status = local_status.LocalStatus(self.status_path, plugin="hermes", version="x", host_label="h")
        service = self.service()
        service.status = status
        service._set(state="pending", code="link_pending", deviceId=DEVICE)
        for value in secrets_in_play:
            self.assertNotIn(value, self.status_path.read_text())

    async def test_l1_14_local_gateway_auth(self):
        # Loopback dashboard: the session token from its page, Host 127.0.0.1, no Origin.
        async with Gateway() as gateway:
            host, port, token = await asyncio.to_thread(link.resolve_local_gateway, f"http://127.0.0.1:{gateway.port}")
            self.assertEqual((host, port, token), ("127.0.0.1", gateway.port, FAKE_DASHBOARD_TOKEN))
            local = await link.open_local_gateway(f"http://127.0.0.1:{gateway.port}", 3145728)
            self.assertEqual(await local.recv(), READY)
            await local.close()
            # A wrong credential is refused by the dashboard.
            with self.assertRaises(link.LocalGatewayError) as caught:
                await link.open_local_gateway("", 1024, resolver=lambda _u: ("127.0.0.1", gateway.port, "wrong-token"))
            self.assertEqual(caught.exception.code, "local_gateway_refused")
            # A gated dashboard withholds the token: no non-interactive credential.
            gateway.page = "<html><script>window.__HERMES_AUTH_REQUIRED__=true;</script></html>"
            with self.assertRaises(link.LocalGatewayError) as caught:
                await asyncio.to_thread(link.resolve_local_gateway, f"http://127.0.0.1:{gateway.port}")
            self.assertEqual(caught.exception.code, "local_gateway_auth_unavailable")
        # Not running.
        with self.assertRaises(link.LocalGatewayError) as caught:
            link.resolve_local_gateway(f"http://127.0.0.1:{port}")
        self.assertEqual(caught.exception.code, "local_gateway_unavailable")
        # Not loopback.
        with self.assertRaises(link.LocalGatewayError) as caught:
            link.resolve_local_gateway("http://192.0.2.1:9119")
        self.assertEqual(caught.exception.code, "local_gateway_auth_unavailable")
        # The core's host rendezvous (newer cores) is read first: no page fetch.
        fake = mock.Mock(ROLE_SERVE="serve", ROLE_DESKTOP_SERVE="desktop-serve")
        fake.read_record.side_effect = lambda role: mock.Mock(host="127.0.0.1", port=9, pid=1) if role == "serve" else None
        fake.read_token.side_effect = lambda role: FAKE_DASHBOARD_TOKEN if role == "serve" else ""
        fake.record_token_is_consistent.return_value = True
        import gateway as core_gateway

        with mock.patch.object(core_gateway, "host_rendezvous", fake, create=True), \
                mock.patch.object(link, "_no_proxy_opener", side_effect=AssertionError("no page fetch")):
            self.assertEqual(link.resolve_local_gateway(""), ("127.0.0.1", 9, FAKE_DASHBOARD_TOKEN))
        # In a channel: a gated dashboard fails the open with its code.
        async with Harness() as harness:
            self.active_record(harness.audience)
            service = self.service()
            service.local_opener = lambda max_size: link.open_local_gateway(
                "", max_size, resolver=mock.Mock(side_effect=link.LocalGatewayError("local_gateway_auth_unavailable")))
            await self.running(service)
            ws, _ = await harness.accept(limits=FAST_LIMITS)
            channel = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
            await ws.send(json.dumps({"version": 1, "type": "hermes-open", "channelId": channel}))
            self.assertEqual(await next_frame(ws), {"version": 1, "type": "hermes-open-failed", "channelId": channel})
            self.assertEqual(self.status_link()["lastChannelErrorCode"], "local_gateway_auth_unavailable")


if __name__ == "__main__":
    unittest.main()
