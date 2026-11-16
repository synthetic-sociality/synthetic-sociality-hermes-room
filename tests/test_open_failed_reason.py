"""Hermes 1.0.110: hermes-open-failed carries a coded reason, behind a capability flag.

Contract paragraph "Open-failure reason (additive, frame version 1)": the
device sends ``reason`` (one code of a closed list) only when the Harness's
``authentication-complete`` has ``limits.openFailedReason`` 1. A Harness before
canary 19 drops the device socket on an unknown field, so without the flag the
frame keeps exactly its three keys. A fake Harness and fake local gateways on
loopback stand in for the real ones; no real service, token or key is used.
"""

from __future__ import annotations

import asyncio
import json
import re
import socket
import unittest

from test_parity_l1 import FAST_LIMITS, ROOT, Gateway, Harness, LinkTestCase, link, next_frame, until

CONTRACT = (ROOT / "docs" / "delivery-lifecycle-contract-v1.md").read_text(encoding="utf-8")
CHANNEL = "abcdefab-cdef-4abc-8def-abcdefabcdef"
FLAGGED = dict(FAST_LIMITS, openFailedReason=1)
SECRET_TEXT = "upstream says: token=dashboard-secret-0123 at 10.0.0.7"


def legacy(channel: str = CHANNEL) -> dict:
    return {"version": 1, "type": "hermes-open-failed", "channelId": channel}


def coded(reason: str, channel: str = CHANNEL) -> dict:
    return dict(legacy(channel), reason=reason)


def failing_opener(error: BaseException):
    async def opener(_max_size: int):
        raise error
    return opener


async def never_opens(_max_size: int):
    await asyncio.Event().wait()


def closed_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class OpenFailedReasonContractTests(unittest.TestCase):
    def test_the_contract_paragraph_and_the_closed_list(self):
        paragraph = next(line for line in CONTRACT.splitlines()
                         if line.startswith("**Open-failure reason (additive, frame version 1).**"))
        listed = re.search(r"closed list: (.*?)\. Send no other field", paragraph).group(1)
        self.assertEqual(set(re.findall(r"`([a-z_]+)` \(", listed)), set(link.OPEN_FAILED_REASONS))
        self.assertEqual(len(link.OPEN_FAILED_REASONS), 7)
        self.assertIn("`limits.openFailedReason` is `1`", paragraph)

    def test_the_flag_is_exactly_the_integer_1_in_limits(self):
        accepted = link.open_failed_reason_accepted
        self.assertTrue(accepted({"limits": {"openFailedReason": 1}}))
        self.assertTrue(accepted({"limits": dict(FAST_LIMITS, openFailedReason=1)}))
        for complete in ({}, {"limits": {}}, {"limits": dict(FAST_LIMITS)}, {"openFailedReason": 1},
                         {"limits": {"openFailedReason": 2}}, {"limits": {"openFailedReason": 0}},
                         {"limits": {"openFailedReason": "1"}}, {"limits": {"openFailedReason": True}},
                         {"limits": {"openFailedReason": 1.0}}, {"limits": [1]}, None):
            self.assertFalse(accepted(complete), complete)
        # The flag is not a size limit: it alone does not make the limits "advertised".
        self.assertEqual(link.read_limits({"limits": {"openFailedReason": 1}}), (link.FALLBACK_LIMITS, "fallback"))


class OpenFailedReasonLinkTests(LinkTestCase):
    async def open_failed(self, opener, *, limits, channel: str = CHANNEL) -> dict:
        async with Harness() as harness:
            self.active_record(harness.audience)
            service = self.service()
            service.local_opener = opener
            task = await self.running(service)
            try:
                ws, _ = await harness.accept(limits=limits)
                await ws.send(json.dumps({"version": 1, "type": "hermes-open", "channelId": channel}))
                return await next_frame(ws)
            finally:
                # One service per call: a running one would follow the next record to the next Harness.
                await self._stop(task)

    async def test_each_local_gateway_code_is_sent_as_its_reason_with_the_flag(self):
        for code in ("local_gateway_refused", "local_gateway_auth_unavailable", "local_gateway_unavailable",
                     link.SITE_PROTECTION_CODE):
            with self.subTest(code=code):
                frame = await self.open_failed(failing_opener(link.LocalGatewayError(code)), limits=FLAGGED)
                self.assertEqual(frame, coded(code))
                self.assertEqual(list(frame), ["version", "type", "channelId", "reason"])
                await until(lambda: self.status_link()["lastChannelErrorCode"] == code)

    async def test_without_the_flag_the_frame_has_no_reason_key(self):
        for limits in (None, FAST_LIMITS, dict(FAST_LIMITS, openFailedReason=2),
                       dict(FAST_LIMITS, openFailedReason="1"), dict(FAST_LIMITS, openFailedReason=True)):
            with self.subTest(limits=limits):
                frame = await self.open_failed(
                    failing_opener(link.LocalGatewayError("local_gateway_refused")), limits=limits)
                self.assertEqual(frame, legacy())
                self.assertNotIn("reason", frame)

    async def test_a_real_refusing_and_a_silent_local_gateway(self):
        # The dashboard refuses the socket (wrong session token): HTTP 403 before accept.
        async with Gateway() as gateway:
            refusing = lambda max_size: link.open_local_gateway(  # noqa: E731
                "", max_size, resolver=lambda _url: ("127.0.0.1", gateway.port, "not-the-token"))
            self.assertEqual(await self.open_failed(refusing, limits=FLAGGED), coded("local_gateway_refused"))
            self.assertEqual(await self.open_failed(refusing, limits=None), legacy())
        # Nothing listens on the gateway address.
        port = closed_port()
        silent = lambda max_size: link.open_local_gateway(  # noqa: E731
            "", max_size, resolver=lambda _url: ("127.0.0.1", port, "t"))
        self.assertEqual(await self.open_failed(silent, limits=FLAGGED), coded("local_gateway_unavailable"))
        # A dashboard page without a session token (a login page): no usable local credential.
        async with Gateway() as gateway:
            gateway.page = "<html><form>login</form></html>"
            gated = lambda max_size: link.open_local_gateway(  # noqa: E731
                f"http://127.0.0.1:{gateway.port}", max_size)
            self.assertEqual(await self.open_failed(gated, limits=FLAGGED), coded("local_gateway_auth_unavailable"))

    async def test_the_device_channel_limit_reads_channel_limit(self):
        async with Harness() as harness, Gateway() as gateway:
            self.active_record(harness.audience)
            await self.running(self.service(gateway))
            first, second = "12121212-1212-4121-8121-121212121212", "34343434-3434-4343-8343-343434343434"
            for limits, expected in ((dict(FLAGGED, maxChannels=1), coded("channel_limit", second)),
                                     (dict(FAST_LIMITS, maxChannels=1), legacy(second))):
                ws, _ = await harness.accept(limits=limits)
                await ws.send(json.dumps({"version": 1, "type": "hermes-open", "channelId": first}))
                self.assertEqual((await next_frame(ws))["type"], "hermes-opened")
                await next_frame(ws)  # gateway.ready
                await ws.send(json.dumps({"version": 1, "type": "hermes-open", "channelId": second}))
                self.assertEqual(await next_frame(ws), expected)
                await until(lambda: self.status_link()["lastChannelErrorCode"] == "channel_cap")
                await ws.close()
                await until(lambda: self.status_link()["state"] != "connected")

    async def test_an_open_past_the_deadline_reads_open_timeout(self):
        quick = dict(FLAGGED, openTimeoutMs=1000)
        self.assertEqual(await self.open_failed(never_opens, limits=quick), coded("open_timeout"))
        self.assertEqual(await self.open_failed(never_opens, limits=dict(FAST_LIMITS, openTimeoutMs=1000)), legacy())

    async def test_any_other_cause_reads_unknown_and_never_carries_upstream_text(self):
        for error in (RuntimeError(SECRET_TEXT), ValueError(SECRET_TEXT), link.LocalGatewayError(SECRET_TEXT),
                      link.LocalGatewayError("link_handshake_timeout")):
            with self.subTest(error=type(error).__name__):
                frame = await self.open_failed(failing_opener(error), limits=FLAGGED)
                self.assertEqual(frame, coded("unknown"))
                self.assertNotIn("dashboard-secret", json.dumps(frame))
                self.assertNotIn("10.0.0.7", json.dumps(frame))


if __name__ == "__main__":
    unittest.main()
