"""Hermes 1.0.111: the "Link features" paragraph (contract, OpenClaw link parity).

The paragraph amends L1o for OpenClaw only. Hermes ships it because the
contract file is byte-identical in both archives; a Hermes device ignores
``limits.openclawLinkFeatures`` (LF-17). A fake Harness and a fake local
Gateway on loopback stand in for the real ones; no real service, token or key
is used.
"""

from __future__ import annotations

import json
import unittest

from test_parity_l1 import FAST_LIMITS, READY, ROOT, Gateway, Harness, LinkTestCase, link, next_frame

CONFORMANCE = json.loads((ROOT / "conformance.json").read_text(encoding="utf-8"))
CONTRACT = (ROOT / "docs" / "delivery-lifecycle-contract-v1.md").read_text(encoding="utf-8")
OPENCLAW_ONLY = "openclaw-only"
FEATURE_CASES = [
    "LF-01 without limits.openclawLinkFeatures 1 the opened frame has no features, the connect asks no extra scope, and every LF method is method_not_allowed and every approval event is dropped",
    "LF-02 with the capability the opened frame carries features create, archive, attachments, approvals in that order after channelId",
    "LF-03 sessions.create keeps only agentId",
    "LF-04 sessions.patch keeps only key and archived true, drops every other key, and refuses a missing or other archived with invalid_params",
    "LF-05 sessions.patch for a key not returned on the same channel is invalid_params and never reaches the gateway",
    "LF-06 an unlisted method (sessions.delete, sessions.reset, connect) stays method_not_allowed with the capability",
    "LF-07 with approvals the connect asks operator.approvals in addition to operator.read and operator.write",
    "LF-07b without operator.approvals in the hello-ok auth.scopes the opened frame omits approvals and no approval method or event passes; a connect refused with the extra scope is retried once without it and the channel opens without approvals",
    "LF-08 an approval request event is forwarded, reduced, only when its session belongs to a served session of the channel, nested keys included, never a prefix sibling",
    "LF-08b an approval request whose command (or any kept request string) exceeds 32768 characters is not forwarded, is not pending, and its resolve is invalid_params",
    "LF-09 an approval request event of a session the channel does not serve, or of another channel, is never forwarded",
    "LF-10 an approval answer for a request that is not pending on the channel is invalid_params and never reaches the gateway",
    "LF-11 an approval answer for a request pending on another channel, or of the other kind, is invalid_params",
    "LF-12 a second answer for the same request (an answered id) is invalid_params; it is pending again only after the gateway answered the first with ok false and no resolved event came",
    "LF-13 an approval answer with a decision outside the request's allowed decisions, an expired request, or a key beyond id and decision is refused",
    "LF-14 the approval list answer is a bare array of the channel's own rows, reduced, and those rows become pending",
    "LF-14b entries are forgotten on their resolved event, on expiry and on channel close; a list replay does not re-arm a pending or answered id and leaves out rows beyond the 32 entries; a full channel forwards no further request",
    "LF-15 a resolved event is forwarded only for a pending or answered id and ends its entry",
    "LF-16 with attachments chat.send keeps valid attachments and refuses an invalid type, mime type, file name, base64, more than 10 files or more than 716800 decoded bytes with invalid_params",
    "LF-17 a Hermes device ignores limits.openclawLinkFeatures and its hermes-opened frame is unchanged",
]
LINK_FEATURES = {
    "capability": "openclawLinkFeatures",
    "closedList": [
        "create",
        "archive",
        "attachments",
        "approvals"
    ],
    "methods": {
        "create": [
            "sessions.create"
        ],
        "archive": [
            "sessions.patch"
        ],
        "attachments": [
            "chat.send"
        ],
        "approvals": [
            "exec.approval.list",
            "plugin.approval.list",
            "exec.approval.resolve",
            "plugin.approval.resolve"
        ]
    },
    "events": {
        "approvals": [
            "exec.approval.requested",
            "plugin.approval.requested",
            "exec.approval.resolved",
            "plugin.approval.resolved"
        ]
    },
    "scope": {
        "approvals": "operator.approvals"
    }
}
CHANNEL = "f0f0f0f0-f0f0-4f0f-8f0f-f0f0f0f0f0f0"


class LinkFeaturesConformanceTests(unittest.TestCase):
    def test_the_paragraph_follows_the_open_failure_reason_paragraph(self):
        lines = CONTRACT.split("\n")
        reason = next(index for index, line in enumerate(lines)
                      if line.startswith("**Open-failure reason (additive, frame version 1).**"))
        self.assertEqual(lines[reason + 1], "")
        self.assertTrue(lines[reason + 2].startswith("**Link features (additive, frame version 1, 2026-11-17).**"))
        self.assertIn("Hermes is unchanged by it", lines[reason + 2])
        self.assertIn("`limits.openclawLinkFeatures` is `1`", CONTRACT)
        self.assertIn("P11b", CONFORMANCE["parityContractSectionSha256"])

    def test_the_link_features_and_the_case_names_are_the_shared_ones(self):
        self.assertEqual(CONFORMANCE["parityL1oLinkFeatures"], LINK_FEATURES)
        cases = CONFORMANCE["parityL1oFeatureCases"]
        self.assertEqual(list(cases), FEATURE_CASES)
        sources = "\n".join(path.read_text(encoding="utf-8") for path in (ROOT / "tests").glob("test_*.py"))
        for name, method in cases.items():
            if name.startswith("LF-17 "):
                self.assertEqual(method, "test_lf_17_a_hermes_device_ignores_the_capability_and_its_opened_frame_is_unchanged")
                self.assertEqual(sources.count(f"def {method}(self"), 1, method)
            else:
                # LF-01 to LF-16 (with LF-07b, LF-08b, LF-14b) are OpenClaw's device link only.
                self.assertEqual(method, OPENCLAW_ONLY, name)


class LinkFeaturesHermesDeviceTests(LinkTestCase):
    async def test_lf_17_a_hermes_device_ignores_the_capability_and_its_opened_frame_is_unchanged(self):
        variants = (None, 1, 2, 0, "1", True, ["create"])
        async with Harness() as harness, Gateway() as gateway:
            self.active_record(harness.audience)
            service = self.service(gateway)
            task = await self.running(service)
            for value in variants:
                with self.subTest(openclawLinkFeatures=value):
                    limits = dict(FAST_LIMITS) if value is None else dict(FAST_LIMITS, openclawLinkFeatures=value)
                    ws, proof = await harness.accept(limits=limits)
                    self.assertNotIn("features", proof)
                    await ws.send(json.dumps({"version": 1, "type": "hermes-open", "channelId": CHANNEL}))
                    opened = await ws.recv()
                    self.assertEqual(opened, '{"version":1,"type":"hermes-opened","channelId":"' + CHANNEL + '"}')
                    self.assertEqual((await next_frame(ws))["data"], READY)
                    await ws.close()
            await self._stop(task)

    async def test_lf_17_the_open_failed_reason_rules_are_unchanged_by_the_capability(self):
        async def refused(_max_size: int):
            raise link.LocalGatewayError("local_gateway_refused")

        for limits, expected in (
            (dict(FAST_LIMITS, openclawLinkFeatures=1),
             {"version": 1, "type": "hermes-open-failed", "channelId": CHANNEL}),
            (dict(FAST_LIMITS, openclawLinkFeatures=1, openFailedReason=1),
             {"version": 1, "type": "hermes-open-failed", "channelId": CHANNEL, "reason": "local_gateway_refused"}),
        ):
            with self.subTest(limits=limits):
                async with Harness() as harness:
                    self.active_record(harness.audience)
                    service = self.service()
                    service.local_opener = refused
                    task = await self.running(service)
                    try:
                        ws, _ = await harness.accept(limits=limits)
                        await ws.send(json.dumps({"version": 1, "type": "hermes-open", "channelId": CHANNEL}))
                        self.assertEqual(await next_frame(ws), expected)
                    finally:
                        await self._stop(task)


if __name__ == "__main__":
    unittest.main()
