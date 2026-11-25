"""Hermes 1.0.118: an expired base credential never waits for in-flight work (contract P2c-3j), paired with OpenClaw 0.2.93.

Case names are the keys of ``conformance.json`` ``parityP2c3jCases``; OpenClaw implements the same
names. Live finding (25 Nov 2026, Aura in Networks and Kabeltracker, OpenClaw 0.2.92): an owner Renew
never started because unfinished delivery and lifecycle intents counted as blocking, and they could
never drain without a valid credential. Hermes had the same rule in renewal.quiescent. The intents
are built by the real delivery paths (_send_final, _record_lifecycle_pending).
No test uses a real credential, and no test talks to a live Room.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from test_delivery_lifecycle import (
    ROOT, adapter, cli, configured_instance, lifecycle_binding, persisted_instance, state_store,
)

CONFORMANCE = json.loads((ROOT / "conformance.json").read_text())
CONTRACT = (ROOT / "docs" / "delivery-lifecycle-contract-v1.md").read_text(encoding="utf-8")
PACKAGE = adapter.__name__.rsplit(".", 1)[0]


def live(name):
    """The package module as it is now: other test modules call load_adapter() again, which replaces
    the package's modules in sys.modules, and renewal.resume imports .cli at call time."""
    return sys.modules[f"{PACKAGE}.{name}"]

CASES = [
    "RENEW-INFLIGHT-01 delivery_pending",
    "RENEW-INFLIGHT-02 lifecycle_blocked",
    "RENEW-INFLIGHT-03 valid_credential_unchanged",
]
BLOCK_SHA256 = "12562226cb850af791e70a554e0156e4dee36ad73032a888dc2931e09ee4089d"
ROOM, MEMBER = "room-1", "member-1"
INTENT = {"requestId": cli._new_renewal_secret(), "roomId": ROOM, "membershipId": MEMBER, "state": "owner_requested",
          "displayName": "Aura", "identityVersion": 2, "baseCredentialGeneration": 1}


def aura(binding, *, expired=True, refused=False):
    """Aura's identity and credential state on a binding that already holds the in-flight work."""
    binding.display_name, binding.identity_version = "Aura", 2
    binding.installation_id = binding.installation_id or "installation-1"
    binding.credential_expires_at = "2020-01-01T00:00:00Z" if expired else "2099-01-01T00:00:00Z"
    if refused:
        # The Room refused the base as credential_expired (adapter _expire).
        binding.enabled, binding.disabled_reason = False, "credential_expired"
    return binding


def delivery_pending_binding():
    binding = adapter.RoomBinding("https://room.example/api", ROOM, MEMBER, "credential", display_name="Aura",
                                  identity_version=2, installation_id="installation-1", cursor=5, acknowledged_cursor=4)
    instance = configured_instance(binding, None, [])

    class API:
        def room_state(self, _room_id):
            return {"headSeq": 5, "activeEpoch": {"id": "epoch-1"}}

        def post_message(self, *_args, **_kwargs):
            raise adapter.ProtocolError("network unreachable", code="", retryable=True)

    instance._call = lambda _binding, operation, **_options: asyncio.sleep(0, result=operation(API()))
    instance._publish = lambda *_args, **_kwargs: asyncio.sleep(0)
    result = asyncio.run(instance._send_final(
        ROOM, adapter._dispatch_source_ref("evt-5", "generation-7"), "A frozen answer."))
    assert not result.success and binding.delivery_intents["evt-5"]["delivery_state"] == "delivery_pending"
    return binding


def lifecycle_blocked_binding(code="credential_expired"):
    binding = lifecycle_binding()
    binding.identity_version = 2
    binding.delivery_lifecycle["evt-5"]["binding"]["identity_version"] = 2
    instance = object.__new__(adapter.SyntheticSocialityAdapter)
    instance._persist_binding = lambda _binding: True
    instance._record_lifecycle_pending(
        binding, "evt-5", adapter.ProtocolError("refused", code=code, retryable=False), error_code=code)
    assert binding.delivery_lifecycle["evt-5"]["lifecycle_state"] == "blocked"
    return binding


def renewal_api(calls, shared):
    class API:
        def __init__(self, _url, credential=""):
            self.credential = credential

        def identity(self):
            return {"roomId": ROOM, "membershipId": MEMBER, "displayName": "Aura", "identityVersion": 2,
                    "deliveredSeq": 5, "acknowledgedSeq": 4,
                    "credentialExpiresAt": "2099-01-01T00:00:00Z", "confirmBy": "2099-01-01T00:00:00Z"}

        def request_credential_renewal(self, room_id, request_id, digest, *, client_instance_id=""):
            calls.append("request")
            return self.credential_renewal_request(room_id, request_id)

        def credential_renewal_request(self, room_id, request_id):
            return {**self.identity(), "requestId": request_id, "grantId": "grant-1",
                    "baseCredentialGeneration": 1, "state": "issued"}

        def redeem_credential_renewal(self, room_id, grant_id, grant_secret, replacement):
            calls.append("redeem")
            shared["replacement"] = replacement
            return self.identity()

        def verify_credential_renewal(self, room_id, grant_id, replacement):
            calls.append("verify")
            return self.identity()

        def confirm_credential_renewal(self, room_id, grant_id, replacement):
            if shared.pop("lose_confirm", False):
                calls.append("confirm-lost")
                raise OSError("lost reply")
            calls.append("confirm")
            shared["confirmed"] = True
            return {"id": "event-1", "type": "credential.renewed", "payload": self.identity()}

        def room_state(self, room_id):
            assert shared.get("confirmed"), "an expired base never reads ordinary /state"
            calls.append("state")
            return {"roomId": room_id, "roster": [self.identity()]}

    return API


class Store:
    def __init__(self, binding):
        self.scratch = tempfile.TemporaryDirectory()
        self.path = Path(self.scratch.name) / "private" / "state.json"
        self.path.parent.mkdir(mode=0o700)
        state_store.save(state_store.PluginState(bindings=[binding]), self.path)

    def renew(self, shared=None):
        calls, shared = [], ({} if shared is None else shared)
        # load_adapter() re-executes state, protocol, cli and adapter but never renewal, so the renewal
        # module may still hold an earlier state module; run it against the live one.
        engine, current = live("renewal"), live("state")
        with mock.patch.object(live("cli"), "RoomProtocol", renewal_api(calls, shared)), \
                mock.patch.object(engine, "state_path_scope", current.state_path_scope), \
                mock.patch.object(engine, "load", current.load):
            engine.resume(self.path, ROOM, INTENT)
        return calls, shared

    def binding(self):
        return live("state").load(self.path).binding(ROOM)


class RenewInflightConformanceTests(unittest.TestCase):
    def test_the_paragraph_follows_p2c_3i_byte_identical(self):
        start = CONTRACT.index("<<<P2c-3j\n")
        end = CONTRACT.index("\nP2c-3j>>>\n", start) + len("\nP2c-3j>>>\n")
        self.assertEqual(hashlib.sha256(CONTRACT[start:end].encode("utf-8")).hexdigest(), BLOCK_SHA256)
        self.assertTrue(CONTRACT[:start].endswith("\nP2c-3i>>>\n\n"))
        self.assertTrue(CONTRACT[end:].startswith("\nP2c-4 Local status. "))
        self.assertEqual(CONTRACT.count("<<<P2c-3j"), 1)

    def test_the_case_names_are_the_shared_ones_and_each_has_a_test(self):
        self.assertEqual(CONFORMANCE["parityP2c3jCases"], CASES)
        names = set(dir(RenewInflightTests))
        for case in CASES:
            number = case.split(" ", 1)[0].lower().replace("-", "_")
            self.assertTrue(any(name.startswith(f"test_{number}_") for name in names), case)


class RenewInflightTests(unittest.TestCase):
    def test_renew_inflight_01_an_expired_base_renews_first_and_the_frozen_delivery_resumes(self):
        for label, refused in (("stored expiry passed", False), ("the Room refused it as credential_expired", True)):
            with self.subTest(label):
                store = Store(aura(delivery_pending_binding(), refused=refused))
                before = copy.deepcopy(store.binding().delivery_intents["evt-5"])
                calls, shared = store.renew()
                saved = store.binding()
                self.assertEqual(calls, ["request", "redeem", "verify", "confirm", "state"])
                self.assertEqual(saved.credential, shared["replacement"])
                self.assertEqual(saved.credential_rotation, {})
                self.assertTrue(saved.enabled)
                self.assertEqual(saved.delivery_intents["evt-5"], before, "the frozen intent is unchanged")
                # The normal delivery path resumes it: the same frozen bytes, the new credential.
                instance = configured_instance(saved, None, [])
                posts = []

                class API:
                    def room_state(self, _room_id):
                        return {"headSeq": 5, "activeEpoch": {"id": "epoch-1"}}

                    def post_message(self, room_id, turn_id, observed_seq, source_event_id, body, *args, **kwargs):
                        posts.append({"credential": saved.credential, "body": body, "source": source_event_id})
                        return {"id": "posted-6", "seq": 6, "ts": "2026-11-25T06:00:00Z"}

                instance._call = lambda _binding, operation, **_options: asyncio.sleep(0, result=operation(API()))
                instance._publish = lambda *_args, **_kwargs: asyncio.sleep(0)
                result = asyncio.run(instance._send_final(
                    ROOM, adapter._dispatch_source_ref("evt-5", "generation-7"), "A frozen answer."))
                self.assertTrue(result.success)
                self.assertEqual(len(posts), 1)
                self.assertEqual(posts[0]["credential"], shared["replacement"])
                self.assertEqual(posts[0]["body"], "A frozen answer.")
                self.assertEqual(saved.delivery_intents["evt-5"]["delivery_state"], "posted")

    def test_renew_inflight_01_a_lost_confirm_reply_resumes_after_the_swap(self):
        store = Store(aura(delivery_pending_binding()))
        before = copy.deepcopy(store.binding().delivery_intents["evt-5"])
        with self.assertRaises(OSError):
            store.renew({"lose_confirm": True})
        swapped = store.binding()
        self.assertEqual(swapped.credential_rotation["phase"], "swapped")
        self.assertEqual(swapped.credential_expires_at, "2099-01-01T00:00:00Z")  # the replacement's expiry
        self.assertTrue(live("renewal").quiescent(swapped, for_renewal=True))  # the journal began on an expired base
        store.renew()
        saved = store.binding()
        self.assertEqual(saved.credential_rotation, {})
        self.assertEqual(saved.delivery_intents["evt-5"], before)

    def test_renew_inflight_02_lifecycle_blocked_by_expiry_is_reopened_and_completes(self):
        store = Store(aura(lifecycle_blocked_binding()))
        calls, shared = store.renew()
        saved = store.binding()
        self.assertEqual(saved.credential, shared["replacement"])
        journal, intent = saved.delivery_lifecycle["evt-5"], saved.delivery_intents["evt-5"]
        self.assertEqual((journal["lifecycle_state"], journal["state"], journal["automatic_retry"], journal["attempts"]),
                         ("pending", "lifecycle_pending", True, 0))
        self.assertEqual((intent["lifecycle_state"], intent["state"]), ("pending", "lifecycle_pending"))
        self.assertEqual(journal["receipt"]["canonical_event_id"], "posted-6")
        completions = []

        async def repair():
            instance, original_update = persisted_instance(store.path)

            class API:
                def complete_discussion_attempt(self, room_id, cycle_id, attempt_id, payload):
                    completions.append((live("state").load(store.path).binding(room_id).credential, cycle_id, payload["eventId"]))
                    return {"state": "completed"}

            instance._call = lambda _binding, operation, **_options: asyncio.sleep(0, result=operation(API()))
            try:
                reloaded = live("state").load(store.path).binding(ROOM)
                await instance._repair_pending_lifecycles(reloaded)
            finally:
                adapter.update = original_update

        asyncio.run(repair())
        self.assertEqual(completions, [(shared["replacement"], "cycle-1", "posted-6")])
        self.assertNotIn("evt-5", store.binding().delivery_lifecycle)

    def test_renew_inflight_02_lifecycle_blocked_for_another_reason_stays_blocked(self):
        store = Store(aura(lifecycle_blocked_binding("cycle_conflict")))
        before = copy.deepcopy(store.binding().delivery_lifecycle["evt-5"])
        store.renew()
        self.assertEqual(store.binding().delivery_lifecycle["evt-5"], before)

    def test_renew_inflight_03_a_valid_credential_keeps_the_quiescence_rule(self):
        for build in (delivery_pending_binding, lifecycle_blocked_binding):
            with self.subTest(build.__name__):
                store = Store(aura(build(), expired=False))
                binding = store.binding()
                self.assertFalse(live("renewal").quiescent(binding, for_renewal=True))
                self.assertFalse(live("renewal").quiescent(binding))
                with self.assertRaisesRegex(ValueError, "eligible quiescent"):
                    store.renew()
                self.assertEqual(store.binding().credential, "credential")

    def test_renew_inflight_03_the_owner_control_fence_rule_is_unchanged(self):
        binding = aura(delivery_pending_binding())
        # renewal.quiescent without for_renewal is the fence's rule: unfinished work is work.
        self.assertFalse(live("renewal").quiescent(binding))
        self.assertTrue(live("renewal").quiescent(binding, for_renewal=True))
        binding.revoked = True
        self.assertFalse(live("renewal").eligible(binding))


if __name__ == "__main__":
    unittest.main()
