"""Hermes 1.0.118: a Room acknowledgement ahead of the local cursor is adopted (contract P2c-3i), paired with OpenClaw 0.2.93.

Case names are the keys of ``conformance.json`` ``parityP2c3iCases``; OpenClaw implements the same
names. The fixture is the Berlin trace (Digital sovereignty, 2026-11-20..25): local acknowledged
cursor 3220, terminal evidence only for 3221 (ignored), Room acknowledgedSeq 3222. Before the fix
every replayed acknowledgement of 3221 raised the retryable ack_frontier_mismatch.
No test uses a real credential, and no test talks to a live Room.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
import sys
import tempfile
from contextlib import contextmanager
import types
import unittest
from pathlib import Path
from unittest import mock

from test_delivery_lifecycle import ROOT, adapter, cli, state_store

CONFORMANCE = json.loads((ROOT / "conformance.json").read_text())
CONTRACT = (ROOT / "docs" / "delivery-lifecycle-contract-v1.md").read_text(encoding="utf-8")
local_status = adapter.local_status
renewal = adapter.renewal
recovery = adapter.recovery

ACK_AHEAD_CASES = [
    "ACK-AHEAD-01 adopt", "ACK-AHEAD-02 one_warning", "ACK-AHEAD-03 behind_errors",
    "ACK-AHEAD-04 non_int_errors", "ACK-AHEAD-05 restart_keeps_cursor",
    "ACK-AHEAD-06 renewal_resumes", "ACK-AHEAD-07 expired_intent_ends_cleanly",
]
BLOCK_SHA256 = "917c297d29da2b2282106a459f6ae223561f00024c42f02b2ff5aea9c8fc6b2e"
MEMBER = "member-1"
ROOM = "duev8o1h936bl9f"


def ignored(seq: int) -> dict:
    return {"status": "ignored", "sourceEventId": f"ev{seq}", "canonicalEventId": "", "canonicalSeq": 0,
            "canonicalTs": "", "reason": "not_addressed"}


def berlin() -> adapter.RoomBinding:
    """The persisted Berlin binding, loaded through RoomBinding.from_dict."""
    raw = {
        "base_url": "https://room.example/api", "room_id": ROOM, "membership_id": MEMBER,
        "credential": "credential", "installation_id": "inst-1",
        "cursor": 3221, "acknowledged_cursor": 3220,
        "inbox": {"3221": "ignored"},
        "terminal_evidence": {"3221": ignored(3221)},
    }
    return adapter.RoomBinding.from_dict(json.loads(json.dumps(raw)))


class AheadRoom:
    """The acknowledgement route only; ``answer`` builds the Room's reply for a requested seq."""

    def __init__(self, answer):
        self.answer = answer
        self.acks = []

    def acknowledge(self, _room, seq):
        self.acks.append(seq)
        return self.answer(seq)


def room_max(held: int):
    return lambda seq: {"acknowledgedSeq": max(seq, held)}


class Harness:
    def __init__(self, binding, api, directory=None, *, persist=False):
        self.scratch = tempfile.TemporaryDirectory()  # removed with the harness
        directory = Path(directory or self.scratch.name)
        self.binding, self.api = binding, api
        self.state_file = directory / "private" / "state.json"
        instance = object.__new__(adapter.SyntheticSocialityAdapter)
        instance._state_file = self.state_file
        instance._ledger_locks = {}
        instance._status = local_status.LocalStatus(directory / "status.json", plugin="hermes",
                                                    version=adapter.CONNECTOR_VERSION, host_label="host-a")
        if persist:
            instance._persist_binding = types.MethodType(adapter.SyntheticSocialityAdapter._persist_binding, instance)
        else:
            instance._persist_binding = lambda _current: True
        instance._call = lambda _binding, operation, **_options: asyncio.sleep(0, result=operation(api))
        instance._inflight_events = set()
        instance._retry_acknowledgement = mock.Mock(side_effect=AssertionError("no reconnect loop"))
        instance._status_room = mock.Mock()
        self.instance = instance

    def complete(self, seq, **options):
        with state_store.state_path_scope(self.state_file):
            asyncio.run(self.instance._complete_event(self.binding, seq, **options))

    def save(self):
        with state_store.state_path_scope(self.state_file):
            state_store.save(state_store.PluginState(bindings=[self.binding]))

    def reload(self) -> adapter.RoomBinding:
        return state_store.load(self.state_file).binding(ROOM)


class AckAheadConformanceTests(unittest.TestCase):
    def test_the_paragraph_follows_p2c_3h_byte_identical(self):
        start = CONTRACT.index("<<<P2c-3i\n")
        end = CONTRACT.index("\nP2c-3i>>>\n", start) + len("\nP2c-3i>>>\n")
        self.assertEqual(hashlib.sha256(CONTRACT[start:end].encode("utf-8")).hexdigest(), BLOCK_SHA256)
        self.assertTrue(CONTRACT[:start].endswith("\nP2c-3h>>>\n\n"))
        self.assertTrue(CONTRACT[end:].startswith("\n<<<P2c-3j\n"))
        self.assertEqual(CONTRACT.count("<<<P2c-3i"), 1)

    def test_the_contract_hash_is_the_pinned_one(self):
        digest = hashlib.sha256((ROOT / "docs" / "delivery-lifecycle-contract-v1.md").read_bytes()).hexdigest()
        self.assertEqual(CONFORMANCE["contractSha256"], digest)

    def test_the_case_names_are_the_shared_ones_and_each_has_a_test(self):
        self.assertEqual(CONFORMANCE["parityP2c3iCases"], ACK_AHEAD_CASES)
        names = set(dir(AckAheadCaseTests)) | set(dir(AckAheadRenewalTests))
        for case in ACK_AHEAD_CASES:
            number = case.split(" ", 1)[0].lower().replace("-", "_")
            self.assertTrue(any(name.startswith(f"test_{number}_") for name in names), case)


class AckAheadCaseTests(unittest.TestCase):
    def test_ack_ahead_01_a_room_ack_ahead_is_adopted_as_a_fence(self):
        harness = Harness(berlin(), AheadRoom(room_max(3222)))
        before = copy.deepcopy(harness.binding.terminal_evidence["3221"])
        # The replay of the durable 3221 completion that ran every poll.
        harness.complete(3221)
        binding = harness.binding
        self.assertEqual(harness.api.acks, [3221])
        self.assertEqual(binding.acknowledged_cursor, 3222)
        self.assertGreaterEqual(binding.cursor, 3222)
        self.assertEqual(binding.inbox["3222"], "ignored")
        self.assertEqual(binding.terminal_evidence["3222"]["status"], "ignored")
        self.assertEqual(binding.terminal_evidence["3222"]["reason"], "room_ack_ahead")
        self.assertEqual(binding.inbox["3221"], "ignored")
        self.assertEqual(binding.terminal_evidence["3221"], before)
        harness.instance._retry_acknowledgement.assert_not_called()
        for call in harness.instance._status_room.call_args_list:
            self.assertNotEqual(call.kwargs.get("connection"), "reconnecting")
        # Delivery continues from 3223: an earlier seq is not consumed again.
        self.assertIsNone(asyncio.run(harness.instance._consume_locked(binding, {"id": "ev3222", "seq": 3222})))
        self.assertEqual(binding.inbox["3222"], "ignored")

    def test_ack_ahead_01_a_deferred_acknowledgement_adopts_too(self):
        harness = Harness(berlin(), AheadRoom(room_max(3222)))
        harness.complete(3221, defer_ack_failure=True)
        self.assertEqual(harness.binding.acknowledged_cursor, 3222)
        harness.instance._retry_acknowledgement.assert_not_called()

    def test_ack_ahead_01_existing_evidence_in_the_adopted_range_is_kept(self):
        binding = berlin()
        binding.inbox["3223"] = "ignored"
        binding.terminal_evidence["3223"] = ignored(3223)
        binding.inbox["3222"] = "pending"
        harness = Harness(binding, AheadRoom(room_max(3224)))
        before = copy.deepcopy(binding.terminal_evidence["3223"])
        harness.complete(3221)
        self.assertEqual(harness.api.acks, [3221])
        self.assertEqual(binding.acknowledged_cursor, 3224)
        self.assertEqual(binding.terminal_evidence["3223"], before)
        for seq in ("3222", "3224"):
            self.assertEqual(binding.inbox[seq], "ignored")
            self.assertEqual(binding.terminal_evidence[seq]["reason"], "room_ack_ahead")

    def test_ack_ahead_02_one_warning_per_adoption_and_none_for_an_equal_answer(self):
        harness = Harness(berlin(), AheadRoom(room_max(3222)))
        with self.assertLogs(adapter.logger, level=logging.WARNING) as logs:
            harness.complete(3221)
        warnings = [record for record in logs.records if record.levelno >= logging.WARNING]
        self.assertEqual(len(warnings), 1)
        self.assertIn("room_ack_ahead", warnings[0].getMessage())
        self.assertIn(ROOM, warnings[0].getMessage())
        # The next polls replay the same events and the Room answers the same value.
        with self.assertNoLogs(adapter.logger, level=logging.WARNING):
            harness.complete(3221)
            harness.complete(3222)
            asyncio.run(harness.instance._consume_locked(harness.binding, {"id": "ev3221", "seq": 3221}))
        self.assertEqual(harness.binding.acknowledged_cursor, 3222)
        self.assertEqual(harness.api.acks, [3221])

    def test_ack_ahead_03_an_answer_behind_the_cursor_still_raises(self):
        harness = Harness(berlin(), AheadRoom(lambda _seq: {"acknowledgedSeq": 3220}))
        with self.assertRaises(adapter.ProtocolError) as raised:
            harness.complete(3221)
        self.assertEqual(raised.exception.code, "ack_frontier_mismatch")
        self.assertEqual(harness.binding.acknowledged_cursor, 3220)
        self.assertNotIn("3222", harness.binding.inbox)

    def test_ack_ahead_04_a_missing_or_non_int_answer_still_raises(self):
        for answer in (None, {}, {"acknowledgedSeq": None}, {"acknowledgedSeq": "3222"},
                       {"acknowledgedSeq": 3222.0}, {"acknowledgedSeq": True}):
            with self.subTest(answer=answer):
                harness = Harness(berlin(), AheadRoom(lambda _seq, answer=answer: answer))
                with self.assertRaises(adapter.ProtocolError) as raised:
                    harness.complete(3221)
                self.assertEqual(raised.exception.code, "ack_frontier_mismatch")
                self.assertEqual(harness.binding.acknowledged_cursor, 3220)
                self.assertNotIn("3222", harness.binding.inbox)

    def test_ack_ahead_05_a_restart_after_adoption_keeps_the_cursor(self):
        harness = Harness(berlin(), AheadRoom(room_max(3222)), persist=True)
        harness.save()
        harness.complete(3221)
        restarted = harness.reload()
        self.assertEqual(restarted.acknowledged_cursor, 3222)
        self.assertEqual(restarted.inbox.get("3222"), "ignored")
        self.assertEqual(restarted.terminal_evidence["3222"]["reason"], "room_ack_ahead")
        self.assertEqual(restarted.terminal_evidence["3221"], ignored(3221))
        # The restarted connector acknowledges nothing again for the adopted range.
        second = Harness(restarted, AheadRoom(room_max(3222)), harness.scratch.name, persist=True)
        with self.assertNoLogs(adapter.logger, level=logging.WARNING):
            second.complete(3221)
            second.complete(3222)
        self.assertEqual(second.api.acks, [])
        self.assertEqual(second.reload().acknowledged_cursor, 3222)

    def test_ack_ahead_05_another_room_is_untouched(self):
        harness = Harness(berlin(), AheadRoom(room_max(3222)), persist=True)
        other = berlin()
        other.room_id, other.membership_id = "room-other", "member-2"
        with state_store.state_path_scope(harness.state_file):
            state_store.save(state_store.PluginState(bindings=[harness.binding, other]))
        harness.complete(3221)
        state = state_store.load(harness.state_file)
        self.assertEqual(state.binding(ROOM).acknowledged_cursor, 3222)
        self.assertEqual(state.binding("room-other").acknowledged_cursor, 3220)
        self.assertNotIn("3222", state.binding("room-other").inbox)



# --- r1: renewal (ACK-AHEAD-06/07) and the site matrix -------------------------

REQUEST_ID = cli._new_renewal_secret()
INTENT = {"requestId": REQUEST_ID, "roomId": ROOM, "membershipId": MEMBER, "state": "owner_requested",
          "displayName": "Berlin", "identityVersion": 3, "baseCredentialGeneration": 1}


def berlin_expired() -> adapter.RoomBinding:
    """Berlin as the Room left it: credential expired, disabled, local ack 3220, evidence 3221."""
    binding = berlin()
    binding.display_name, binding.identity_version = "Berlin", 3
    binding.credential_expires_at = "2020-01-01T00:00:00Z"
    binding.enabled, binding.disabled_reason = False, "credential_expired"
    return binding


def live(name):
    """The package module as it is now (F2): other test modules call load_adapter() again, which
    re-executes state, protocol, cli and adapter (never renewal), and renewal.resume imports .cli
    at call time."""
    return sys.modules[f"{adapter.__name__.rsplit('.', 1)[0]}.{name}"]


@contextmanager
def live_renewal(shared):
    """The fake Room on the live cli, and the renewal engine on the live state module."""
    engine, current = live("renewal"), live("state")
    with mock.patch.object(live("cli"), "RoomProtocol", renewal_api(shared)), \
            mock.patch.object(engine, "state_path_scope", current.state_path_scope), \
            mock.patch.object(engine, "load", current.load):
        yield


def renewal_api(shared):
    """The renewal routes of one fake Room; ``shared`` holds the answers and the calls."""

    class API:
        def __init__(self, _url, credential=""):
            self.credential = credential

        def identity(self):
            ack = shared["ack_after_confirm"] if shared.get("confirmed") and "ack_after_confirm" in shared else shared["ack"]
            return {"roomId": ROOM, "membershipId": MEMBER, "displayName": "Berlin", "identityVersion": 3,
                    "deliveredSeq": shared.get("delivered", 3222), "acknowledgedSeq": ack,
                    "credentialExpiresAt": "2099-01-01T00:00:00Z", "confirmBy": "2099-01-01T00:00:00Z"}

        def request_credential_renewal(self, room_id, request_id, digest, *, client_instance_id=""):
            shared["calls"].append("request")
            if shared.get("lose_request"):
                shared["lose_request"] = False
                raise OSError("lost reply")
            return self.credential_renewal_request(room_id, request_id)

        def credential_renewal_request(self, room_id, request_id):
            state = shared.get("state", "issued")
            answer = {"roomId": ROOM, "membershipId": MEMBER, "requestId": request_id, "state": state}
            if state != "expired":
                answer.update(self.identity(), grantId="grant-1", baseCredentialGeneration=1)
            return answer

        def redeem_credential_renewal(self, room_id, grant_id, grant_secret, replacement):
            shared["calls"].append("redeem")
            shared["replacement"] = replacement
            return self.identity()

        def verify_credential_renewal(self, room_id, grant_id, replacement):
            shared["calls"].append("verify")
            return self.identity()

        def confirm_credential_renewal(self, room_id, grant_id, replacement):
            shared["calls"].append("confirm")
            shared["confirmed"] = True
            return {"id": "event-1", "type": "credential.renewed", "payload": self.identity()}

        def room_state(self, room_id):
            if not shared.get("confirmed"):
                raise AssertionError("an expired base never reads ordinary /state")
            shared["calls"].append("state")
            return {"roomId": room_id, "roster": [self.identity()]}

    return API


class RenewalFixture:
    """Berlin's state file with an owner renewal journal in phase requesting_owner (expected/protected 3220)."""

    def __init__(self, binding=None, other=None):
        self.scratch = tempfile.TemporaryDirectory()
        self.path = Path(self.scratch.name) / "private" / "state.json"
        self.path.parent.mkdir(mode=0o700)
        bindings = [binding or berlin_expired()] + ([other] if other else [])
        state_store.save(state_store.PluginState(bindings=bindings), self.path)
        shared = {"ack": 3222, "calls": [], "lose_request": True}
        with self.assertRaises(OSError):
            self.resume(shared, INTENT)
        journal = self.binding().credential_rotation
        assert journal["phase"] == "requesting_owner", journal
        assert journal["expected"]["acknowledged_cursor"] == 3220
        assert journal["protected_snapshot"]["acknowledged_cursor"] == 3220

    @staticmethod
    def assertRaises(kind):
        return _Raises(kind)

    def resume(self, shared, intent=None):
        with live_renewal(shared):
            renewal.resume(self.path, ROOM, intent)

    def binding(self):
        return state_store.load(self.path).binding(ROOM)


class _Raises:
    def __init__(self, kind):
        self.kind = kind

    def __enter__(self):
        return self

    def __exit__(self, kind, value, _traceback):
        if kind is None or not issubclass(kind, self.kind):
            raise AssertionError(f"expected {self.kind.__name__}, got {kind}")
        self.exception = value
        return True


def maintenance_instance(path):
    with mock.patch.object(adapter, "load", lambda *_: state_store.load(path)):
        instance = adapter.SyntheticSocialityAdapter(adapter.PlatformConfig())
    instance._state_file = path
    instance._membership_active = mock.AsyncMock(return_value=True)
    instance._watch = mock.AsyncMock()
    return instance


class AckAheadRenewalTests(unittest.IsolatedAsyncioTestCase):
    def test_ack_ahead_06_an_owner_renewal_resumes_and_completes_with_the_room_ahead(self):
        fixture = RenewalFixture()
        shared = {"ack": 3222, "calls": []}
        with self.assertLogs(cli.logger, level=logging.WARNING) as logs:
            fixture.resume(shared)
        saved = fixture.binding()
        self.assertEqual(saved.credential_rotation, {})
        self.assertEqual(saved.credential, shared["replacement"])
        self.assertTrue(saved.enabled)
        self.assertEqual(saved.acknowledged_cursor, 3222)
        self.assertEqual(saved.terminal_evidence["3222"]["reason"], "room_ack_ahead")
        self.assertEqual(saved.terminal_evidence["3221"], ignored(3221))
        self.assertEqual(shared["calls"], ["request", "redeem", "verify", "confirm", "state"])
        self.assertEqual(len([line for line in logs.output if "room_ack_ahead" in line]), 1)

    def test_ack_ahead_06_a_room_behind_the_journal_is_still_a_conflict(self):
        fixture = RenewalFixture()
        with self.assertRaises(recovery.AckFrontierConflict) as raised:
            fixture.resume({"ack": 3219, "calls": []})
        self.assertEqual(raised.exception.code, "ack_frontier_mismatch")
        self.assertRegex(str(raised.exception), "conflicts with local replay cursors")
        self.assertEqual(fixture.binding().acknowledged_cursor, 3220)
        self.assertEqual(fixture.binding().credential_rotation["phase"], "requesting_owner")

    async def test_ack_ahead_06_maintenance_does_not_cancel_the_room_again(self):
        fixture = RenewalFixture()
        instance = maintenance_instance(fixture.path)
        shared = {"ack": 3222, "calls": []}
        target = fixture.binding()
        with live_renewal(shared), \
                mock.patch.object(renewal, "discover", return_value=None) as discover:
            await instance._maintain_renewal(target)
            self.assertEqual(fixture.binding().credential_rotation, {})
            self.assertNotIn(ROOM, instance._renewal_retry_after)
            self.assertIn(ROOM, instance._tasks)  # the watch restarted on the renewed credential
            await asyncio.gather(instance._tasks[ROOM], return_exceptions=True)
            watcher = asyncio.create_task(asyncio.Event().wait())
            instance._tasks[ROOM] = watcher
            await instance._maintain_renewal(fixture.binding())
            discover.assert_called_once()
            self.assertFalse(watcher.done())  # no second drain: nothing left to resume
            watcher.cancel()
            await asyncio.gather(watcher, return_exceptions=True)

    def test_ack_ahead_07_an_expired_owner_intent_closes_the_journal_with_its_reason(self):
        fixture = RenewalFixture()
        shared = {"ack": 3222, "calls": [], "state": "expired"}
        with self.assertRaises(renewal.RenewalClosed) as raised:
            fixture.resume(shared)
        self.assertEqual(raised.exception.reason, "owner_request_expired")
        self.assertIn("new owner Renew", str(raised.exception))
        saved = fixture.binding()
        self.assertEqual(saved.credential_rotation, {})
        self.assertEqual(saved.renewal_closed["reason"], "owner_request_expired")
        self.assertEqual(saved.renewal_closed["requestId"], REQUEST_ID)
        self.assertEqual(saved.credential, "credential")  # nothing was swapped
        self.assertEqual(shared["calls"], ["request"])
        # A new owner Renew starts a fresh journal and clears the closure.
        intent = {**INTENT, "requestId": cli._new_renewal_secret()}
        fresh = {"ack": 3222, "calls": []}
        fixture.resume(fresh, intent)
        self.assertEqual(fixture.binding().renewal_closed, {})
        self.assertEqual(fixture.binding().acknowledged_cursor, 3222)

    async def test_ack_ahead_07_maintenance_ends_cleanly_and_stops_draining(self):
        fixture = RenewalFixture()
        instance = maintenance_instance(fixture.path)
        shared = {"ack": 3222, "calls": [], "state": "expired"}
        with live_renewal(shared), \
                mock.patch.object(renewal, "discover", return_value=None):
            with self.assertLogs(adapter.logger, level=logging.WARNING) as logs:
                await instance._maintain_renewal(fixture.binding())
            self.assertTrue(any("new owner Renew is needed" in line for line in logs.output))
            self.assertEqual(fixture.binding().credential_rotation, {})
            watcher = asyncio.create_task(asyncio.Event().wait())
            instance._tasks[ROOM] = watcher
            instance._renewal_retry_after.clear()
            for _ in range(3):
                await instance._maintain_renewal(fixture.binding())
            self.assertFalse(watcher.done())
            self.assertEqual(shared["calls"], ["request"])
            watcher.cancel()
            await asyncio.gather(watcher, return_exceptions=True)


class AckAheadSiteMatrixTests(unittest.TestCase):
    """ROOT-CAUSE RULE (r1): every site where a Room acknowledgement meets a local cursor.

    Local 3220, evidence 3221 (ignored), Room 3222: the same outcome everywhere
    (ack 3222, 3222 ignored/room_ack_ahead, 3221 unchanged, one warning). Room
    3219: the same conflict code everywhere, nothing changes.
    """

    def outcome(self, binding):
        self.assertEqual(binding.acknowledged_cursor, 3222)
        self.assertEqual(binding.terminal_evidence["3222"]["reason"], "room_ack_ahead")
        self.assertEqual(binding.terminal_evidence["3221"], ignored(3221))

    def live_ack(self, room_ack):
        harness = Harness(berlin(), AheadRoom(lambda _seq: {"acknowledgedSeq": room_ack}))
        harness.complete(3221)
        return harness.binding

    def reconnect(self, room_ack):
        harness = Harness(berlin(), AheadRoom(lambda _seq: {"acknowledgedSeq": room_ack}))
        with state_store.state_path_scope(harness.state_file):
            asyncio.run(harness.instance._consume_locked(harness.binding, {"id": "ev3221", "seq": 3221}))
        return harness.binding

    def stuck_scan(self, room_ack):
        harness = Harness(berlin(), AheadRoom(lambda _seq: {"acknowledgedSeq": room_ack}))
        asyncio.run(harness.instance._apply_stuck_events(harness.binding))
        if harness.binding.acknowledged_cursor == 3220:
            raise recovery.AckFrontierConflict("stuck scan logged the conflict")
        return harness.binding

    def restart_load(self, room_ack):
        harness = Harness(berlin(), AheadRoom(lambda _seq: {"acknowledgedSeq": room_ack}), persist=True)
        harness.save()
        loaded = Harness(harness.reload(), harness.api, harness.scratch.name, persist=True)
        loaded.complete(3221)
        return loaded.reload()

    def renewal_preflight(self, room_ack):
        fixture = RenewalFixture()
        fixture.resume({"ack": room_ack, "calls": [], "state": "requested"})
        return fixture.binding()

    def renewal_active_state(self, room_ack):
        fixture = RenewalFixture()
        # The Room moves only after the confirm: only the active-state verify sees it.
        fixture.resume({"ack": 3220, "ack_after_confirm": room_ack, "delivered": 3222, "calls": []})
        return fixture.binding()

    SITES = ("live_ack", "reconnect", "renewal_preflight", "renewal_active_state", "stuck_scan", "restart_load")

    def test_every_site_adopts_a_room_ahead_the_same_way(self):
        for site in self.SITES:
            with self.subTest(site=site):
                self.outcome(getattr(self, site)(3222))

    def test_every_site_reports_a_room_behind_as_the_one_conflict(self):
        for site in self.SITES:
            with self.subTest(site=site):
                with self.assertRaises(Exception) as raised:
                    getattr(self, site)(3219)
                self.assertEqual(getattr(raised.exception, "code", ""), "ack_frontier_mismatch", site)

    def test_every_site_reports_a_non_integer_room_ack_as_the_one_conflict(self):
        # OP-12 r3 F1: a Room acknowledgement that is a string, a float or missing is the one
        # conflict at every site, never coerced into a fence.
        for site in self.SITES:
            for value in ("3222", 3222.0, None):
                with self.subTest(site=site, value=value):
                    with self.assertRaises(Exception) as raised:
                        getattr(self, site)(value)
                    self.assertEqual(getattr(raised.exception, "code", ""), "ack_frontier_mismatch", (site, value))


if __name__ == "__main__":
    unittest.main()
