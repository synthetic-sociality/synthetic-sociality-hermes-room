"""Shared conformance cases P2c-4-M1..M6 (2026-11-14, tkw-049): local status per (Room, membership).

Delivery contract P2c-4 and its Appendix "Per-membership status"; case names
are the keys of ``conformance.json`` ``parityP2c4MembershipCases``; each names
exactly one test here (its method is ``test_`` plus the name in snake case).
OpenClaw 0.2.83 implements the same names. The updates go through the
adapter's own ``_status_room`` and ``_end_session_wait``, as the Room work
does, into a real ``LocalStatus`` writing a real file.
"""

from __future__ import annotations

import json
import re
import tempfile
import time
import unittest
from pathlib import Path

from test_delivery_lifecycle import ROOT, adapter
from test_parity_p15 import time_limit

CONFORMANCE = json.loads((ROOT / "conformance.json").read_text())
CONTRACT = (ROOT / "docs" / "delivery-lifecycle-contract-v1.md").read_text()
local_status = adapter.local_status
CONTRACT_SHA256 = "b31e2b9747530cd421d5dd5328dae77f03eed9d355d9b597325cd3f64b783c54"


def method_name(case: str) -> str:
    return "test_" + re.sub(r"[^a-z0-9]+", "_", case.lower()).strip("_")


def binding(membership_id: str, room_id: str = "room-1"):
    return adapter.RoomBinding("https://room.example/api", room_id, membership_id, "credential-TEST")


class Gateway:
    """One adapter's status path, with a real status file."""

    def __init__(self, directory: str) -> None:
        self.path = Path(directory) / "status.json"
        self.instance = adapter.SyntheticSocialityAdapter.__new__(adapter.SyntheticSocialityAdapter)
        self.instance._status = local_status.LocalStatus(self.path, plugin="hermes", version=adapter.CONNECTOR_VERSION,
                                                         host_label="host-a")

    def update(self, room_binding, **fields) -> None:
        self.instance._status_room(room_binding, **fields)

    def read(self) -> dict:
        return local_status.read(self.path)


def unstamped(entry: dict) -> dict:
    return {key: value for key, value in entry.items() if not key.endswith("At")}


class P2c4MembershipConformanceTests(unittest.TestCase):
    @time_limit(30)
    def test_the_contract_and_the_membership_cases_are_the_shared_ones(self):
        import hashlib

        self.assertEqual(hashlib.sha256((ROOT / "docs" / "delivery-lifecycle-contract-v1.md").read_bytes()).hexdigest(),
                         CONTRACT_SHA256)
        self.assertEqual(CONFORMANCE["contractSha256"], CONTRACT_SHA256)
        self.assertIn("The connection state is kept per (Room, membership) (2026-11-14, tkw-049)", CONTRACT)
        cases = CONFORMANCE["parityP2c4MembershipCases"]
        self.assertEqual(list(cases), [f"P2c-4-M{index}" for index in range(1, 7)])
        sources = "\n".join(path.read_text() for path in (ROOT / "tests").glob("test_*.py"))
        for key, case in cases.items():
            self.assertTrue(case.startswith(key + " "), case)
            self.assertEqual(sources.count(f"def {method_name(case)}(self"), 1, case)


class P2c4MembershipTests(unittest.TestCase):
    @time_limit(30)
    def test_p2c_4_m1_two_accounts_in_one_room_each_keep_their_own_memberships_entry(self):
        with tempfile.TemporaryDirectory() as directory:
            gateway = Gateway(directory)
            aura, claw = binding("member-aura"), binding("member-claw")
            gateway.update(aura, connected=True, registered=True)
            gateway.update(claw, connected=True, registered=True)
            gateway.update(claw, connection="reconnecting", error_code="network")
            written = gateway.read()
            memberships = written["memberships"]
            self.assertEqual(sorted(memberships), ["member-aura", "member-claw"])
            self.assertEqual({key: (entry["roomId"], entry["membershipId"], entry["connection"], entry["lastErrorCode"])
                              for key, entry in memberships.items()},
                             {"member-aura": ("room-1", "member-aura", "connected", ""),
                              "member-claw": ("room-1", "member-claw", "reconnecting", "network")})
            summary = written["rooms"]["room-1"]
            self.assertEqual(summary["membershipIds"], ["member-aura", "member-claw"])
            self.assertNotIn("roomId", summary)

    @time_limit(30)
    def test_p2c_4_m2_one_membership_stopping_changes_only_its_own_entry(self):
        with tempfile.TemporaryDirectory() as directory:
            gateway = Gateway(directory)
            aura, claw = binding("member-aura"), binding("member-claw")
            gateway.update(aura, connected=True, registered=True)
            gateway.update(claw, connected=True, registered=True)
            before = gateway.read()["memberships"]["member-aura"]
            gateway.update(claw, connection="stopped", error_code="credential_revoked")
            written = gateway.read()
            self.assertEqual(written["memberships"]["member-aura"], before)
            self.assertEqual((written["memberships"]["member-claw"]["connection"],
                              written["memberships"]["member-claw"]["lastErrorCode"]), ("stopped", "credential_revoked"))
            # The stopped membership is the worst; it represents the Room.
            self.assertEqual((written["rooms"]["room-1"]["membershipId"], written["rooms"]["room-1"]["connection"]),
                             ("member-claw", "stopped"))

    @time_limit(30)
    def test_p2c_4_m3_the_rooms_summary_is_derived_deterministically_and_does_not_alternate_with_the_writer(self):
        with tempfile.TemporaryDirectory() as directory:
            gateway = Gateway(directory)
            aura, claw = binding("member-aura"), binding("member-claw")
            gateway.update(aura, connected=True, registered=True)
            gateway.update(claw, connected=True, registered=True)
            gateway.update(claw, connection="inactive", inactive=True, error_code="connector_not_active")
            seen = []
            for _ in range(3):
                # Heartbeats of both accounts in turn, as the two loops write.
                for writer in (aura, claw):
                    gateway.update(writer, connected=True)
                    summary = gateway.read()["rooms"]["room-1"]
                    seen.append((summary["membershipId"], summary["connection"], summary["lastErrorCode"]))
            # Claw is muted: `inactive` is worse than `connected`, whoever wrote last.
            self.assertEqual(set(seen), {("member-claw", "inactive", "connector_not_active")}, f"alternated: {seen}")
            # The same updates in the other order give the same summary.
            with tempfile.TemporaryDirectory() as other:
                reverse = Gateway(other)
                reverse.update(claw, connected=True, registered=True)
                reverse.update(claw, connection="inactive", inactive=True, error_code="connector_not_active")
                reverse.update(aura, connected=True, registered=True)
                self.assertEqual(unstamped(reverse.read()["rooms"]["room-1"]), unstamped(gateway.read()["rooms"]["room-1"]))
            # Ties of the worst connection go to the smallest membership id.
            gateway.update(aura, connection="reconnecting", error_code="network")
            gateway.update(claw, inactive=False, connection="reconnecting", error_code="timeout")
            summary = gateway.read()["rooms"]["room-1"]
            self.assertEqual((summary["membershipId"], summary["lastErrorCode"]), ("member-aura", "network"))

    @time_limit(30)
    def test_p2c_4_m4_a_single_account_gateway_keeps_its_rooms_entry_unchanged_and_mirrors_it_in_memberships(self):
        with tempfile.TemporaryDirectory() as directory:
            gateway = Gateway(directory)
            only = binding("member-1")
            gateway.update(only, connected=True, registered=True,
                           ledger={"acknowledgedSeq": 3, "holeSeqs": [5], "quarantinedSeqs": [], "ledgerCode": "ack_hole"})
            gateway.update(only, connection="reconnecting", error_code="network")
            written = gateway.read()
            entry, summary = written["memberships"]["member-1"], written["rooms"]["room-1"]
            self.assertEqual({**entry, "membershipIds": ["member-1"]}, {**summary, "roomId": "room-1"})
            # The Room entry keeps every key it had before 1.0.108, plus membershipIds.
            self.assertEqual(set(summary), {
                "membershipId", "connection", "lastConnectedAt", "lastErrorCode", "lastErrorAt", "lastGateWaitSeconds",
                "lastGateWaitAt", "lastGateWaitReason", "lastReconnectedAt", "lastOutageSeconds", "acknowledgedSeq",
                "holeSeqs", "quarantinedSeqs", "ledgerCode", "membershipIds"})
            self.assertEqual((summary["membershipId"], summary["connection"], summary["lastErrorCode"], summary["holeSeqs"]),
                             ("member-1", "reconnecting", "network", [5]))

    @time_limit(30)
    def test_p2c_4_m5_mute_outage_and_ledger_values_stay_per_membership(self):
        with tempfile.TemporaryDirectory() as directory:
            gateway = Gateway(directory)
            aura, claw = binding("member-aura"), binding("member-claw")
            gateway.update(aura, connected=True, registered=True)
            gateway.update(claw, connected=True, registered=True)
            # Aura is muted (P2c-3b) and has a ledger hole (P3-3).
            gateway.update(aura, connection="inactive", inactive=True, error_code="connector_not_active")
            gateway.update(aura, ledger={"acknowledgedSeq": 7, "holeSeqs": [9], "quarantinedSeqs": [11],
                                         "ledgerCode": "ack_hole"})
            # Claw has an outage and is re-registered (P2c-3a).
            gateway.update(claw, connection="reconnecting", error_code="network")
            time.sleep(0.15)
            gateway.update(claw, connected=True, registered=True)
            # Claw's heartbeat never ends Aura's mute.
            gateway.update(claw, connected=True)
            written = gateway.read()["memberships"]
            aura_entry, claw_entry = written["member-aura"], written["member-claw"]
            self.assertEqual(aura_entry["connection"], "inactive")
            self.assertEqual(claw_entry["connection"], "connected")
            self.assertEqual((aura_entry["acknowledgedSeq"], aura_entry["holeSeqs"], aura_entry["quarantinedSeqs"],
                              aura_entry["ledgerCode"]), (7, [9], [11], "ack_hole"))
            self.assertEqual((claw_entry["acknowledgedSeq"], claw_entry["holeSeqs"], claw_entry["quarantinedSeqs"],
                              claw_entry["ledgerCode"]), (0, [], [], ""))
            self.assertIsNotNone(claw_entry["lastReconnectedAt"])
            self.assertGreaterEqual(claw_entry["lastOutageSeconds"], 0.1)
            self.assertEqual((aura_entry["lastReconnectedAt"], aura_entry["lastOutageSeconds"]), (None, 0))
            # Aura's unmute ends only Aura's mark.
            gateway.update(aura, inactive=False, connection="reconnecting", error_code="")
            gateway.update(aura, connected=True, registered=True)
            self.assertEqual(gateway.read()["memberships"]["member-aura"]["connection"], "connected")

    @time_limit(30)
    def test_p2c_4_m6_a_room_only_value_is_attributed_to_the_room_s_only_membership_and_to_none_of_several(self):
        with tempfile.TemporaryDirectory() as directory:
            gateway = Gateway(directory)
            gateway.update(binding("member-1"), connected=True, registered=True)
            # The host session gate knows the Room only (ISSUE-023).
            gateway.instance._session_wait_started = {"room-1": time.time() - 2.5}
            gateway.instance._end_session_wait("room-1")
            entry = gateway.read()["memberships"]["member-1"]
            self.assertGreaterEqual(entry["lastGateWaitSeconds"], 2.4)
            self.assertEqual(entry["lastGateWaitReason"], "host_run_busy")
        with tempfile.TemporaryDirectory() as directory:
            gateway = Gateway(directory)
            gateway.update(binding("member-aura"), connected=True, registered=True)
            gateway.update(binding("member-claw"), connected=True, registered=True)
            before = gateway.read()
            gateway.instance._session_wait_started = {"room-1": time.time() - 2.5}
            gateway.instance._end_session_wait("room-1")
            gateway.instance._status_room(None, "room-1", connection="stopped", error_code="network")
            after = gateway.read()
            self.assertEqual(after["memberships"], before["memberships"])
            self.assertEqual(after["rooms"], before["rooms"])


class SingleAccountReadmissionTests(unittest.TestCase):
    """Hermes keeps one binding per Room (state.py ``upsert``); a re-admission replaces its membership in place
    (``_adopt_binding``). The status then holds the new membership alone, with the Room entry carried over, as
    the single Room entry did before 1.0.108."""

    @time_limit(30)
    def test_a_readmitted_room_keeps_one_membership_entry(self):
        with tempfile.TemporaryDirectory() as directory:
            gateway = Gateway(directory)
            live = binding("member-old")
            gateway.instance._state = adapter.PluginState(bindings=[live])
            gateway.update(live, connected=True, registered=True,
                           ledger={"acknowledgedSeq": 4, "holeSeqs": [], "quarantinedSeqs": [], "ledgerCode": ""})
            adopted = gateway.instance._adopt_binding(binding("member-new"))
            self.assertIs(adopted, live)
            gateway.update(live, connected=True)
            written = gateway.read()
            self.assertEqual(list(written["memberships"]), ["member-new"])
            summary = written["rooms"]["room-1"]
            self.assertEqual((summary["membershipId"], summary["membershipIds"], summary["connection"],
                              summary["acknowledgedSeq"]), ("member-new", ["member-new"], "connected", 4))


if __name__ == "__main__":
    unittest.main()
