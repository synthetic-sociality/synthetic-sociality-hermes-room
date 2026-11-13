"""P18 (Hermes 1.0.105, owner decision 2026-11-13): a revoked binding's pending work is abandoned on re-admission.

The fixture is the live Real binding of Room duev8o1h936bl9f as persisted by Hermes Room 1.0.104 (only the credential
and the connector session id are redacted): revoked, credential_revoked, a blocked cycle_conflict lifecycle entry,
inbox 2646 pending with pending_since and pending_retries. It goes through the real state save/load and the real
enrollment path; the Room protocol is a fake (no live traffic).
"""
import asyncio
import copy
import json
import tempfile
import unittest
from pathlib import Path

from test_delivery_lifecycle import adapter, state_store

REAL = json.loads((Path(__file__).parent / "fixtures" / "real-duev-binding-1.0.104-redacted.json").read_text())
ROOM = REAL["room_id"]
JOURNAL = "revocation-abandonment.jsonl"
FRESH = {
    "invitationId": "invite-readmit-real", "roomId": ROOM, "roomTitle": "Digital sovereignty", "displayName": "Real",
    "membershipId": REAL["membership_id"], "credential": "credential-fresh", "credentialExpiresAt": "2026-12-13T00:00:00Z",
    "identityVersion": REAL["identity_version"], "headSeq": 2700,
}


def source_binding():
    return adapter.RoomBinding(
        REAL["base_url"], "room-sibling", "member-sibling", "credential-sibling", display_name="Real",
        identity_version=1, installation_id=REAL["installation_id"], cursor=10, acknowledged_cursor=10,
    )


class RevokedAbandonmentTests(unittest.IsolatedAsyncioTestCase):
    async def run_claim(self, real, *, listing=None, before_update=None):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "state.json"
        # Real save/load of the persisted 1.0.104 binding.
        state_store.save(state_store.PluginState(bindings=[source_binding(), state_store.RoomBinding.from_dict(real)]), path)
        instance = object.__new__(adapter.SyntheticSocialityAdapter)
        instance._state = state_store.load(path)
        live = instance._state.binding(ROOM)
        instance._tasks, instance._heartbeat_tasks = {}, {}
        instance._stop = asyncio.Event()
        instance._enrollment_lock = asyncio.Lock()
        instance._state_file = path
        claims, watched = [], []
        listed = [{"invitationId": FRESH["invitationId"], "roomId": ROOM}] if listing is None else listing

        async def call(_binding, operation, **_request_options):
            class Protocol:
                def connector_enrollments(self, _installation_id):
                    return [dict(item) for item in listed]

                def claim_connector_enrollment(self, invitation_id, _installation_id):
                    claims.append(invitation_id)
                    return dict(FRESH)
            return operation(Protocol())

        async def watch(binding, *, is_reconnect):
            watched.append((binding, state_store.load(path).binding(binding.room_id).credential))

        instance._call = call
        instance._watch = watch
        original_update = adapter.update

        def update(mutator):
            if before_update is not None:
                before_update(path)
            return state_store.update(mutator, path)

        adapter.update = update
        before = path.read_bytes()
        try:
            await asyncio.wait_for(instance._accept_connector_enrollments(source_binding()), timeout=5)
            for task in list(instance._tasks.values()):
                if not task.done():
                    await asyncio.wait([task], timeout=0.5)
        finally:
            adapter.update = original_update
            for task in instance._tasks.values():
                task.cancel()
        return instance, path, live, claims, watched, before

    def journal(self, path, phase="recorded"):
        target = path.parent / JOURNAL
        lines = [json.loads(line) for line in target.read_text().splitlines()] if target.exists() else []
        return lines if phase is None else [line for line in lines if line["phase"] == phase]

    def assert_untouched(self, path, before, claims, watched):
        self.assertEqual(claims, [], "the enrollment must not be claimed")
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(self.journal(path), [])
        self.assertEqual(watched, [])

    async def test_p18_01_the_live_real_binding_is_replaced_by_the_fresh_credential(self):
        instance, path, live, claims, watched, _before = await self.run_claim(REAL)
        self.assertEqual(claims, [FRESH["invitationId"]], "1.0.104 leaves this enrollment unclaimed")
        stored = state_store.load(path).binding(ROOM)
        self.assertEqual((stored.credential, stored.membership_id), ("credential-fresh", REAL["membership_id"]))
        self.assertTrue(stored.enabled)
        self.assertFalse(stored.revoked)
        self.assertEqual((stored.cursor, stored.acknowledged_cursor), (2700, 2700))
        self.assertEqual((stored.inbox, stored.pending_since, stored.delivery_lifecycle), ({}, {}, {}))
        self.assertIs(instance._state.binding(ROOM), live, "one live binding object (P16)")
        self.assertEqual(len(watched), 1, "exactly one listener")
        self.assertEqual(watched[0][1], "credential-fresh")
        self.assertEqual(state_store.load(path).binding("room-sibling").credential, "credential-sibling")

    async def test_p18_02_the_journal_records_every_abandoned_entry_and_survives_replacement_and_reload(self):
        _instance, path, _live, _claims, _watched, _before = await self.run_claim(REAL)
        entries = self.journal(path)
        kinds = sorted((entry["entry"]["kind"], entry["entry"]["id"]) for entry in entries)
        self.assertEqual(kinds, [("delivery_lifecycle", "joss0s11rj0vhre"), ("inbox", "2646")])
        for entry in entries:
            self.assertEqual(entry["reason"], "abandoned_on_revocation")
            self.assertEqual((entry["roomId"], entry["membershipId"], entry["installationId"]),
                             (ROOM, REAL["membership_id"], REAL["installation_id"]))
            self.assertEqual(entry["revocation"], {"revoked": True, "disabledReason": "credential_revoked"})
            self.assertTrue(entry["abandonedAt"].endswith("Z"))
            self.assertRegex(entry["auditId"], r"^[0-9a-f]{64}$")
            self.assertNotIn("credential-old-redacted", json.dumps(entry))
            self.assertNotIn("session-old-redacted", json.dumps(entry))
        by_kind = {entry["entry"]["kind"]: entry["entry"] for entry in entries}
        self.assertEqual(by_kind["delivery_lifecycle"]["record"], REAL["delivery_lifecycle"]["joss0s11rj0vhre"])
        self.assertEqual(by_kind["inbox"]["record"], {"state": "pending", "pendingSince": REAL["pending_since"]["2646"],
                                                      "pendingRetries": REAL["pending_retries"]["2646"]})
        self.assertEqual(sorted(line["auditId"] for line in self.journal(path, "applied")),
                         sorted(entry["auditId"] for entry in entries), "every recorded entry was applied")
        self.assertEqual(oct((path.parent / JOURNAL).stat().st_mode & 0o777), "0o600")
        # Survives the replacement and any later load/save of the state.
        state_store.save(state_store.load(path), path)
        self.assertEqual(self.journal(path), entries)
        self.assertEqual(len(self.journal(path, None)), 4)

    async def test_p18_03_an_active_binding_with_pending_work_is_untouched_and_unclaimed(self):
        active = copy.deepcopy(REAL)
        active.update(enabled=True, revoked=False, disabled_reason="")
        _instance, path, _live, claims, watched, before = await self.run_claim(active)
        self.assert_untouched(path, before, claims, watched)

    async def test_p18_04_an_operator_disabled_binding_is_untouched_and_unclaimed(self):
        for revoked in (False, True):
            with self.subTest(revoked=revoked):
                disabled = copy.deepcopy(REAL)
                disabled.update(enabled=False, revoked=revoked, disabled_reason="manual")
                _instance, path, _live, claims, watched, before = await self.run_claim(disabled)
                self.assert_untouched(path, before, claims, watched)

    async def test_p18_05_a_replay_appends_no_duplicate_audit_entry(self):
        _instance, path, _live, _claims, _watched, _before = await self.run_claim(REAL)
        entries = self.journal(path)
        # A crash after the journal append and before the state transaction: the same work is seen again.
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        again = Path(directory.name) / "state.json"
        state_store.save(state_store.PluginState(bindings=[source_binding(), state_store.RoomBinding.from_dict(REAL)]), again)
        # Only the recorded entries reached the journal before the crash.
        (again.parent / JOURNAL).write_text("".join(json.dumps(entry, sort_keys=True) + "\n" for entry in entries))
        instance = object.__new__(adapter.SyntheticSocialityAdapter)
        instance._state = state_store.load(again)
        instance._state_file = again
        original_update = adapter.update
        adapter.update = lambda mutator: state_store.update(mutator, again)
        try:
            self.assertTrue(instance._abandon_revoked_pending_work(instance._state.binding(ROOM)))
            # The work is gone now: a further call records nothing.
            self.assertFalse(instance._abandon_revoked_pending_work(instance._state.binding(ROOM)))
        finally:
            adapter.update = original_update
        self.assertEqual(self.journal(again), entries, "no duplicate recorded entry")
        self.assertEqual(len(self.journal(again, "applied")), len(entries))
        self.assertEqual(state_store.load(again).binding(ROOM).inbox, {})

    async def test_p18_06_a_binding_changed_before_the_transaction_is_kept_and_unclaimed(self):
        def reactivate(path):
            data = json.loads(path.read_text())
            for binding in data["bindings"]:
                if binding["room_id"] == ROOM:
                    binding["inbox"]["2647"] = "pending"
                    binding["pending_since"]["2647"] = 1794464300.0
                    binding["pending_retries"]["2647"] = 0
            path.write_text(json.dumps(data))
            path.chmod(0o600)

        calls = {"n": 0}

        def first_update_only(path):
            calls["n"] += 1
            if calls["n"] == 1:
                reactivate(path)

        _instance, path, _live, claims, watched, _before = await self.run_claim(REAL, before_update=first_update_only)
        self.assertEqual(claims, [])
        self.assertEqual(watched, [])
        stored = state_store.load(path).binding(ROOM)
        self.assertEqual(stored.credential, "credential-old-redacted")
        self.assertEqual(sorted(stored.inbox), ["2646", "2647"], "nothing removed from a binding that changed")
        self.assertEqual(len(self.journal(path, "not_applied")), 2)
        self.assertEqual(self.journal(path, "applied"), [])

    async def test_p18_07_a_revoked_binding_with_an_intent_or_quarantine_is_not_abandoned(self):
        intent = {"state": "prepared", "delivery_state": "not_started", "lifecycle_state": "not_started"}
        for change in ({"delivery_intents": {"evt-9": intent}}, {"inbox": {"2646": "pending", "2640": "quarantined"}}):
            with self.subTest(change=sorted(change)):
                held = copy.deepcopy(REAL)
                held.update(copy.deepcopy(change))
                _instance, path, _live, claims, watched, before = await self.run_claim(held)
                self.assert_untouched(path, before, claims, watched)


if __name__ == "__main__":
    unittest.main()


class RevokedAbandonmentRemedyTests(RevokedAbandonmentTests):
    """OP-12 105 R1 remedies: H105-F001 (abandonment eligibility) and H105-F004 (journal open/write safety)."""

    async def test_h105_f001_an_enabled_binding_whose_credential_merely_expired_is_not_abandoned(self):
        renewable = copy.deepcopy(REAL)
        renewable.update(enabled=True, revoked=False, disabled_reason="", credential_expires_at="2026-01-01T00:00:00Z")
        _instance, path, _live, claims, watched, before = await self.run_claim(renewable)
        self.assert_untouched(path, before, claims, watched)

    async def test_h105_f001_a_stopped_binding_with_credential_expired_reason_is_abandoned(self):
        stopped = copy.deepcopy(REAL)
        stopped.update(enabled=False, revoked=False, disabled_reason="credential_expired")
        _instance, path, _live, claims, watched, _before = await self.run_claim(stopped)
        self.assertEqual(claims, [FRESH["invitationId"]])
        self.assertEqual(state_store.load(path).binding(ROOM).credential, "credential-fresh")
        self.assertEqual(len(self.journal(path)), 2)
        self.assertEqual(len(watched), 1)

    async def test_h105_f001_operator_disabled_with_revoked_or_expired_flags_is_untouched(self):
        for flags in (dict(revoked=True), dict(credential_expires_at="2026-01-01T00:00:00Z"),
                      dict(revoked=True, credential_expires_at="2026-01-01T00:00:00Z")):
            with self.subTest(flags=sorted(flags)):
                disabled = copy.deepcopy(REAL)
                disabled.update({"enabled": False, "disabled_reason": "manual", "revoked": False, **flags})
                _instance, path, _live, claims, watched, before = await self.run_claim(disabled)
                self.assert_untouched(path, before, claims, watched)

    async def test_h105_f004_a_symlinked_journal_fails_closed_without_claim(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        target = Path(directory.name) / "elsewhere.jsonl"
        target.write_text("")
        original_update = adapter.update
        # Plant the symlink where the journal belongs, then abandon.
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        state = Path(tmp.name) / "state.json"
        state_store.save(state_store.PluginState(bindings=[source_binding(), state_store.RoomBinding.from_dict(REAL)]), state)
        (state.parent / JOURNAL).symlink_to(target)
        before = state.read_bytes()
        instance = object.__new__(adapter.SyntheticSocialityAdapter)
        instance._state = state_store.load(state)
        instance._state_file = state
        adapter.update = lambda mutator: state_store.update(mutator, state)
        try:
            with self.assertRaises(OSError):
                instance._abandon_revoked_pending_work(instance._state.binding(ROOM))
        finally:
            adapter.update = original_update
        self.assertEqual(state.read_bytes(), before, "state unchanged when the journal is refused")
        self.assertEqual(target.read_text(), "", "nothing written through the symlink")

    async def test_h105_f004_short_writes_are_completed_and_a_zero_write_fails_closed(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        journal = Path(directory.name) / JOURNAL
        records = [{"phase": "recorded", "auditId": "a" * 64, "pad": "x" * 300}]
        real_write = adapter.os.write
        calls = []

        def short(fd, data):
            calls.append(len(data))
            return real_write(fd, bytes(data)[:7])

        original = adapter.os.write
        adapter.os.write = short
        try:
            adapter._append_journal(journal, records)
        finally:
            adapter.os.write = original
        self.assertGreater(len(calls), 1, "a short write is continued, never accepted")
        self.assertEqual([json.loads(line) for line in journal.read_text().splitlines()], records)
        adapter.os.write = lambda fd, data: 0
        try:
            with self.assertRaises(OSError):
                adapter._append_journal(Path(directory.name) / "second.jsonl", records)
        finally:
            adapter.os.write = original
