"""Owner 2026-11-12, "re-invite a removed agent": a connector replaces a stale binding automatically.

The Room server reactivates the existing membership with fresh credentials. When the claimed enrollment names a
Room this profile already holds, the connector adopts it only when the stored binding is stale (revoked or disabled)
and inert (no delivery intent, lifecycle, quarantined inbox entry, recovery closure or credential rotation). The
fresh credential is adopted into the one live binding object of that Room (contract P16) and exactly one listener
starts. An active binding is never replaced, and a stale binding that holds evidence stays untouched.
"""

import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from test_delivery_lifecycle import adapter, state_store


def _source():
    return adapter.RoomBinding(
        "https://room.example/api", "room-1", "member-1", "credential-1",
        display_name="Real", identity_version=2, installation_id="installation-1", cursor=5, acknowledged_cursor=5,
    )


CLAIM = {
    "invitationId": "invite-9", "roomId": "room-2", "roomTitle": "Digital sovereignty", "displayName": "Real",
    "membershipId": "member-2", "credential": "credential-fresh", "credentialExpiresAt": "2026-12-12T00:00:00Z",
    "identityVersion": 2, "headSeq": 40,
}


class StaleBindingAdoptionTests(unittest.IsolatedAsyncioTestCase):
    def stored(self, **fields):
        values = dict(display_name="Real", identity_version=2, installation_id="installation-1",
                      cursor=12, acknowledged_cursor=12)
        values.update(fields)
        return adapter.RoomBinding("https://room.example/api", "room-2", "member-2", "credential-old", **values)

    async def claim(self, existing, *, running_listener=False):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "state.json"
        source = _source()
        state_store.save(state_store.PluginState(bindings=[source, existing]), path)
        instance = object.__new__(adapter.SyntheticSocialityAdapter)
        instance._state = state_store.load(path)
        live_before = instance._state.binding("room-2")
        instance._tasks, instance._heartbeat_tasks = {}, {}
        instance._stop = asyncio.Event()
        instance._enrollment_lock = asyncio.Lock()
        instance._state_file = path
        watched, cancelled = [], []
        if running_listener:
            async def old_listener():
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.append("room-2")
            instance._tasks["room-2"] = asyncio.create_task(old_listener())
            await asyncio.sleep(0)

        async def call(_binding, operation, **_request_options):
            class Protocol:
                def connector_enrollments(self, _installation_id):
                    return [{"invitationId": "invite-9"}]

                def claim_connector_enrollment(self, _invitation_id, _installation_id):
                    return dict(CLAIM)
            return operation(Protocol())

        async def watch(binding, *, is_reconnect):
            watched.append((binding, is_reconnect, state_store.load(path).binding(binding.room_id).credential))

        instance._call = call
        instance._watch = watch
        original_update = adapter.update
        adapter.update = lambda mutator: state_store.update(mutator, path)
        source_before = json.loads(path.read_text())["bindings"][0]
        before = path.read_bytes()
        try:
            await asyncio.wait_for(instance._accept_connector_enrollments(source), timeout=5)
            # Bounded: a listener left running (the stale one) must not hang the test.
            pending = [task for task in instance._tasks.values() if not task.done()]
            if pending:
                await asyncio.wait(pending, timeout=0.5)
            leftover = [task for task in instance._tasks.values() if not task.done()]
            for task in leftover:
                task.cancel()
            await asyncio.gather(*leftover, return_exceptions=True)
        finally:
            adapter.update = original_update
        self.assertEqual(json.loads(path.read_text())["bindings"][0], source_before, "the sibling binding is untouched")
        return instance, path, live_before, watched, cancelled, before

    async def test_a_revoked_binding_of_a_reactivated_membership_is_replaced(self):
        existing = self.stored(enabled=False, revoked=True, disabled_reason="credential_revoked")
        instance, path, live_before, watched, _cancelled, _before = await self.claim(existing)
        persisted = state_store.load(path).binding("room-2")
        self.assertEqual((persisted.credential, persisted.membership_id), ("credential-fresh", "member-2"))
        self.assertTrue(persisted.enabled)
        self.assertFalse(persisted.revoked)
        self.assertEqual(persisted.disabled_reason, "")
        self.assertEqual((persisted.cursor, persisted.acknowledged_cursor), (40, 40))
        self.assertEqual(persisted.installation_id, "installation-1")
        self.assertIs(instance._state.binding("room-2"), live_before, "adopted into the one live binding object (P16)")
        self.assertEqual(len([b for b in instance._state.bindings if b.room_id == "room-2"]), 1)
        self.assertEqual(len(watched), 1, "exactly one listener")
        self.assertIs(watched[0][0], live_before)
        self.assertEqual(watched[0][2], "credential-fresh", "the credential is persisted before the listener starts")

    async def test_an_expired_disabled_binding_is_replaced(self):
        existing = self.stored(enabled=False, disabled_reason="credential_expired")
        _instance, path, _live, watched, _cancelled, _before = await self.claim(existing)
        persisted = state_store.load(path).binding("room-2")
        self.assertEqual(persisted.credential, "credential-fresh")
        self.assertTrue(persisted.enabled)
        self.assertEqual(len(watched), 1)

    async def test_a_stale_binding_with_a_leftover_listener_keeps_exactly_one_listener(self):
        existing = self.stored(enabled=False, revoked=True, disabled_reason="credential_revoked")
        _instance, path, _live, watched, cancelled, _before = await self.claim(existing, running_listener=True)
        self.assertEqual(cancelled, ["room-2"], "the old listener is stopped and awaited")
        self.assertEqual(len(watched), 1)
        self.assertEqual(state_store.load(path).binding("room-2").credential, "credential-fresh")

    async def test_an_active_binding_is_never_replaced(self):
        existing = self.stored()
        instance, path, live_before, watched, _cancelled, before = await self.claim(existing)
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(instance._state.binding("room-2").credential, "credential-old")
        self.assertIs(instance._state.binding("room-2"), live_before)
        self.assertEqual(watched, [])

    async def test_a_stale_binding_that_holds_evidence_is_left_untouched(self):
        evidence = {
            "quarantined inbox": dict(inbox={"9": "quarantined"}),
            "delivery intent": dict(delivery_intents={"evt-9": {"state": "prepared", "delivery_state": "not_started",
                                                                 "lifecycle_state": "not_started"}}),
            "recovery closure": dict(recovery_closures={"9": {"recordDigest": "sha256:aa", "appliedAt": "2026-11-01T00:00:00Z"}}),
        }
        for label, fields in evidence.items():
            with self.subTest(evidence=label):
                existing = self.stored(enabled=False, revoked=True, disabled_reason="credential_revoked", **fields)
                instance, path, _live, watched, _cancelled, before = await self.claim(existing)
                self.assertEqual(path.read_bytes(), before, "cursor, journal and quarantine stay as stored")
                self.assertEqual(instance._state.binding("room-2").credential, "credential-old")
                self.assertEqual(watched, [])


class StaleBindingAdoptionRaceTests(StaleBindingAdoptionTests):
    async def test_a_binding_that_became_active_in_the_state_file_is_kept(self):
        """The in-memory copy looks stale, the stored binding is active: the state transaction keeps it."""
        existing = self.stored()
        original = adapter.SyntheticSocialityAdapter._accept_connector_enrollments

        async def stale_in_memory(instance, source):
            live = instance._state.binding("room-2")
            live.enabled, live.revoked = False, True
            return await original(instance, source)

        adapter.SyntheticSocialityAdapter._accept_connector_enrollments = stale_in_memory
        try:
            _instance, path, _live, watched, _cancelled, before = await self.claim(existing)
        finally:
            adapter.SyntheticSocialityAdapter._accept_connector_enrollments = original
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(watched, [])


if __name__ == "__main__":
    unittest.main()
