"""P2c-3k persisted Berlin recovery. Fixture produced by 1.0.113 (a6d7f2a)
cli._renew with synthetic values, a gen-3 owner claim returning "issued", and
Room ACK 3222. The historical preflight failed *after* the writer durably
recorded the grant. In particular its protected snapshot lacks renewal_closed.
"""
import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from test_ack_ahead import (
    ROOM, ROOT, AheadRoom, Harness, INTENT, adapter, live, maintenance_instance,
    renewal_api, room_max, state_store,
)


class RenewStaleTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.path = Path(self.scratch.name) / "state.json"
        self.path.write_bytes((ROOT / "tests/fixtures/berlin-ds-1.0.113-generated.json").read_bytes())
        self.path.chmod(0o600)
        self.instance = maintenance_instance(self.path)
        self.instance._status_room = mock.Mock()
        self.room = {"ack": 3222, "calls": []}
        base = renewal_api(self.room)
        self.api = type("FixtureRoom", (base,), {})
        self.status, self.code, self.message, self.retryable = 401, "unauthorized", "credential renewal grant expired", False
        self.error_stage = "request"
        self.generation = 3
        test = self

        def request(api, room, request_id, digest, **options):
            if test.error_stage == "request":
                raise test.error()
            return base.request_credential_renewal(api, room, request_id, digest, **options)

        def status(api, room, request_id):
            return {**base.credential_renewal_request(api, room, request_id), "baseCredentialGeneration": test.generation, "grantId": "masked-grant"}

        def redeem(api, *args):
            if test.error_stage == "redeem":
                raise test.error()
            return base.redeem_credential_renewal(api, *args)

        def confirm(api, *args):
            event = base.confirm_credential_renewal(api, *args)
            test.generation += 1
            return event

        def state(api, room):
            if test.generation == 4 and api.credential != test.room["replacement"]:
                raise live("protocol").ProtocolError("base credential was revoked", status=401, code="credential_revoked")
            return base.room_state(api, room)

        # The persisted trace predates February 2027; keep it reproducible
        # when the suite runs beyond the fixture's historical expiry date.
        passed = adapter.renewal._passed
        clock = mock.patch.object(adapter.renewal, "_passed", lambda raw, now: passed(raw, 1795597200))
        clock.start()
        self.addCleanup(clock.stop)
        self.api.request_credential_renewal = request
        self.api.credential_renewal_request = status
        self.api.redeem_credential_renewal = redeem
        self.api.confirm_credential_renewal = confirm
        self.api.room_state = state
        for obj, field, value in [
            (live("cli"), "RoomProtocol", self.api),
            (adapter, "ProtocolError", live("cli").ProtocolError),
            (adapter.renewal, "load", live("state").load),
            (adapter.renewal, "state_path_scope", live("state").state_path_scope),
        ]:
            patch = mock.patch.object(obj, field, value)
            patch.start()
            self.addCleanup(patch.stop)

    def error(self):
        return live("cli").ProtocolError(self.message, status=self.status, code=self.code, retryable=self.retryable)

    def binding(self):
        return state_store.load(self.path).binding(ROOM)

    async def maintain(self):
        await self.instance._maintain_renewal(self.binding())
        await asyncio.sleep(0)

    async def test_renew_stale_01_grant_expired(self):
        initial = self.binding()
        with self.assertLogs(adapter.logger, "WARNING") as logs:
            await self.maintain()
        saved = self.binding()
        self.assertEqual(saved.credential_rotation, {})
        self.assertEqual(saved.renewal_closed["reason"], "grant_expired")
        self.assertEqual(saved.credential, initial.credential)
        self.assertEqual(saved.membership_id, initial.membership_id)
        self.assertEqual(saved.terminal_evidence, initial.terminal_evidence)
        self.assertEqual(saved.acknowledged_cursor, 3220)
        self.instance._watch.assert_awaited_once()
        self.instance._status_room.assert_called_with(mock.ANY, renewal_code="renewal_grant_expired", error_code="renewal_grant_expired")
        self.assertEqual(len(logs.output), 1)
        # Normal ACK recovery after the watcher restarts advances the frontier
        # without dispatching a turn for previously acknowledged evidence.
        harness = Harness(saved, AheadRoom(room_max(3224)), self.path.parent, persist=True)
        self.addCleanup(harness.scratch.cleanup)
        harness.save()
        with state_store.state_path_scope(harness.state_file):
            await harness.instance._complete_event(saved, 3221)
            await harness.instance._consume_locked(saved, {"id": "ev3221", "seq": 3221})
        self.assertEqual(harness.reload().acknowledged_cursor, 3224)
        self.assertEqual(harness.reload().delivery_intents, {})
        self.assertEqual(harness.api.acks, [3221])

    async def test_renew_stale_02_fresh_intent(self):
        await self.maintain()
        # Simulate restart from the durable closure before the next discovery.
        self.instance = maintenance_instance(self.path)
        self.error_stage = None
        new = {**INTENT, "baseCredentialGeneration": 3, "requestId": live("cli")._new_renewal_secret()}
        with mock.patch.object(adapter.renewal, "discover", return_value=new) as discover:
            await self.maintain()
        discover.assert_called_once()
        self.assertEqual(self.generation, 4)
        saved = self.binding()
        self.assertEqual(saved.renewal_closed, {})
        self.assertEqual(saved.membership_id, new["membershipId"])
        self.assertEqual(saved.credential, self.room["replacement"])
        self.api("", saved.credential).room_state(ROOM)
        with self.assertRaises(live("protocol").ProtocolError) as revoked:
            self.api("", "masked-base").room_state(ROOM)
        self.assertTrue(revoked.exception.revoked)

    async def test_renew_stale_03_rejected_or_not_found(self):
        for status, code, msg, reason, stage in [
            (409, "idempotency_mismatch", "credential renewal request was rejected", "request_rejected", "request"),
            (404, "not_found", "room or object not found", "request_not_found", "redeem"),
        ]:
            with self.subTest(status=status):
                self.path.write_bytes((ROOT / "tests/fixtures/berlin-ds-1.0.113-generated.json").read_bytes())
                self.status, self.code, self.message, self.error_stage = status, code, msg, stage
                await self.maintain()
                self.assertEqual(self.binding().renewal_closed["reason"], reason)

    async def test_renew_stale_04_retryable_failure(self):
        self.status, self.code, self.message, self.retryable = 503, "internal_error", "SENSITIVE-CANARY", True
        original = self.binding().credential_rotation
        with self.assertLogs(adapter.logger, "WARNING") as logs:
            await self.maintain()
            await self.maintain()
        self.assertEqual(self.binding().credential_rotation, original)
        self.assertEqual(len(logs.output), 1)
        self.assertNotIn("SENSITIVE-CANARY", logs.output[0])
        self.instance._watch.assert_not_awaited()
        self.instance._status_room.assert_called_with(mock.ANY, renewal_code="renewal_http_503", error_code="renewal_http_503")

    async def test_other_conflict_or_revocation_does_not_discard_the_journal(self):
        for status, code, msg in [
            (409, "idempotency_mismatch", "credential renewal payload mismatch"),
            (401, "credential_revoked", "base credential was revoked"),
        ]:
            with self.subTest(code=code):
                self.instance._renewal_retry_after.clear()
                self.status, self.code, self.message = status, code, msg
                original = self.binding().credential_rotation
                await self.maintain()
                self.assertEqual(self.binding().credential_rotation, original)
                self.instance._watch.assert_not_awaited()

    async def test_expired_base_never_starts_after_closure(self):
        b = self.binding()
        b.credential_expires_at = "2026-01-01T00:00:00Z"
        b.credential_rotation["base_credential_expires_at"] = b.credential_expires_at
        state_store.save(state_store.PluginState(bindings=[b]), self.path)
        await self.maintain()
        self.assertEqual(self.binding().renewal_closed["reason"], "grant_expired")
        self.instance._watch.assert_not_awaited()
        self.assertFalse(adapter.renewal.can_start(self.binding()))

    async def test_changed_protected_field_keeps_the_original_journal(self):
        b = self.binding()
        b.display_name = "different"
        state_store.save(state_store.PluginState(bindings=[b]), self.path)
        await self.maintain()
        self.assertEqual(self.binding().credential_rotation, b.credential_rotation)
        self.instance._watch.assert_not_awaited()
