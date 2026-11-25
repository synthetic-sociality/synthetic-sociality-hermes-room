"""P2c-3k persisted Berlin recovery. Fixture produced by 1.0.113 (a6d7f2a)
cli._renew with synthetic values, a gen-3 owner claim returning "issued", and
Room ACK 3222. The historical preflight failed *after* the writer durably
recorded the grant. In particular its protected snapshot lacks renewal_closed.
"""
import asyncio
import copy
from dataclasses import asdict
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from test_delivery_lifecycle import configured_instance
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

    async def test_watcher_resumes_after_a_concurrent_control_refusal_lifts_its_fence(self):
        released = False

        def fenced():
            return not released and not self.binding().credential_rotation

        async def lift():
            nonlocal released
            self.instance._watch.assert_not_awaited()
            released = True

        self.instance._control_fenced = fenced
        self.instance._control_fence_lifted = lift
        await self.maintain()
        self.assertTrue(released)
        self.instance._watch.assert_awaited_once()

    async def test_parked_digital_sovereignty_recovers_its_frozen_post_with_networks_online(self):
        for already_committed in (False, True):
            with self.subTest(reply_lost_after_commit=already_committed):
                await self._exercise_pending_post(already_committed)

    async def _exercise_pending_post(self, already_committed):
        self.path.write_bytes((ROOT / "tests/fixtures/berlin-ds-1.0.113-generated.json").read_bytes())
        ds = self.binding()
        networks = state_store.RoomBinding("https://room.example/api", "networks", "networks-member",
                                           "synthetic-networks-base", installation_id="inst-1",
                                           connector_session_id="networks-online")
        original_networks = asdict(networks)
        fake_events, posted_requests = {}, []
        unavailable = True
        class DeliveryRoom(live("protocol").RoomProtocol):
            def room_state(api, room_id):
                return {"headSeq": 3224, "activeEpoch": {"id": "epoch-1"}}

            def room_policy(api, room_id):
                return {"coordinationMode": "open"}

            def request(api, method, path, payload=None):
                self.assertEqual((method, path), ("POST", f"/rooms/{ROOM}/messages"))
                frozen = copy.deepcopy(payload)
                posted_requests.append(frozen)
                key = payload["idempotencyKey"]
                if unavailable:
                    if already_committed:
                        fake_events[key] = frozen
                    raise adapter.ProtocolError("lost reply", retryable=True)
                if key in fake_events:
                    self.assertEqual(fake_events[key], frozen)
                fake_events[key] = frozen
                return {"id":"canonical-ds-3225", "seq":3225, "ts":"2026-11-25T09:00:00Z"}

        room = DeliveryRoom(ds.base_url)
        def sender(binding):
            runtime = configured_instance(binding, None, [])
            runtime._event_seq[ROOM]["evt-5"] = 3224
            runtime._cycle_attempts = {}
            runtime._call = lambda _, operation, **kwargs: asyncio.sleep(0, result=operation(room))
            runtime._publish = mock.AsyncMock()
            return runtime

        source = adapter._dispatch_source_ref("evt-5", "synthetic-generation")
        first = await sender(ds)._send_final(ROOM, source, "Frozen DS reply")
        self.assertFalse(first.success)
        self.assertEqual(ds.delivery_intents["evt-5"]["delivery_state"], "delivery_pending")
        self.assertFalse(adapter.renewal.quiescent(ds, for_renewal=True))
        # An older selected intent in the protection snapshot may already
        # carry newer receipt/retry evidence by the time the journal closes.
        if already_committed:
            earlier = copy.deepcopy(ds.delivery_intents["evt-5"])
            earlier["state"] = earlier["delivery_state"] = "selected"
            ds.credential_rotation["protected_snapshot"]["delivery_intents"]["evt-5"] = earlier
        state_store.save(state_store.PluginState(bindings=[ds, networks]), self.path)
        before = asdict(self.binding())

        # Both rooms share one adapter after restart. Only DS is parked.
        self.instance = maintenance_instance(self.path)
        online = asyncio.create_task(asyncio.Event().wait())
        self.addAsyncCleanup(self._cancel_task, online)
        self.instance._tasks[networks.room_id] = online
        self.instance._state = state_store.load(self.path)
        def get_request(api, room_id, request_id):
            self.assertEqual((room_id, request_id), (ROOM, ds.credential_rotation["request_id"]))
            return {"roomId":ROOM, "membershipId":ds.membership_id, "requestId":request_id, "state":"expired"}
        with mock.patch.object(self.api, "credential_renewal_request", get_request), \
                mock.patch.object(self.api, "request_credential_renewal", side_effect=AssertionError("no claim")), \
                mock.patch.object(self.api, "redeem_credential_renewal", side_effect=AssertionError("no exchange")):
            await self.maintain()
        recovered = self.binding()
        self.assertFalse(recovered.credential_rotation)
        self.assertEqual({k:v for k,v in asdict(recovered).items() if k not in {"credential_rotation","renewal_closed"}},
                         {k:v for k,v in before.items() if k not in {"credential_rotation","renewal_closed"}})
        self.instance._watch.assert_awaited_once()
        self.assertEqual(self.instance._watch.await_args.args[0].room_id, ROOM)
        self.assertIs(self.instance._tasks[networks.room_id], online)
        self.assertFalse(online.done())
        self.assertEqual(asdict(state_store.load(self.path).binding(networks.room_id)), original_networks)
        # Retry from disk uses the already selected payload and key, even if a
        # new caller supplies different content. A canonical receipt ends replay.
        unavailable = False
        runtime = sender(recovered)
        runtime._persist_binding = adapter.SyntheticSocialityAdapter._persist_binding.__get__(runtime)
        with state_store.state_path_scope(self.path):
            second = await runtime._send_final(ROOM, source, "Different regenerated text")
        self.assertTrue(second.success, getattr(second, "error", None))
        self.assertEqual(second.message_id, "canonical-ds-3225")
        self.assertGreaterEqual(len(posted_requests), 2)
        self.assertTrue(all(request == posted_requests[0] for request in posted_requests))
        self.assertEqual(len(fake_events), 1)
        self.assertEqual(self.binding().delivery_intents["evt-5"]["canonical_event"]["id"], second.message_id)
        before_replay = len(posted_requests)
        with state_store.state_path_scope(self.path), self.assertNoLogs(adapter.logger, "ERROR"):
            after_restart = await sender(self.binding())._send_final(ROOM, source, "Another caller")
        self.assertTrue(after_restart.success)
        self.assertEqual(len(posted_requests), before_replay)
        self.assertEqual(asdict(state_store.load(self.path).binding(networks.room_id)), original_networks)

    async def test_parked_nonterminal_request_or_changed_membership_cannot_exchange(self):
        ds = self.binding()
        ds.inbox["3224"] = "pending"
        state_store.save(state_store.PluginState(bindings=[ds]), self.path)
        original = asdict(ds)
        request = {"roomId": ROOM, "membershipId": ds.membership_id,
                   "requestId": ds.credential_rotation["request_id"], "state": "issued"}
        with mock.patch.object(self.api, "credential_renewal_request", return_value=request) as get, \
                mock.patch.object(self.api, "request_credential_renewal") as claim, \
                mock.patch.object(self.api, "redeem_credential_renewal") as redeem:
            await self.maintain()
            self.assertEqual(asdict(self.binding()), original)
            self.instance._renewal_retry_after.clear()
            get.return_value = {**request, "membershipId": "different-membership", "state": "expired"}
            await self.maintain()
            self.assertEqual(asdict(self.binding()), original)
            self.assertEqual(get.call_count, 2)
            claim.assert_not_called()
            redeem.assert_not_called()
        self.instance._watch.assert_not_awaited()

    @staticmethod
    async def _cancel_task(task):
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
