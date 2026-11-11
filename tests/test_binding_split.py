"""One live RoomBinding per Room after expiry, revocation and renewal.

Change request A (2026-11-11): when one Room's credential expired or was
revoked, ``_expire``/``_revoke`` replaced the whole in-memory ``PluginState``.
Every other Room's listener kept its old ``RoomBinding`` while the send path
read the replaced snapshot, so the next turn in a sibling Room failed with
``attempt_authority_missing`` and its finished reply was quarantined.

These tests drive the real adapter from a real state file with two bound
Rooms. Each listener object is the exact object ``connect()`` hands to
``_watch``; the send path is the real ``send`` -> ``_binding`` route.
"""
from __future__ import annotations

import asyncio
import copy
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from test_delivery_lifecycle import adapter, state_store


ROOM_A = "room-a"
ROOM_B = "room-b"
SOURCE = "evt-5"


def _binding(room_id: str, membership_id: str, installation_id: str):
    return adapter.RoomBinding(
        "https://room.example/api", room_id, membership_id, f"credential-{room_id}",
        installation_id=installation_id, cursor=5, acknowledged_cursor=4,
        credential_expires_at="2099-01-01T00:00:00Z",
    )


class BindingSplitTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._directory = tempfile.TemporaryDirectory()
        self.addCleanup(self._directory.cleanup)
        self.path = Path(self._directory.name) / "state.json"
        state_store.save(adapter.PluginState(bindings=[
            _binding(ROOM_A, "member-a", "installation-a"),
            _binding(ROOM_B, "member-b", "installation-b"),
        ]), self.path)
        scope = state_store.state_path_scope(self.path)
        scope.__enter__()
        self.addCleanup(scope.__exit__, None, None, None)
        self.instance = adapter.SyntheticSocialityAdapter(adapter.PlatformConfig())
        self.assertEqual(self.instance._state_file, self.path)
        # Exactly the objects connect() passes to _watch for each Room.
        self.listeners = {binding.room_id: binding for binding in self.instance._state.bindings}
        self.posted: list[tuple[str, str]] = []
        self._configure_send_path()

    def _configure_send_path(self) -> None:
        """Unit boundary below the real send -> _binding route (no network)."""
        instance = self.instance
        instance._binding_generation_active = lambda _binding: True
        instance._persist_binding = lambda _binding: True
        instance._event_seq = {ROOM_A: {SOURCE: 5}, ROOM_B: {SOURCE: 5}}
        instance._event_epoch = {SOURCE: "epoch-1"}
        instance._source_coordination_modes = {SOURCE: "open"}
        instance._open_reply_recipients = {}
        instance._cycle_response_sources = {SOURCE: "evt-human"}
        instance._superseded_sources = set()
        instance._attempt_renewal_tasks = {}
        instance._terminal_sources = {}
        instance._terminal_results = {}
        instance._run_for_event = {SOURCE: "run-1"}
        instance._activity_seq = {}
        instance._publish = lambda *_args, **_kwargs: asyncio.sleep(0)
        posted = self.posted

        def call(binding, operation, **_request_options):
            room_id = binding.room_id

            class API:
                def claim_discussion_attempt(self, *_args):
                    return instance._cycle_attempts.get(SOURCE)

                def room_state(self, _room_id):
                    return {"headSeq": 5, "activeEpoch": {"id": "epoch-1", "startsAtSeq": 1}}

                def post_message(self, *args, **_kwargs):
                    posted.append((room_id, args[4]))
                    return {"id": "posted-6", "seq": 6, "ts": "2026-11-11T00:00:00Z"}

                def complete_discussion_attempt(self, *_args):
                    return {"state": "completed"}

                def acknowledge(self, _room_id, seq):
                    return {"acknowledgedSeq": seq}

            return asyncio.sleep(0, result=operation(API()))

        instance._call = call

    def _listener_claims_turn(self, room_id: str) -> dict:
        """What the Room listener does when it claims a turn (adapter ~line 5000)."""
        listener = self.listeners[room_id]
        attempt = {
            "cycle": {"id": f"cycle-{room_id}", "generation": 3},
            "attempt": {"id": f"attempt-{room_id}", "membershipId": listener.membership_id,
                        "leaseExpiresAt": "2099-08-29T00:00:00Z"},
        }
        key = adapter._attempt_authority_key(listener, SOURCE, attempt)
        listener.delivery_authority[key] = adapter._attempt_authority_record(listener, SOURCE, attempt)
        self.instance._cycle_attempts = {SOURCE: attempt}
        return attempt

    async def _send_turn(self, room_id: str, generation: str):
        return await self.instance.send(
            room_id, f"reply in {room_id}",
            reply_to=adapter._dispatch_source_ref(SOURCE, generation),
            metadata={"notify": True},
        )

    async def _listener_stops_on(self, room_id: str, code: str) -> None:
        """Run the real listener until the Room answers with a terminal credential error."""
        async def refresh(binding):
            if binding.room_id == room_id:
                raise adapter.ProtocolError(code, code=code, retryable=False)
            return True

        self.instance._refresh_binding = refresh
        await asyncio.wait_for(self.instance._watch(self.listeners[room_id], is_reconnect=False), 2)

    def assert_one_live_binding_per_room(self, rooms) -> None:
        for room_id in rooms:
            with self.subTest(invariant=room_id):
                self.assertIs(self.instance._binding(room_id), self.listeners[room_id])

    async def assert_sibling_turn_is_accepted(self) -> None:
        self._listener_claims_turn(ROOM_B)
        result = await self._send_turn(ROOM_B, "g-b")
        self.assertTrue(result.success, getattr(result, "error", None))
        self.assertEqual(result.message_id, "posted-6")
        self.assertEqual(self.posted, [(ROOM_B, "reply in room-b")])
        intent = self.listeners[ROOM_B].delivery_intents.get(SOURCE, {})
        self.assertNotEqual(intent.get("state"), "quarantined")
        self.assertNotEqual(intent.get("last_error_code"), "attempt_authority_missing")

    async def assert_stopped_room_posts_nothing(self, reason: str) -> None:
        stopped = self.listeners[ROOM_A]
        live = self.instance._state.binding(ROOM_A)
        persisted = state_store.load(self.path).binding(ROOM_A)
        for label, binding in (("listener", stopped), ("send path", live), ("file", persisted)):
            with self.subTest(stopped_room=label):
                self.assertFalse(binding.enabled)
                self.assertEqual(binding.disabled_reason, reason)
                self.assertEqual(binding.revoked, reason == "credential_revoked")
                self.assertEqual(binding.connector_session_id, "")
        self.assertNotIn(ROOM_A, self.instance._tasks)
        # A late host callback for the stopped Room must not become Room speech.
        self.instance._binding_generation_active = adapter.SyntheticSocialityAdapter._binding_generation_active.__get__(
            self.instance)
        before = list(self.posted)
        self._listener_claims_turn(ROOM_A)
        result = await self._send_turn(ROOM_A, "g-a")
        self.assertFalse(result.success)
        self.assertEqual(self.posted, before)

    async def test_expiry_in_one_room_keeps_sibling_send_path_on_the_listener_binding(self):
        await self._listener_stops_on(ROOM_A, "credential_expired")
        self.assert_one_live_binding_per_room([ROOM_B])
        await self.assert_sibling_turn_is_accepted()
        self.assert_one_live_binding_per_room([ROOM_B])
        await self.assert_stopped_room_posts_nothing("credential_expired")

    async def test_revocation_in_one_room_keeps_sibling_send_path_on_the_listener_binding(self):
        await self._listener_stops_on(ROOM_A, "credential_revoked")
        self.assert_one_live_binding_per_room([ROOM_B])
        await self.assert_sibling_turn_is_accepted()
        self.assert_one_live_binding_per_room([ROOM_B])
        await self.assert_stopped_room_posts_nothing("credential_revoked")

    async def test_expiry_preserves_a_concurrently_persisted_revocation(self):
        # Existing control: expiry is weaker evidence than a persisted revocation.
        state_store.update(lambda current: setattr(current.binding(ROOM_A), "revoked", True))
        await self._listener_stops_on(ROOM_A, "credential_expired")
        self.assertTrue(state_store.load(self.path).binding(ROOM_A).revoked)
        self.assertTrue(self.listeners[ROOM_A].revoked)
        self.assertTrue(self.instance._binding(ROOM_A).revoked)
        self.assert_one_live_binding_per_room([ROOM_B])

    async def test_renewal_in_one_room_keeps_one_shared_binding_per_room(self):
        instance = self.instance
        target = self.listeners[ROOM_A]
        watched: dict[str, object] = {}

        async def idle_listener():
            try:
                await asyncio.Event().wait()
            finally:
                instance._tasks.pop(ROOM_A, None)

        old_task = asyncio.create_task(idle_listener())
        sibling_task = asyncio.create_task(asyncio.Event().wait())
        self.addCleanup(sibling_task.cancel)
        instance._tasks = {ROOM_A: old_task, ROOM_B: sibling_task}
        await asyncio.sleep(0)

        async def watch(binding, **_kwargs):
            watched[binding.room_id] = binding

        async def active(_binding, **_kwargs):
            return True

        def renew(exact_path, room_id, _intent):
            state_store.update(lambda s: setattr(s.binding(room_id), "credential", "renewed"), exact_path)

        instance._watch = watch
        instance._membership_active = active
        with mock.patch.object(adapter.renewal, "discover", return_value={"requestId": "intent"}), \
                mock.patch.object(adapter.renewal, "resume", renew):
            await instance._maintain_renewal(target)
        await asyncio.gather(instance._tasks[ROOM_A], return_exceptions=True)
        self.assertIs(instance._tasks[ROOM_B], sibling_task)
        # The renewed Room's new listener and its send path share one object.
        self.assertIn(ROOM_A, watched)
        self.assertIs(instance._binding(ROOM_A), watched[ROOM_A])
        self.assertEqual(instance._binding(ROOM_A).credential, "renewed")
        self.listeners[ROOM_A] = watched[ROOM_A]
        self.assert_one_live_binding_per_room([ROOM_A, ROOM_B])
        await self.assert_sibling_turn_is_accepted()
        self.assert_one_live_binding_per_room([ROOM_A, ROOM_B])

    async def test_renewal_while_the_listener_runs_never_replaces_its_binding(self):
        # A listener that is still registered when renewal finishes (for
        # example a reconnect started meanwhile) must keep the send path's
        # object: renewal applies the persisted fields to that same object.
        instance = self.instance
        running = asyncio.create_task(asyncio.Event().wait())
        self.addCleanup(running.cancel)

        def renew(exact_path, room_id, _intent):
            state_store.update(lambda s: setattr(s.binding(room_id), "credential", "renewed"), exact_path)
            instance._tasks[room_id] = running

        async def active(_binding, **_kwargs):
            return True

        instance._tasks = {}
        instance._watch = lambda *_args, **_kwargs: self.fail("a running listener must not get a second one")
        instance._membership_active = active
        with mock.patch.object(adapter.renewal, "discover", return_value={"requestId": "intent"}), \
                mock.patch.object(adapter.renewal, "resume", renew):
            await instance._maintain_renewal(self.listeners[ROOM_A])
        self.assert_one_live_binding_per_room([ROOM_A, ROOM_B])
        self.assertEqual(self.listeners[ROOM_A].credential, "renewed")


if __name__ == "__main__":
    unittest.main()
