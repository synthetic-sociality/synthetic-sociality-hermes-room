"""A slow or wedged model run must never block the Room for later events.

Reproduces the 2026-09-12 ByteDance incident: a 231-second model run was
re-dispatched at 180 seconds, the duplicate never completed, its Room marker
blocked every later event, and the connector kept renewing an attempt it had
already quarantined until the cycle deadline.
"""
from __future__ import annotations

import asyncio
import time
import unittest

from test_delivery_lifecycle import adapter


def stale_instance(binding, *, cycle_attempts=None):
    instance = object.__new__(adapter.SyntheticSocialityAdapter)
    instance._binding = lambda _room_id: binding
    instance._persist_binding = lambda _binding: True
    instance._event_seq = {binding.room_id: {}}
    instance._inflight_events = set()
    instance._event_dispatch_generation = {}
    instance._active_dispatch_rooms = {}
    instance._dispatch_started_at = {}
    instance._cycle_attempts = dict(cycle_attempts or {})
    instance._cycle_response_sources = {}
    instance._attempt_renewal_tasks = {}
    instance.completed = []
    instance.published = []
    instance.stopped = []
    instance.failures = []
    instance._superseded_sources = set()

    async def complete(_binding, cycle_attempt, action, event_id="", execution_failure=None):
        instance.completed.append((str(cycle_attempt["attempt"]["id"]), action))
        instance.failures.append(execution_failure)

    async def stop(source_event_id):
        instance.stopped.append(source_event_id)

    async def publish(_binding, event_id, kind, **fields):
        instance.published.append((event_id, kind, fields.get("status")))

    instance._complete_cycle_attempt = complete
    instance._stop_attempt_renewal = stop
    instance._publish = publish
    return instance


def binding_with_pending(seqs, *, retries=None, age=200.0):
    binding = adapter.RoomBinding("https://room.example/api", "room-1", "member-1", "credential", cursor=0, acknowledged_cursor=0)
    now = time.time()
    for seq in seqs:
        binding.inbox[str(seq)] = "pending"
        binding.pending_since[str(seq)] = now - age
        binding.pending_retries[str(seq)] = (retries or {}).get(seq, 0)
    return binding


ATTEMPT = {"cycle": {"id": "cycle-1", "generation": 1}, "attempt": {"id": "attempt-1", "leaseExpiresAt": "2099-01-01T00:00:00Z"}}


class StaleDispatchRecoveryTests(unittest.TestCase):
    def test_running_dispatch_is_not_duplicated_after_the_pending_ttl(self):
        binding = binding_with_pending([367])
        instance = stale_instance(binding, cycle_attempts={"evt-367": ATTEMPT})
        instance._event_seq["room-1"]["evt-367"] = 367
        instance._inflight_events.add("evt-367")
        instance._event_dispatch_generation["evt-367"] = "gen-a"
        instance._active_dispatch_rooms["room-1"] = "gen-a"
        instance._dispatch_started_at["gen-a"] = time.time() - 200
        before = time.time()
        asyncio.run(instance._expire_stale_pending(binding))
        self.assertEqual(binding.inbox["367"], "pending")
        self.assertEqual(binding.pending_retries["367"], 0)
        self.assertGreaterEqual(binding.pending_since["367"], before)
        self.assertEqual(instance._active_dispatch_rooms, {"room-1": "gen-a"})
        self.assertIn("evt-367", instance._inflight_events)
        self.assertEqual(instance.completed, [])
        self.assertEqual(instance.stopped, [])

    def test_wedged_dispatch_releases_the_room_and_fails_its_attempt(self):
        binding = binding_with_pending([384])
        instance = stale_instance(binding, cycle_attempts={"evt-367": ATTEMPT})
        instance._event_seq["room-1"].update({"evt-367": 367, "evt-384": 384})
        instance._inflight_events.add("evt-367")
        instance._event_dispatch_generation["evt-367"] = "gen-dup"
        instance._active_dispatch_rooms["room-1"] = "gen-dup"
        instance._dispatch_started_at["gen-dup"] = time.time() - adapter.DISPATCH_INFLIGHT_MAX_SECONDS - 5
        asyncio.run(instance._expire_stale_pending(binding))
        self.assertEqual(instance._active_dispatch_rooms, {})
        self.assertNotIn("evt-367", instance._inflight_events)
        self.assertNotIn("evt-367", instance._event_dispatch_generation)
        self.assertEqual(instance.completed, [("attempt-1", "fail")])
        self.assertEqual(instance.stopped, ["evt-367"])
        self.assertNotIn("evt-367", instance._cycle_attempts)
        self.assertEqual(instance.published, [("evt-367", "terminal", "failed")])
        self.assertEqual(instance.failures, [{"layer": "infrastructure", "code": "attempt_timeout"}])
        self.assertIn("evt-367", instance._superseded_sources)
        # The queued later event follows the ordinary bounded retry path.
        self.assertEqual(binding.inbox["384"], "failed-retryable")
        self.assertEqual(binding.pending_retries["384"], 1)

    def test_quarantine_stops_renewal_and_reports_the_failed_attempt(self):
        binding = binding_with_pending([384], retries={384: adapter.PENDING_EVENT_MAX_RETRIES})
        instance = stale_instance(binding, cycle_attempts={"evt-384": ATTEMPT})
        instance._event_seq["room-1"]["evt-384"] = 384
        instance._cycle_response_sources["evt-384"] = "evt-human"
        asyncio.run(instance._expire_stale_pending(binding))
        self.assertEqual(binding.inbox["384"], "quarantined")
        self.assertEqual(binding.acknowledged_cursor, 0)
        self.assertEqual(instance.completed, [("attempt-1", "fail")])
        self.assertEqual(instance.stopped, ["evt-384"])
        self.assertNotIn("evt-384", instance._cycle_attempts)
        self.assertNotIn("evt-384", instance._cycle_response_sources)
        self.assertEqual(instance.failures, [{"layer": "infrastructure", "code": "attempt_timeout"}])
        self.assertIn("evt-384", instance._superseded_sources)

    def test_superseded_attempt_report_is_not_an_error(self):
        binding = binding_with_pending([9], retries={9: adapter.PENDING_EVENT_MAX_RETRIES})
        instance = stale_instance(binding, cycle_attempts={"evt-9": ATTEMPT})
        instance._event_seq["room-1"]["evt-9"] = 9

        async def superseded(_binding, _cycle_attempt, _action, event_id="", execution_failure=None):
            raise adapter.ProtocolError("cycle superseded", code="cycle_superseded", retryable=False)

        instance._complete_cycle_attempt = superseded
        asyncio.run(instance._expire_stale_pending(binding))
        self.assertEqual(binding.inbox["9"], "quarantined")
        self.assertEqual(instance.stopped, ["evt-9"])


def queue_behind(instance, binding, holder_seq=None, *, holder_generation="gen-a", started_age=10.0):
    """Hold the Room with a running dispatch and record dispatches of queued events."""
    instance._queued_events = {binding.room_id: {}}
    instance.dispatched = []

    async def dispatch(_binding, event, generation):
        instance.dispatched.append((str(event.get("id")), generation))
        return True

    instance._dispatch_room_event = dispatch
    if holder_seq is not None:
        holder = f"evt-{holder_seq}"
        instance._event_seq[binding.room_id][holder] = holder_seq
        instance._inflight_events.add(holder)
        instance._event_dispatch_generation[holder] = holder_generation
        instance._active_dispatch_rooms[binding.room_id] = holder_generation
        instance._dispatch_started_at[holder_generation] = time.time() - started_age
    return instance


def enqueue(instance, binding, seq, *, event_id=None, index=True):
    event_id = event_id or f"evt-{seq}"
    instance._queued_events[binding.room_id][seq] = {"id": event_id, "sequence": seq}
    if index:
        instance._event_seq[binding.room_id][event_id] = seq


class QueuedBehindDispatchTests(unittest.TestCase):
    def _assert_waits(self, retries):
        binding = binding_with_pending([2], retries={2: retries})
        instance = queue_behind(stale_instance(binding, cycle_attempts={"evt-2": ATTEMPT}), binding, holder_seq=1)
        enqueue(instance, binding, 2)
        before = time.time()
        asyncio.run(instance._expire_stale_pending(binding))
        self.assertEqual(binding.inbox["2"], "pending")
        self.assertEqual(binding.pending_retries["2"], retries)
        self.assertGreaterEqual(binding.pending_since["2"], before)
        self.assertEqual(instance._active_dispatch_rooms, {"room-1": "gen-a"})
        self.assertIn(2, instance._queued_events["room-1"])
        self.assertIn("evt-2", instance._cycle_attempts)
        self.assertEqual(instance.completed, [])
        self.assertEqual(instance.stopped, [])
        self.assertEqual(instance.dispatched, [])

    def test_queued_event_waits_while_another_dispatch_holds_the_room(self):
        self._assert_waits(0)

    def test_queued_event_at_retry_cap_is_not_quarantined_while_room_is_held(self):
        self._assert_waits(adapter.PENDING_EVENT_MAX_RETRIES)

    def test_wedged_holder_is_released_and_the_queue_is_dispatched(self):
        binding = binding_with_pending([2], age=1.0)
        instance = queue_behind(
            stale_instance(binding, cycle_attempts={"evt-1": ATTEMPT}), binding, holder_seq=1,
            started_age=adapter.DISPATCH_INFLIGHT_MAX_SECONDS + 5,
        )
        enqueue(instance, binding, 2)
        asyncio.run(instance._expire_stale_pending(binding))
        self.assertEqual(instance.completed, [("attempt-1", "fail")])
        self.assertEqual(instance.stopped, ["evt-1"])
        self.assertEqual([event_id for event_id, _ in instance.dispatched], ["evt-2"])
        self.assertEqual(instance._active_dispatch_rooms["room-1"], instance.dispatched[0][1])
        self.assertEqual(binding.inbox["2"], "pending")

    def test_quarantined_queued_event_is_never_dispatched(self):
        binding = binding_with_pending([3], age=1.0)
        binding.inbox["2"] = "quarantined"
        instance = queue_behind(stale_instance(binding), binding)
        enqueue(instance, binding, 2)
        enqueue(instance, binding, 3)
        asyncio.run(instance._dispatch_next_queued(binding))
        self.assertEqual([event_id for event_id, _ in instance.dispatched], ["evt-3"])
        self.assertNotIn(2, instance._queued_events.get("room-1", {}))

    def test_queued_event_quarantined_without_holder_releases_its_attempt(self):
        binding = binding_with_pending([2], retries={2: adapter.PENDING_EVENT_MAX_RETRIES})
        instance = queue_behind(stale_instance(binding, cycle_attempts={"evt-2": ATTEMPT}), binding)
        # The sequence index can be gone; the queued event still names its id.
        enqueue(instance, binding, 2, index=False)
        asyncio.run(instance._expire_stale_pending(binding))
        self.assertEqual(binding.inbox["2"], "quarantined")
        self.assertEqual(instance.completed, [("attempt-1", "fail")])
        self.assertEqual(instance.stopped, ["evt-2"])
        self.assertNotIn("evt-2", instance._cycle_attempts)
        self.assertEqual(instance.dispatched, [])

    def test_queued_event_without_holder_still_retries(self):
        binding = binding_with_pending([2])
        instance = queue_behind(stale_instance(binding), binding)
        enqueue(instance, binding, 2)
        asyncio.run(instance._expire_stale_pending(binding))
        self.assertEqual(binding.pending_retries["2"], 1)
        self.assertNotEqual(binding.inbox["2"], "quarantined")
        self.assertEqual(instance.completed, [])

    def test_pending_event_outside_the_queue_still_retries_while_room_is_held(self):
        binding = binding_with_pending([5])
        instance = queue_behind(stale_instance(binding), binding, holder_seq=1)
        instance._event_seq["room-1"]["evt-5"] = 5
        asyncio.run(instance._expire_stale_pending(binding))
        self.assertEqual(binding.inbox["5"], "failed-retryable")
        self.assertEqual(binding.pending_retries["5"], 1)
        self.assertEqual(instance._active_dispatch_rooms, {"room-1": "gen-a"})
        self.assertEqual(instance.dispatched, [])


if __name__ == "__main__":
    unittest.main()
