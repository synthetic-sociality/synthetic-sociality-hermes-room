"""Parity release P1: refusal, attempt authority and failure reporting.

Each ``test_p1_*`` case carries the shared conformance name from
``conformance.json`` ``parityP1Cases``; the OpenClaw connector implements the
same names. Incidents (2026-10-01): D4a — a reply posted after its attempt
lease expired was refused ``cycle_superseded`` and recorded as no visible
reply; D1 — a refused dispatch held the agent's Room until the watchdog.
"""
from __future__ import annotations

import asyncio
import io
import json
import logging
import types
import unittest
import urllib.error
from datetime import datetime, timedelta, timezone
from unittest import mock

from test_delivery_lifecycle import ROOT, adapter, configured_instance


ACCEPTED_LAYERS = {"provider", "runtime", "infrastructure"}
CONFORMANCE = json.loads((ROOT / "conformance.json").read_text())


def lease_in(seconds: float) -> str:
    moment = datetime.now(timezone.utc) + timedelta(seconds=seconds)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def cycle_attempt(attempt_id="attempt-1", lease="2099-08-29T00:00:00Z"):
    return {
        "cycle": {"id": "cycle-1", "generation": 3},
        "attempt": {"id": attempt_id, "membershipId": "member-1", "leaseExpiresAt": lease},
    }


class FakeRoom:
    """The Room API as the connector sees it; records every write."""

    def __init__(self, attempt):
        self.attempt = attempt
        self.claims = 0
        self.claim = lambda _n: self.attempt
        self.head = 5
        self.post_error = None  # callable(post_number) -> ProtocolError | None
        self.complete_error = None
        self.posts = []
        self.completions = []
        self.acks = []
        self.key = ""

    def with_idempotency_key(self, key):
        self.key = key
        return self

    def with_logical_contribution_id(self, _value):
        return self

    def with_message_payload_dialect(self, _value):
        return self

    def claim_discussion_attempt(self, _room_id, _cycle_id):
        self.claims += 1
        claimed = self.claim(self.claims)
        if claimed is None:
            raise adapter.ProtocolError("cycle superseded", code="cycle_superseded", retryable=False)
        return claimed

    def room_state(self, _room_id):
        return {"headSeq": self.head, "activeEpoch": {"id": "epoch-1", "startsAtSeq": 1}}

    def room_policy(self, _room_id):
        return {"coordinationMode": "coordinated"}

    def post_message(self, _room_id, _turn_id, observed, *_args, **_kwargs):
        self.posts.append((self.key, observed))
        error = self.post_error(len(self.posts)) if self.post_error else None
        if error is not None:
            raise error
        return {"id": f"posted-{len(self.posts)}", "seq": 100 + len(self.posts), "ts": "2026-10-01T10:01:51Z"}

    def complete_discussion_attempt(self, _room_id, _cycle_id, attempt_id, payload):
        self.completions.append((attempt_id, json.loads(json.dumps(payload))))
        if self.complete_error is not None:
            raise self.complete_error
        return {"state": "completed"}

    def acknowledge(self, _room_id, seq):
        self.acks.append(seq)
        return {"acknowledgedSeq": seq}


def room_instance(attempt, *, mode="coordinated", queued=()):
    """A connector holding the Room for evt-5 (seq 5) under generation g5."""
    binding = adapter.RoomBinding(
        "https://room.example/api", "room-1", "member-1", "credential-SECRET",
        installation_id="installation-1", cursor=5, acknowledged_cursor=4,
    )
    instance = configured_instance(binding, attempt, [])
    if attempt is None:
        instance._cycle_attempts = {}
    instance._source_coordination_modes = {"evt-5": mode}
    instance._cycle_response_sources = {}
    instance._state = types.SimpleNamespace(binding=lambda room_id: binding if room_id == "room-1" else None)
    instance._stop = asyncio.Event()
    instance._active_dispatch_rooms = {"room-1": "g5"}
    instance._dispatch_started_at = {"g5": 0.0}
    instance._event_dispatch_generation = {"evt-5": "g5"}
    instance._inflight_events = {"evt-5"}
    instance._queued_events = {"room-1": {seq: {"id": f"evt-{seq}", "seq": seq} for seq in queued}}
    instance._ledger_locks = {}
    instance._submission_tasks = {}
    instance._buffered_source = {}
    instance._buffered_output = {}
    binding.inbox["5"] = "pending"
    api = FakeRoom(attempt)
    instance._call = lambda _binding, operation: asyncio.sleep(0, result=operation(api))
    published, dispatched = [], []

    async def publish(_binding, source_id, kind, **fields):
        published.append((source_id, kind, fields.get("status")))

    async def dispatch(_binding, event, _generation):
        dispatched.append(str(event["id"]))
        return True

    instance._publish = publish
    instance._dispatch_room_event = dispatch
    return instance, binding, api, published, dispatched


def source_ref():
    return adapter._dispatch_source_ref("evt-5", "g5")


def completion_event(binding):
    return adapter.MessageEvent(
        message_id=source_ref(), source=types.SimpleNamespace(chat_id=binding.room_id),
        raw_message={"id": "evt-5", "seq": 5, "_dispatchGeneration": "g5"},
    )


def refusal(code, retryable=False):
    return adapter.ProtocolError(f"Room refused the post ({code})", status=409, code=code, retryable=retryable)


def assert_typed_fail(case, payload):
    case.assertEqual(payload["action"], "fail")
    failure = payload.get("executionFailure")
    case.assertIsInstance(failure, dict, payload)
    case.assertIn(failure["layer"], ACCEPTED_LAYERS)
    case.assertTrue(0 < len(failure["code"]) <= 128)
    case.assertEqual(failure["code"], failure["code"].strip().lower())


class ParityP1Tests(unittest.TestCase):
    def test_p1_01_lease_renewed_while_model_runs_and_post_follows_reclaim(self):
        async def run():
            attempt = cycle_attempt(lease=lease_in(3))
            instance, binding, api, _published, _dispatched = room_instance(attempt)
            api.claim = lambda _n: cycle_attempt(lease=lease_in(3))
            instance._start_attempt_renewal(binding, "evt-5", attempt)
            await asyncio.sleep(1.3)  # the model is still running; one renewal is due
            renewals = api.claims
            result = await instance._send_final("room-1", source_ref(), "A considered answer.")
            return instance, binding, api, renewals, result

        instance, binding, api, renewals, result = asyncio.run(run())
        self.assertGreaterEqual(renewals, 1)
        # Pre-post re-claim plus the two authority refreshes around admission.
        self.assertGreaterEqual(api.claims - renewals, 3)
        self.assertTrue(result.success)
        self.assertEqual(len(api.posts), 1)
        self.assertEqual([payload["action"] for _id, payload in api.completions], ["contribute"])
        self.assertNotIn("evt-5", instance._attempt_renewal_tasks)
        self.assertEqual(api.acks, [5])

    def test_p1_02_reclaim_failure_supersedes_without_post(self):
        async def at_pre_post_reclaim():
            instance, _binding, api, _published, _dispatched = room_instance(cycle_attempt())
            api.claim = lambda _n: None
            result = await instance._send_final("room-1", source_ref(), "Late answer.")
            return instance, api, result

        instance, api, result = asyncio.run(at_pre_post_reclaim())
        self.assertTrue(result.success)
        self.assertEqual(api.posts, [])
        self.assertEqual(instance._terminal_results[source_ref()]["status"], "superseded")
        self.assertEqual(instance._terminal_results[source_ref()]["reason"], "attempt_superseded")

        async def at_renewal():
            attempt = cycle_attempt(lease=lease_in(3))
            instance, binding, api, _published, _dispatched = room_instance(attempt)
            api.claim = lambda _n: cycle_attempt(attempt_id="successor", lease=lease_in(3))
            instance._start_attempt_renewal(binding, "evt-5", attempt)
            await asyncio.sleep(1.3)
            key = adapter._attempt_authority_key(binding, "evt-5", attempt)
            durable = binding.delivery_authority[key]["state"]
            api.claim = lambda _n: attempt  # a later valid claim cannot reactivate it
            with self.assertRaises(adapter.ProtocolError) as reactivation:
                await instance._refresh_attempt_authority(binding, "evt-5", attempt)
            result = await instance._send_final("room-1", source_ref(), "Late answer.")
            return instance, api, durable, reactivation.exception.code, result

        instance, api, durable, reactivation_code, result = asyncio.run(at_renewal())
        self.assertEqual(durable, "superseded")
        self.assertEqual(reactivation_code, "attempt_authority_superseded")
        self.assertTrue(result.success)
        self.assertEqual(api.posts, [])
        self.assertEqual(instance._terminal_results[source_ref()]["reason"], "attempt_lease_lost")

    def test_p1_03_post_refused_cycle_superseded_is_superseded_not_pass(self):
        async def run():
            attempt = cycle_attempt()
            instance, binding, api, published, dispatched = room_instance(attempt, queued=(6,))
            api.post_error = lambda _n: refusal("cycle_superseded")
            result = await instance._send_final("room-1", source_ref(), "A produced reply.")
            before_host_completion = list(dispatched)
            await instance.on_processing_complete(completion_event(binding), "success")
            return instance, binding, api, published, dispatched, before_host_completion, result, attempt

        instance, binding, api, published, dispatched, before_host, result, attempt = asyncio.run(run())
        self.assertTrue(result.success)
        terminal = instance._terminal_results[source_ref()]
        self.assertEqual((terminal["status"], terminal["reason"]), ("superseded", "cycle_superseded"))
        self.assertEqual(len(api.posts), 1)
        actions = [payload["action"] for _id, payload in api.completions]
        self.assertNotIn("pass", actions)
        self.assertEqual(actions, ["fail"])
        self.assertEqual(api.completions[0][1]["executionFailure"], {"layer": "infrastructure", "code": "attempt_timeout"})
        self.assertNotIn("evt-5", binding.delivery_intents)
        self.assertEqual(api.acks, [5])
        self.assertNotIn(("evt-5", "terminal", "failed"), published)
        self.assertIn(("evt-5", "terminal", "superseded"), published)
        key = adapter._attempt_authority_key(binding, "evt-5", attempt)
        self.assertEqual(binding.delivery_authority[key]["state"], "superseded")
        # Released at record (P1-6), and the late host callback is a no-op.
        self.assertEqual(before_host, ["evt-6"])
        self.assertEqual(dispatched, ["evt-6"])
        self.assertNotEqual(instance._active_dispatch_rooms.get("room-1"), "g5")

    def test_p1_04_post_refused_stale_epoch_turn_not_active_turn_expired_is_superseded(self):
        for code in ("stale_epoch", "turn_not_active", "turn_expired"):
            with self.subTest(code=code):
                async def run():
                    instance, binding, api, _published, _dispatched = room_instance(cycle_attempt())
                    api.post_error = lambda _n: refusal(code)
                    result = await instance._send_final("room-1", source_ref(), "A produced reply.")
                    return instance, binding, api, result

                instance, binding, api, result = asyncio.run(run())
                self.assertTrue(result.success)
                terminal = instance._terminal_results[source_ref()]
                self.assertEqual((terminal["status"], terminal["reason"]), ("superseded", code))
                self.assertEqual(len(api.posts), 1)
                self.assertNotIn("5", binding.inbox)
                self.assertEqual(binding.acknowledged_cursor, 5)

    def test_p1_05_post_refused_stale_context_refreshes_and_rekeys_once_then_posts(self):
        async def run(head_moves):
            instance, binding, api, _published, _dispatched = room_instance(cycle_attempt())

            def stale_once(number):
                if number == 1:
                    if head_moves:
                        api.head = 7
                    return refusal("stale_context")
                return None

            api.post_error = stale_once
            result = await instance._send_final("room-1", source_ref(), "A produced reply.")
            return instance, binding, api, result

        instance, binding, api, result = asyncio.run(run(True))
        self.assertTrue(result.success)
        self.assertEqual(instance._terminal_results[source_ref()]["status"], "posted")
        self.assertEqual([observed for _key, observed in api.posts], [5, 7])
        self.assertNotEqual(api.posts[0][0], api.posts[1][0])
        self.assertEqual(len({key for key, _observed in api.posts}), 2)

        # No newer head: bounded, never a loop and never a silent success.
        instance, binding, api, result = asyncio.run(run(False))
        self.assertFalse(result.success)
        self.assertEqual(len(api.posts), 1)
        # Quarantined, then released at record with a typed fail (P1-6).
        self.assertEqual(
            [payload.get("executionFailure") for _id, payload in api.completions],
            [{"layer": "infrastructure", "code": "attempt_timeout"}],
        )
        self.assertEqual(api.acks, [5])
        self.assertNotIn("evt-5", binding.delivery_intents)

    def test_p1_06_error_code_vocabulary_keeps_every_listed_code(self):
        codes = CONFORMANCE["errorCodes"]
        self.assertEqual(codes, sorted(set(codes)))
        for required in (
            "stale_epoch", "turn_not_active", "turn_expired", "turns_not_used",
            "message_too_large", "idempotency_mismatch", "invalid_contribution",
            "cycle_superseded", "reply_cap_reached", "stale_context",
        ):
            self.assertIn(required, codes)
        expected = {"reply_cap_reached": "skipped"}
        expected.update({code: "superseded" for code in adapter.POST_REFUSAL_SUPERSEDED_CODES})
        protocol = adapter.RoomProtocol("https://room.example/api", "credential-SECRET")
        for code in codes + ["a_code_no_list_names"]:
            with self.subTest(code=code):
                envelope = json.dumps({"code": code, "message": "refused"}).encode()
                error = urllib.error.HTTPError(
                    "https://room.example/api/x", 409, "Conflict", {}, io.BytesIO(envelope),
                )
                with mock.patch("urllib.request.urlopen", side_effect=error):
                    with self.assertRaises(adapter.ProtocolError) as raised:
                        protocol.request("POST", "/rooms/room-1/messages", {})
                self.assertEqual(raised.exception.code, code)  # verbatim, never ""
                outcome = adapter.post_refusal_outcome(raised.exception.code, raised.exception.retryable)
                self.assertEqual(outcome, expected.get(code, "quarantined"))
        self.assertEqual(adapter.post_refusal_outcome("busy", True), "retryable")
        self.assertEqual(adapter.post_refusal_outcome("cycle_superseded", True), "superseded")

    def test_p1_07_every_fail_carries_execution_failure(self):
        fails = []

        async def typed_host_outcome():
            instance, _binding, api, _p, _d = room_instance(cycle_attempt())
            await instance._send_final_owned(
                "room-1", source_ref(), "",
                operational_outcome=adapter.host_operational_outcome({"operational_outcome": {
                    "layer": "billing", "code": "Quota_Exhausted", "retryable": False,
                }}),
            )
            return api

        async def legacy_fallback():
            instance, _binding, api, _p, _d = room_instance(cycle_attempt())
            await instance._send_final("room-1", source_ref(), next(iter(adapter._GATEWAY_OPERATIONAL_ERRORS)))
            return api

        async def processing_failure():
            instance, binding, api, _p, _d = room_instance(cycle_attempt())
            await instance.on_processing_complete(completion_event(binding), "failure")
            return api

        async def size_refusal():
            instance, binding, api, _p, _d = room_instance(cycle_attempt())
            instance._record_delivery_failure(binding, "evt-5", "too large", True, "message_too_large")
            await instance.on_processing_complete(completion_event(binding), "success")
            return api

        async def send_path_quarantine():
            instance, _binding, api, _p, _d = room_instance(cycle_attempt())
            api.post_error = lambda _n: refusal("validation_error")
            await instance._send_final("room-1", source_ref(), "A produced reply.")
            return api

        async def ttl_quarantine_and_wedge():
            instance, binding, api, _p, _d = room_instance(cycle_attempt())
            await instance._release_cycle_attempt_after_failure(binding, "evt-5", "quarantined_unprocessed")
            return api

        async def bare_call():
            instance, binding, api, _p, _d = room_instance(cycle_attempt())
            await instance._complete_cycle_attempt(binding, cycle_attempt(), "fail")
            return api

        for name, scenario in {
            "typed_host_outcome": typed_host_outcome, "legacy_fallback": legacy_fallback,
            "processing_failure": processing_failure, "size_refusal": size_refusal,
            "send_path_quarantine": send_path_quarantine,
            "ttl_quarantine_and_wedge": ttl_quarantine_and_wedge, "bare_call": bare_call,
        }.items():
            with self.subTest(scenario=name):
                api = asyncio.run(scenario())
                failed = [payload for _id, payload in api.completions if payload["action"] == "fail"]
                self.assertTrue(failed, name)
                for payload in failed:
                    assert_typed_fail(self, payload)
                fails.extend((name, payload["executionFailure"]) for payload in failed)
        by_name = dict(fails)
        self.assertEqual(by_name["typed_host_outcome"], {"layer": "provider", "code": "quota_exhausted"})
        self.assertEqual(by_name["legacy_fallback"], {"layer": "runtime", "code": "legacy_gateway_operational_error"})
        self.assertEqual(by_name["processing_failure"], {"layer": "runtime", "code": "execution_failed"})
        self.assertEqual(by_name["size_refusal"]["code"], "response_too_large")
        self.assertEqual(by_name["send_path_quarantine"], {"layer": "infrastructure", "code": "attempt_timeout"})
        self.assertEqual(by_name["ttl_quarantine_and_wedge"], {"layer": "infrastructure", "code": "attempt_timeout"})
        self.assertEqual(adapter.typed_execution_failure("future_layer", " "), {"layer": "runtime", "code": "execution_failed"})
        self.assertEqual(len(adapter.typed_execution_failure("runtime", "x" * 300)["code"]), 128)

    def test_p1_08_send_path_quarantine_releases_attempt_stops_renewal_and_dispatches_next(self):
        async def run(with_attempt):
            attempt = cycle_attempt() if with_attempt else None
            instance, binding, api, _published, dispatched = room_instance(
                attempt, mode="coordinated" if with_attempt else "open", queued=(6,),
            )
            if with_attempt:
                instance._start_attempt_renewal(binding, "evt-5", attempt)
                await asyncio.sleep(0)
            renewal = instance._attempt_renewal_tasks.get("evt-5")
            api.post_error = lambda _n: refusal("validation_error")
            result = await instance._send_final("room-1", source_ref(), "A produced reply.")
            released_before_host = list(dispatched)
            await instance.on_processing_complete(completion_event(binding), "failure")
            return instance, binding, api, dispatched, released_before_host, renewal, result

        instance, binding, api, dispatched, before_host, renewal, result = asyncio.run(run(True))
        self.assertFalse(result.success)
        self.assertTrue(renewal.cancelled() or renewal.done())
        self.assertNotIn("evt-5", instance._attempt_renewal_tasks)
        self.assertEqual(
            [payload for _id, payload in api.completions],
            [{"generation": 3, "action": "fail", "executionFailure": {"layer": "infrastructure", "code": "attempt_timeout"}}],
        )
        self.assertNotIn("evt-5", instance._cycle_attempts)
        self.assertIn("evt-5", instance._superseded_sources)
        self.assertEqual(api.acks, [5])  # recorded failed, never a pass
        self.assertEqual(before_host, ["evt-6"])
        self.assertEqual(dispatched, ["evt-6"])

        instance, binding, api, dispatched, before_host, _renewal, result = asyncio.run(run(False))
        self.assertFalse(result.success)
        self.assertEqual(api.completions, [])
        self.assertEqual(binding.inbox["5"], "quarantined")  # held for the operator
        self.assertEqual(binding.acknowledged_cursor, 4)
        self.assertEqual(binding.delivery_intents["evt-5"]["delivery_state"], "quarantined")
        self.assertEqual(before_host, ["evt-6"])
        self.assertEqual(dispatched, ["evt-6"])

    def test_p1_09_refusal_outcomes_are_logged_without_body_or_secrets(self):
        body = "SECRET-BODY-TEXT that must never reach a log"

        async def run(code, mode, attempt):
            instance, _binding, api, _p, _d = room_instance(attempt, mode=mode)
            api.post_error = lambda _n: refusal(code)
            await instance._send_final("room-1", source_ref(), body)

        with self.assertLogs(adapter.logger, level="INFO") as logs:
            asyncio.run(run("reply_cap_reached", "open", None))
            asyncio.run(run("cycle_superseded", "coordinated", cycle_attempt()))
            asyncio.run(run("validation_error", "coordinated", cycle_attempt()))
        output = "\n".join(logs.output)
        self.assertIn("room-1 skipped source seq 5 attempt - code reply_cap_reached", output)
        self.assertIn("room-1 superseded source seq 5 attempt attempt-1 code cycle_superseded", output)
        self.assertIn("quarantined failed delivery for sequence 5 attempt attempt-1 code validation_error", output)
        self.assertIn("room-1 settled attempt attempt-1 action fail code infrastructure/attempt_timeout", output)
        self.assertNotIn("SECRET-BODY-TEXT", output)
        self.assertNotIn("credential-SECRET", output)

    def test_p1_10_d4a_replay_produces_superseded_and_a_typed_settle(self):
        # D4a shape (staging Room, 2026-10-01): 949 discussion.started opens
        # the epoch; 950 closes the prior epoch's cycle; 951 starts the cycle;
        # 952 offers this agent the attempt. Its lease (claim + 120 s) expired
        # before the model finished, the post was refused cycle_superseded, and
        # 953-957 continued with the other agent. Neutral IDs; codes as observed.
        async def run():
            attempt = {
                "cycle": {"id": "cycle-d4a", "generation": 1},
                "attempt": {"id": "attempt-d4a", "membershipId": "member-1", "leaseExpiresAt": "2099-01-01T00:00:00Z"},
            }
            instance, binding, api, published, _dispatched = room_instance(None)
            binding.cursor = binding.acknowledged_cursor = 951
            binding.inbox = {"952": "pending"}
            instance._cycle_attempts = {"evt-952": attempt}
            instance._source_coordination_modes = {"evt-952": "coordinated"}
            instance._event_seq = {"room-1": {"evt-952": 952}}
            instance._event_epoch = {"evt-952": "epoch-1"}
            instance._event_dispatch_generation = {"evt-952": "g952"}
            instance._active_dispatch_rooms = {"room-1": "g952"}
            instance._inflight_events = {"evt-952"}
            key = adapter._attempt_authority_key(binding, "evt-952", attempt)
            binding.delivery_authority[key] = adapter._attempt_authority_record(binding, "evt-952", attempt)
            api.attempt = attempt
            api.head = 952
            api.post_error = lambda _n: refusal("cycle_superseded")
            # The server already timed the attempt out; the settle is refused too.
            api.complete_error = refusal("cycle_superseded")
            ref = adapter._dispatch_source_ref("evt-952", "g952")
            result = await instance._send_final("room-1", ref, "Zurie's reply, produced and refused.")
            return instance, binding, api, published, ref, result

        instance, binding, api, published, ref, result = asyncio.run(run())
        self.assertTrue(result.success)
        terminal = instance._terminal_results[ref]
        self.assertEqual((terminal["status"], terminal["reason"]), ("superseded", "cycle_superseded"))
        self.assertNotEqual(terminal["reason"], "model_skip")
        self.assertEqual(len(api.posts), 1)  # never reposted
        self.assertEqual(len(api.completions), 1)
        assert_typed_fail(self, api.completions[0][1])
        self.assertEqual(api.acks, [952])
        self.assertNotIn("evt-952", binding.delivery_intents)
        self.assertIn(("evt-952", "terminal", "superseded"), published)
        self.assertEqual(instance._active_dispatch_rooms, {})

    def test_d1_replay_refused_dispatch_releases_room_without_host_completion(self):
        # D1 shape (staging Room, 2026-10-01): 960 offered this agent an attempt;
        # its turn expired (963) and the owner's targeted ask (964) interrupted
        # the cycle (965) while the connector still held the Room. The host's
        # completion callback is deliberately never called here.
        for code in ("turn_expired", "cycle_superseded"):
            with self.subTest(code=code):
                async def run():
                    attempt = cycle_attempt(lease=lease_in(600))
                    instance, binding, api, _published, dispatched = room_instance(attempt, queued=(6, 7))
                    instance._start_attempt_renewal(binding, "evt-5", attempt)
                    await asyncio.sleep(0)
                    api.post_error = lambda _n: refusal(code)
                    await instance._send_final("room-1", source_ref(), "A produced reply.")
                    return instance, api, dispatched

                instance, api, dispatched = asyncio.run(run())
                self.assertEqual(dispatched, ["evt-6"])
                self.assertNotIn("evt-5", instance._attempt_renewal_tasks)
                self.assertNotIn("evt-5", instance._inflight_events)
                self.assertNotEqual(instance._active_dispatch_rooms.get("room-1"), "g5")
                self.assertEqual([payload["action"] for _id, payload in api.completions], ["fail"])


if __name__ == "__main__":
    unittest.main()
