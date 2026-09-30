from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

from test_delivery_lifecycle import adapter, state_store


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests" / "fixtures" / "joined-coding-epoch-terminal" / "seq-838-840.canonical.json"
FIXTURE_SHA256 = "f7419f81c5a63d9ac9e5c6af229143e60d14e6657fe2283ab4b117ea00ece320"


def load_fixture() -> dict:
    raw = FIXTURE.read_bytes()
    if hashlib.sha256(raw).hexdigest() != FIXTURE_SHA256:
        raise AssertionError("Joined Coding epoch-terminal fixture bytes changed")
    return json.loads(raw)


def make_binding():
    binding = adapter.RoomBinding(
        "https://room.example/api",
        "bjqiu9u7dwqptlq",
        "membership-test",
        "credential-test",
        installation_id="installation-test",
        cursor=840,
        acknowledged_cursor=836,
    )
    # A pre-existing quarantined gap must remain owner-protected. It also keeps
    # later terminal evidence inspectable across the real save/load boundary.
    binding.inbox["837"] = "quarantined"
    binding.pending_retries["837"] = 2
    return binding


def make_instance(binding, path: Path | None = None):
    instance = object.__new__(adapter.SyntheticSocialityAdapter)
    for name, value in {
        "_inflight_events": set(),
        "_event_seq": {},
        "_event_epoch": {},
        "_latest_source": {},
        "_run_for_event": {},
        "_activity_seq": {},
        "_context_acknowledged_sources": set(),
        "_cycle_attempts": {},
        "_cycle_response_sources": {},
        "_source_coordination_modes": {},
        "_open_reply_recipients": {},
        "_queued_events": {},
        "_active_dispatch_rooms": {},
        "_event_dispatch_generation": {},
        "_receive_locks": {},
        "_ledger_locks": {},
        "_terminal_sources": {},
        "_terminal_results": {},
        "_attempt_renewal_tasks": {},
        "_renewing_rooms": set(),
    }.items():
        setattr(instance, name, value)

    effects = {
        "model": 0,
        "post": 0,
        "fallback": 0,
        "claim": 0,
        "turn": 0,
        "activity": 0,
        "dispatch": 0,
    }
    calls = {"room_state": 0, "acknowledge": 0}

    def forbidden(kind):
        async def fail(*_args, **_kwargs):
            effects[kind] += 1
            raise AssertionError(f"unexpected {kind} side effect")

        return fail

    class API:
        def room_state(self, room_id):
            calls["room_state"] += 1
            if room_id != binding.room_id:
                raise AssertionError("receive path changed Room binding")
            fixture = load_fixture()
            return {
                "activeEpoch": {
                    "id": fixture["activeEpochId"],
                    "startsAtSeq": fixture["activeEpochStartsAtSeq"],
                }
            }

        def acknowledge(self, _room_id, seq):
            calls["acknowledge"] += 1
            return {"acknowledgedSeq": seq}

        def __getattr__(self, name):
            if "activity" in name:
                kind = "activity"
            elif "claim" in name or "attempt" in name:
                kind = "claim"
            elif "turn" in name:
                kind = "turn"
            elif "message" in name or "post" in name:
                kind = "post"
            else:
                kind = "fallback"

            def fail(*_args, **_kwargs):
                effects[kind] += 1
                raise AssertionError(f"unexpected protocol operation {name}")

            return fail

    api = API()

    async def call(_binding, operation):
        result = operation(api)
        return await result if hasattr(result, "__await__") else result

    instance._call = call
    instance.handle_message = forbidden("model")
    instance.send = forbidden("fallback")
    instance._publish = forbidden("activity")
    instance._dispatch_next_queued = forbidden("dispatch")
    if path is None:
        instance._persist_binding = lambda _binding: True
        update_patch = mock.patch.object(adapter, "update", wraps=adapter.update)
    else:
        instance._persist_binding = types.MethodType(
            adapter.SyntheticSocialityAdapter._persist_binding,
            instance,
        )
        update_patch = mock.patch.object(
            adapter,
            "update",
            side_effect=lambda mutator: state_store.update(mutator, path),
        )
    return instance, effects, calls, update_patch


class PriorEpochSystemTerminalTests(unittest.TestCase):
    def test_system_human_interrupted_terminal_is_durable_idempotent_and_continues(self):
        async def run():
            fixture = load_fixture()
            self.assertEqual([event["seq"] for event in fixture["events"]], [838, 839, 840])
            old_terminal = fixture["events"][1]
            self.assertEqual(
                (
                    old_terminal["actorRole"],
                    old_terminal["payload"]["state"],
                    old_terminal["payload"]["reason"],
                ),
                ("system", "interrupted", "human_interrupted"),
            )
            self.assertEqual(old_terminal["payload"]["epochId"], "ssftrri7y8uioxl")

            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "state.json"
                binding = make_binding()
                state_store.save(adapter.PluginState(bindings=[binding]), path)
                instance, effects, calls, update_patch = make_instance(binding, path)
                with update_patch:
                    await instance._consume(binding, copy.deepcopy(fixture["events"][0]))
                    await instance._consume(binding, copy.deepcopy(old_terminal))

                durable = state_store.load(path).binding(binding.room_id)
                self.assertIsNotNone(durable)
                self.assertEqual(durable.acknowledged_cursor, 836)
                self.assertEqual(durable.inbox["837"], "quarantined")
                self.assertEqual(durable.inbox["839"], "ignored")
                self.assertEqual(
                    durable.terminal_evidence["839"],
                    {
                        "status": "ignored",
                        "sourceEventId": "5k207wbhmkomklv",
                        "canonicalEventId": "",
                        "canonicalSeq": 0,
                        "canonicalTs": "",
                        "reason": "prior_epoch_lifecycle",
                    },
                )

                restarted, restart_effects, restart_calls, restart_patch = make_instance(durable, path)
                with restart_patch:
                    await restarted._consume(durable, copy.deepcopy(old_terminal))
                    await restarted._consume(durable, copy.deepcopy(fixture["events"][2]))

                reloaded = state_store.load(path).binding(binding.room_id)
                self.assertIsNotNone(reloaded)
                self.assertEqual(reloaded.acknowledged_cursor, 836)
                self.assertEqual(reloaded.inbox["837"], "quarantined")
                self.assertEqual(reloaded.inbox["839"], "ignored")
                self.assertEqual(reloaded.inbox["840"], "ignored")
                self.assertEqual(
                    reloaded.terminal_evidence["840"]["sourceEventId"],
                    "zwvepi1rjrhr38t",
                )
                self.assertEqual(reloaded.terminal_evidence["840"]["reason"], "ineligible_event_type")
                self.assertEqual(calls["acknowledge"] + restart_calls["acknowledge"], 0)
                self.assertEqual(effects, {key: 0 for key in effects})
                self.assertEqual(restart_effects, {key: 0 for key in restart_effects})
                self.assertEqual(old_terminal["payload"]["epochId"], "ssftrri7y8uioxl")

        asyncio.run(run())

    def test_old_epoch_conversational_malformed_and_unknown_events_still_fail_closed(self):
        fixture = load_fixture()
        terminal = fixture["events"][1]
        controls = {
            "conversational_message": {
                "id": "old-message",
                "seq": 839,
                "type": "message.posted",
                "actorId": "peer",
                "actorRole": "participant_agent",
                "payload": {"epochId": "ssftrri7y8uioxl", "body": "must not dispatch"},
            },
            "attempt_ready": {
                "id": "old-attempt",
                "seq": 839,
                "type": "discussion.cycle_attempt_ready",
                "actorId": "room_coordinator",
                "actorRole": "system",
                "payload": {
                    "epochId": "ssftrri7y8uioxl",
                    "cycleId": "old-cycle",
                    "membershipId": "membership-test",
                },
            },
            "missing_cycle_id": {
                **terminal,
                "id": "missing-cycle",
                "payload": {**terminal["payload"], "cycleId": ""},
            },
            "unknown_tuple": {
                **terminal,
                "id": "unknown-tuple",
                "payload": {**terminal["payload"], "reason": "arbitrary_reason"},
            },
            "unreviewed_actor": {
                **terminal,
                "id": "unreviewed-actor",
                "actorRole": "participant_agent",
            },
            "contradictory_epoch": {
                **terminal,
                "id": "contradictory-epoch",
                "payload": {
                    **terminal["payload"],
                    "summaryHandoff": {"epochId": "another-epoch"},
                },
            },
            "malformed_epoch": {
                **terminal,
                "id": "malformed-epoch",
                "payload": {**terminal["payload"], "epochId": " \t"},
            },
            "future_conversational_epoch": {
                "id": "future-message",
                "seq": 839,
                "type": "message.posted",
                "actorId": "peer",
                "actorRole": "participant_agent",
                "payload": {"epochId": "future-epoch", "body": "must not dispatch"},
            },
        }

        for name, event in controls.items():
            with self.subTest(name=name):
                binding = adapter.RoomBinding(
                    "https://room.example/api",
                    "bjqiu9u7dwqptlq",
                    "membership-test",
                    "credential-test",
                    installation_id="installation-test",
                    cursor=839,
                    acknowledged_cursor=837,
                )
                instance, effects, calls, update_patch = make_instance(binding)
                with update_patch, self.assertRaises(adapter.ProtocolError) as raised:
                    asyncio.run(instance._consume(binding, copy.deepcopy(event)))
                self.assertIn(
                    raised.exception.code,
                    {"epoch_boundary_contradiction", "epoch_evidence_contradictory", "epoch_evidence_malformed"},
                )
                self.assertEqual(binding.acknowledged_cursor, 837)
                self.assertEqual(calls["acknowledge"], 0)
                self.assertEqual(effects, {key: 0 for key in effects})


if __name__ == "__main__":
    unittest.main()
