"""Hermes 1.0.116: retained terminal delivery intents do not block credential renewal.

recovery.prune_terminal_intents keeps the newest 32 terminal intents per binding
(contract P3-2), so renewal.quiescent must not count them as unfinished work.
"""
from __future__ import annotations

import asyncio
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from support import load_module

renewal = load_module("renewal")
state = load_module("state")
recovery = load_module("recovery")


RECEIPT = {"id": "evt-1", "seq": 5, "ts": "2026-11-24T19:50:00Z"}
POSTED = {"state": "posted", "delivery_state": "posted", "lifecycle_state": "complete", "canonical_event": RECEIPT}
POSTED_NOT_REQUIRED = {"state": "posted", "delivery_state": "posted", "lifecycle_state": "not_required",
                       "canonical_event": RECEIPT}
SKIPPED = {"state": "skipped", "delivery_state": "skipped"}
SUPERSEDED = {"state": "superseded", "delivery_state": "superseded"}
QUARANTINED = {"state": "quarantined", "delivery_state": "quarantined", "lifecycle_state": "not_started"}
SELECTED = {"state": "selected", "delivery_state": "selected"}
POSTED_PENDING = {"state": "posted", "delivery_state": "posted", "lifecycle_state": "pending", "canonical_event": RECEIPT}
POSTED_BLOCKED = {"state": "posted", "delivery_state": "posted", "lifecycle_state": "blocked", "canonical_event": RECEIPT}


def binding(**fields):
    item = state.RoomBinding("https://room.example/api", "room", "member", "base", installation_id="instance")
    for key, value in fields.items():
        setattr(item, key, value)
    return item


class QuiescentTerminalIntentTests(unittest.TestCase):
    def test_terminal_intents_are_quiescent(self):
        for name, intent in (("posted/complete", POSTED), ("posted/not_required", POSTED_NOT_REQUIRED),
                             ("skipped", SKIPPED), ("superseded", SUPERSEDED)):
            with self.subTest(name):
                self.assertTrue(recovery.is_terminal_intent(intent))
                self.assertTrue(renewal.quiescent(binding(delivery_intents={"i1": dict(intent)})))

    def test_retained_terminal_history_with_quarantine_is_quiescent(self):
        intents = {f"t{n}": dict((POSTED, SKIPPED, SUPERSEDED)[n % 3]) for n in range(32)}
        intents["q"] = dict(QUARANTINED)
        self.assertTrue(renewal.quiescent(binding(delivery_intents=intents)))

    def test_negative_controls(self):
        self.assertTrue(renewal.quiescent(binding()))
        self.assertTrue(renewal.quiescent(binding(delivery_intents={"q": dict(QUARANTINED)})))
        for name, item in (
                ("selected in flight", binding(delivery_intents={"s": dict(SELECTED)})),
                ("selected beside terminal", binding(delivery_intents={"t": dict(POSTED), "s": dict(SELECTED)})),
                ("lifecycle pending", binding(delivery_intents={"p": dict(POSTED_PENDING)})),
                ("lifecycle blocked", binding(delivery_intents={"p": dict(POSTED_BLOCKED)})),
                ("delivery_lifecycle", binding(delivery_intents={"t": dict(POSTED)},
                                               delivery_lifecycle={"t": {"state": "pending"}})),
                ("inbox pending", binding(delivery_intents={"t": dict(POSTED)}, inbox={"12": "pending"})),
                ("pending_since", binding(pending_since={"12": "2026-11-24T00:00:00Z"}, inbox={"12": "posted"})),
        ):
            with self.subTest(name):
                self.assertFalse(renewal.quiescent(item))

    def test_revoked_or_ineligible_still_blocks(self):
        terminal = {"t": dict(POSTED)}
        self.assertTrue(renewal.eligible(binding(delivery_intents=terminal)))
        self.assertFalse(renewal.eligible(binding(delivery_intents=terminal, revoked=True)))
        self.assertFalse(renewal.eligible(binding(delivery_intents=terminal, installation_id="")))
        self.assertFalse(renewal.eligible(binding(delivery_intents=terminal, enabled=False, disabled_reason="owner")))


class ResumeTerminalIntentTests(unittest.TestCase):
    def test_resume_and_owner_path_accept_terminal_history(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            item = binding(delivery_intents={"t": dict(POSTED), "s": dict(SKIPPED), "u": dict(SUPERSEDED)},
                           credential_rotation={"owner_initiated": True})
            state.save(state.PluginState(bindings=[item]), path)
            cli = load_module("cli")
            with mock.patch.object(cli, "_renew", return_value=0) as renew:
                renewal.resume(path, "room", None)
            renew.assert_called_once()

    def test_resume_still_refuses_in_flight_intent(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            item = binding(delivery_intents={"t": dict(POSTED), "s": dict(SELECTED)},
                           credential_rotation={"owner_initiated": True})
            state.save(state.PluginState(bindings=[item]), path)
            cli = load_module("cli")
            with mock.patch.object(cli, "_renew", return_value=0) as renew, \
                    self.assertRaises(ValueError):
                renewal.resume(path, "room", None)
            renew.assert_not_called()


class MaintainRenewalTerminalIntentTests(unittest.IsolatedAsyncioTestCase):
    async def maintain(self, intents):
        from test_delivery_lifecycle import load_adapter
        adapter = load_adapter()
        adapter_state = __import__(adapter.__package__ + ".state", fromlist=["save"])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            item = adapter.RoomBinding("https://room.example/api", "room", "member", "base",
                                       installation_id="instance", delivery_intents=intents)
            adapter_state.save(adapter.PluginState(bindings=[item]), path)
            with mock.patch.object(adapter, "load", lambda *_: adapter_state.load(path)):
                instance = adapter.SyntheticSocialityAdapter(adapter.PlatformConfig())
            instance._state_file = path
            instance._membership_active = mock.AsyncMock(return_value=True)
            with mock.patch.object(adapter.renewal, "discover", return_value={"requestId": "r"}) as discover, \
                    mock.patch.object(adapter.renewal, "resume") as resume, \
                    mock.patch.object(adapter, "load", lambda *_: adapter_state.load(path)):
                await instance._maintain_renewal(item)
            return discover, resume

    async def test_discovery_runs_with_terminal_intents(self):
        for name, intent in (("posted", POSTED), ("skipped", SKIPPED), ("superseded", SUPERSEDED)):
            with self.subTest(name):
                discover, resume = await self.maintain({"i1": dict(intent)})
                discover.assert_called_once()
                resume.assert_called_once()

    async def test_discovery_skipped_with_in_flight_intent(self):
        discover, resume = await self.maintain({"t": dict(POSTED), "s": dict(SELECTED)})
        discover.assert_not_called()
        resume.assert_not_called()


if __name__ == "__main__":
    unittest.main()
