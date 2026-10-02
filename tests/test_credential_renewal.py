from __future__ import annotations

import asyncio
import copy
from dataclasses import asdict
from pathlib import Path
import tempfile
import threading
import unittest
from unittest import mock

from support import load_module

state = load_module("state")
cli = load_module("cli")
renewal = load_module("renewal")
protocol = load_module("protocol")


class RenewalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "state.json"
        self.binding = state.RoomBinding(
            "https://room.example/api", "room-1", "member-1", "old-credential",
            display_name="Arbitrary agent", identity_version=3, installation_id="instance-1",
            credential_expires_at="2020-01-01T00:00:00Z", cursor=11, acknowledged_cursor=11,
        )
        self.sibling = state.RoomBinding("https://other.example/api", "room-2", "member-2", "other")
        state.save(state.PluginState(bindings=[self.binding, self.sibling]), self.path)
        self.intent = {"requestId": cli._new_renewal_secret(), "roomId": "room-1",
                       "membershipId": "member-1", "state": "owner_requested",
                       "displayName": "Arbitrary agent", "identityVersion": 3, "baseCredentialGeneration": 1}

    def fake_api(self, fail=""):
        testcase = self

        class API:
            failed = False
            confirmed = False
            redeemed = False
            replacement = ""
            grant_hash = ""
            calls = []

            def __init__(self, url, credential=""):
                self.credential = credential

            def boundary(self, name):
                API.calls.append(name)
                if fail == name and not API.failed:
                    API.failed = True
                    raise OSError("lost reply")

            def identity(self):
                return {"roomId": "room-1", "membershipId": "member-1", "displayName": "Arbitrary agent",
                        "identityVersion": 3, "deliveredSeq": 15, "acknowledgedSeq": 11,
                        "credentialExpiresAt": "2099-01-01T00:00:00Z", "confirmBy": "2099-01-01T00:00:00Z"}

            def room_state(self, room_id):
                testcase.assertEqual(room_id, "room-1")
                if API.confirmed:
                    testcase.assertEqual(self.credential, API.replacement)
                    self.boundary("active_state")
                else:
                    raise AssertionError("expired base must never use ordinary /state")
                return {"roomId": room_id, "roster": [self.identity()]}

            def request_credential_renewal(self, room_id, request_id, digest, *, client_instance_id=""):
                testcase.assertEqual(client_instance_id, "instance-1")
                testcase.assertEqual(request_id, testcase.intent["requestId"])
                current = state.load(testcase.path).binding(room_id)
                testcase.assertEqual(current.credential_rotation["request_id"], request_id)
                testcase.assertTrue(current.credential_rotation["replacement_credential"])
                if API.grant_hash:
                    testcase.assertEqual(API.grant_hash, digest)
                API.grant_hash = digest
                self.boundary("request")
                return self.credential_renewal_request(room_id, request_id)

            def credential_renewal_request(self, room_id, request_id):
                return {**self.identity(), "requestId": request_id, "grantId": "grant-1",
                        "baseCredentialGeneration": 1, "state": "redeemed" if API.redeemed else "issued"}

            def redeem_credential_renewal(self, room_id, grant_id, grant_secret, replacement):
                if API.replacement:
                    testcase.assertEqual(API.replacement, replacement)
                API.replacement = replacement
                API.redeemed = True
                self.boundary("redeem")
                return self.identity()

            def verify_credential_renewal(self, room_id, grant_id, replacement):
                testcase.assertEqual(API.replacement, replacement)
                self.boundary("verify")
                return self.identity()

            def confirm_credential_renewal(self, room_id, grant_id, replacement):
                testcase.assertEqual(state.load(testcase.path).binding(room_id).credential, replacement)
                testcase.assertEqual(API.replacement, replacement)
                API.confirmed = True
                self.boundary("confirm")
                return {"id": "event-1", "type": "credential.renewed", "payload": self.identity()}

        return API

    def run_renewal(self, api, intent):
        with mock.patch.object(cli, "RoomProtocol", api):
            renewal.resume(self.path, "room-1", intent)

    def test_valid_and_expired_disabled_bindings_preserve_identity_and_siblings(self):
        for disabled, expiry in ((False, "2099-01-01T00:00:00Z"), (False, "2020-01-01T00:00:00Z"),
                                 (True, "2020-01-01T00:00:00Z")):
            with self.subTest(disabled=disabled, expiry=expiry):
                binding = copy.deepcopy(self.binding)
                binding.credential_expires_at = expiry
                binding.enabled = not disabled
                binding.disabled_reason = "credential_expired" if disabled else ""
                state.save(state.PluginState(bindings=[binding, self.sibling]), self.path)
                api = self.fake_api()
                self.run_renewal(api, self.intent)
                saved = state.load(self.path)
                actual = saved.binding("room-1")
                self.assertEqual(saved.binding("room-2"), self.sibling)
                self.assertEqual(actual.credential_rotation, {})
                self.assertEqual(actual.credential, api.replacement)
                self.assertTrue(actual.enabled)
                self.assertEqual(actual.installation_id, binding.installation_id)
                self.assertEqual((actual.cursor, actual.acknowledged_cursor), (11, 11))
                self.assertEqual(actual.display_name, binding.display_name)
                self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)
                self.assertNotIn("old-credential", self.path.read_text())

    def test_lost_reply_at_each_phase_keeps_same_journal_and_replacement(self):
        for boundary in ("request", "redeem", "verify", "confirm", "active_state"):
            with self.subTest(boundary=boundary):
                state.save(state.PluginState(bindings=[self.binding, self.sibling]), self.path)
                api = self.fake_api(boundary)
                with self.assertRaises(OSError):
                    self.run_renewal(api, self.intent)
                journal = copy.deepcopy(state.load(self.path).binding("room-1").credential_rotation)
                self.assertTrue(journal)
                self.run_renewal(api, None)
                actual = state.load(self.path).binding("room-1")
                self.assertEqual(actual.credential, journal["replacement_credential"])
                self.assertEqual(actual.credential_rotation, {})
                self.assertTrue(api.confirmed)

    def test_manual_legacy_disabled_revoked_and_active_work_fail_closed(self):
        for changes in ({"enabled": False}, {"enabled": False, "disabled_reason": "manual"},
                        {"revoked": True}, {"inbox": {"12": "pending"}},
                        {"delivery_intents": {"source": {"pending": True}}}):
            binding = copy.deepcopy(self.binding)
            for key, value in changes.items():
                setattr(binding, key, value)
            state.save(state.PluginState(bindings=[binding]), self.path)
            before = self.path.read_bytes()
            with self.assertRaises(ValueError):
                self.run_renewal(self.fake_api(), self.intent)
            self.assertEqual(before, self.path.read_bytes())

    def test_quarantine_survives_renewal_and_lost_replies_without_replay_or_ack(self):
        for boundary in ("", "request", "redeem", "verify", "confirm", "active_state"):
            with self.subTest(boundary=boundary):
                binding = copy.deepcopy(self.binding)
                binding.inbox = {"12": "quarantined", "13": "ignored"}
                binding.pending_retries = {"12": 4}
                binding.pending_since = {"12": 123.0}
                binding.turn_sequences = {"old-source": 12}
                binding.delivery_intents = {"old-source": {
                    "state": "quarantined", "delivery_state": "quarantined",
                    "lifecycle_state": "not_started", "last_error": "retain evidence",
                }}
                state.save(state.PluginState(bindings=[binding, self.sibling]), self.path)
                protected = cli._renewal_protected_fields(binding)
                api = self.fake_api(boundary)
                if boundary:
                    with self.assertRaises(OSError):
                        self.run_renewal(api, self.intent)
                    self.run_renewal(api, None)
                else:
                    self.run_renewal(api, self.intent)
                saved = state.load(self.path)
                self.assertEqual(cli._renewal_protected_fields(saved.binding("room-1")), protected)
                self.assertEqual(saved.binding("room-2"), self.sibling)
                self.assertTrue(api.confirmed)
                self.assertEqual(saved.binding("room-1").credential_rotation, {})

    def test_crash_after_each_durable_phase_resumes_exact_generation(self):
        for phase in ("requesting_owner", "prepared", "redeemed", "swapped", "confirmed"):
            with self.subTest(phase=phase):
                state.save(state.PluginState(bindings=[self.binding]), self.path)
                api = self.fake_api()
                failed = False
                def write(mutator):
                    nonlocal failed
                    result = state.update(mutator)
                    journal = state.load(self.path).binding("room-1").credential_rotation
                    if not failed and journal.get("phase") == phase:
                        failed = True
                        raise OSError("crash after durable phase")
                    return result
                with mock.patch.object(cli, "update", write), self.assertRaises(OSError):
                    self.run_renewal(api, self.intent)
                self.run_renewal(api, None)
                self.assertEqual(state.load(self.path).binding("room-1").credential_rotation, {})

    def test_response_cannot_change_intent_frozen_identity(self):
        api = self.fake_api()
        original = api.credential_renewal_request
        def changed(client, room, request):
            return {**original(client, room, request), "identityVersion": 4}
        with mock.patch.object(api, "credential_renewal_request", changed), self.assertRaises(ValueError):
            self.run_renewal(api, self.intent)
        self.assertEqual(state.load(self.path).binding("room-1").credential, "old-credential")

    def test_expired_base_uses_renewal_only_snapshot_and_requires_explicit_counters(self):
        api = self.fake_api()
        original = api.credential_renewal_request
        def missing(client, room, request):
            result = original(client, room, request)
            result.pop("deliveredSeq")
            return result
        with mock.patch.object(api, "credential_renewal_request", missing), self.assertRaises(ValueError):
            self.run_renewal(api, self.intent)
        self.assertFalse(api.redeemed)
        self.run_renewal(api, None)
        self.assertTrue(api.confirmed)

    def test_wrong_intent_identity_does_not_create_journal(self):
        for key, value in (("roomId", "other"), ("membershipId", "other"), ("requestId", "bad")):
            intent = {**self.intent, key: value}
            with self.assertRaises(ValueError):
                self.run_renewal(self.fake_api(), intent)
            self.assertEqual(state.load(self.path).binding("room-1").credential_rotation, {})

    def test_concurrent_disable_or_protected_cursor_change_is_not_overwritten(self):
        for change in (lambda b: setattr(b, "cursor", 12), lambda b: setattr(b, "disabled_reason", "manual")):
            state.save(state.PluginState(bindings=[self.binding]), self.path)
            api = self.fake_api("request")
            with self.assertRaises(OSError):
                self.run_renewal(api, self.intent)
            state.update(lambda s: change(s.binding("room-1")), self.path)
            before = self.path.read_bytes()
            with self.assertRaises(ValueError):
                self.run_renewal(api, None)
            self.assertEqual(before, self.path.read_bytes())

    def test_exact_profile_scope_and_concurrent_runner_lock(self):
        other_path = Path(self.temp.name) / "other" / "state.json"
        with state.state_path_scope(other_path):
            self.run_renewal(self.fake_api(), self.intent)
            self.assertEqual(state.state_path(), other_path)
        self.assertFalse(other_path.exists())
        with state.state_path_scope(self.path), state.binding_renewal_lock("room-1"):
            with self.assertRaises(BlockingIOError):
                self.run_renewal(self.fake_api(), self.intent)

    def test_leave_cannot_destroy_recovery_journal(self):
        api = self.fake_api("confirm")
        with self.assertRaises(OSError):
            self.run_renewal(api, self.intent)
        before = self.path.read_bytes()
        with state.state_path_scope(self.path), self.assertRaises(ValueError):
            cli._leave("room-1")
        self.assertEqual(before, self.path.read_bytes())
        self.run_renewal(api, None)

    def test_optional_protocol_response_validation_and_exact_instance_wire(self):
        class API(protocol.RoomProtocol):
            result = {"request": None}
            def request(self, method, path, payload=None, **kwargs):
                self.last = (method, path, payload)
                return self.result
        api = API("https://room.example/api", "base")
        self.assertIsNone(api.credential_renewal_intent("room one", "instance/one"))
        self.assertEqual(api.last, ("GET", "/rooms/room%20one/credential-renewal-intent?clientInstanceId=instance%2Fone", None))
        api.request_credential_renewal("room", "request", "hash", client_instance_id="exact-instance")
        self.assertEqual(api.last[2]["clientInstanceId"], "exact-instance")
        for result in ("<html>", {}, {"request": []}, {"request": {"state": "confirmed"}}):
            api.result = result
            with self.assertRaises(protocol.ProtocolError):
                api.credential_renewal_intent("room", "instance")


class MaintenanceTests(unittest.IsolatedAsyncioTestCase):
    async def test_receive_never_implicitly_reopens_legacy_quarantine(self):
        from test_delivery_lifecycle import load_adapter
        adapter = load_adapter()
        instance = object.__new__(adapter.SyntheticSocialityAdapter)
        binding = adapter.RoomBinding("https://room.example/api", "room", "member", "base",
                                      cursor=4, acknowledged_cursor=4, inbox={"5": "quarantined"})
        binding.delivery_intents = {"old-source": {"state": "quarantined", "last_error_code": "cycle_conflict"}}
        before = copy.deepcopy(binding)
        instance._persist_binding = lambda _: True
        with mock.patch.object(instance, "_activate_legacy_canonical_receipt", side_effect=AssertionError("implicit migration")), \
                mock.patch.object(instance, "_activate_legacy_post_commit_recovery", side_effect=AssertionError("implicit replay")):
            await instance._consume(binding, {"id": "old-source", "seq": 5})
        self.assertEqual(binding, before)

    async def test_read_frontier_crosses_only_isolated_or_proven_entries_without_ack(self):
        from test_delivery_lifecycle import load_adapter
        adapter = load_adapter()
        instance = object.__new__(adapter.SyntheticSocialityAdapter)
        binding = adapter.RoomBinding("https://room.example/api", "room", "member", "base",
                                      cursor=4, acknowledged_cursor=4, inbox={"5": "quarantined"})
        # More than one server page of isolated/completed work must not starve
        # a new source. The canonical cursor and every retained record stay put.
        for seq in range(6, 210):
            binding.inbox[str(seq)] = "ignored"
            binding.terminal_evidence[str(seq)] = {"status": "ignored", "reason": "technical"}
        before = copy.deepcopy(binding)
        self.assertEqual(instance._read_frontier(binding), 209)
        reads = []
        class API:
            def events(self, room, cursor, wait, **kwargs):
                reads.append(cursor)
                return {"events": []}
        instance._call = lambda _binding, operation: asyncio.sleep(0, result=operation(API()))
        await instance._long_poll_once(binding)
        self.assertEqual(reads, [209])
        self.assertEqual(binding, before)
        binding.inbox["5"] = "ignored"
        binding.terminal_evidence["5"] = {"status": "ignored", "reason": "technical"}
        # A complete contiguous tail with a lost ack must be replayed through
        # the receipt-only completion path, not stranded behind the read scan.
        self.assertEqual(instance._read_frontier(binding), 4)
        binding.inbox["5"] = "quarantined"
        binding.terminal_evidence.pop("5")
        binding.inbox["7"] = "pending"
        self.assertEqual(instance._read_frontier(binding), 6)
        binding.inbox["7"] = "ignored"
        binding.terminal_evidence.pop("7")
        self.assertEqual(instance._read_frontier(binding), 6)

    async def test_maintenance_drains_only_target_and_hot_resumes_after_durable_success(self):
        from test_delivery_lifecycle import load_adapter
        adapter = load_adapter()
        adapter_state = __import__(adapter.__package__ + ".state", fromlist=["save"])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            target = adapter.RoomBinding("https://room.example/api", "room", "member", "base", installation_id="instance")
            target.inbox = {"5": "quarantined", "6": "ignored"}
            sibling = adapter.RoomBinding("https://room.example/api", "sibling", "other", "other-base", installation_id="other")
            adapter_state.save(adapter.PluginState(bindings=[target, sibling]), path)
            with mock.patch.object(adapter, "load", lambda *_: adapter_state.load(path)):
                instance = adapter.SyntheticSocialityAdapter(adapter.PlatformConfig())
            instance._state_file = path
            stopped = asyncio.Event()
            resumed = asyncio.Event()
            async def old_watch():
                try:
                    await asyncio.Event().wait()
                finally:
                    stopped.set()
                    instance._tasks.pop("room", None)
            old_task = asyncio.create_task(old_watch())
            sibling_task = asyncio.create_task(asyncio.Event().wait())
            instance._tasks = {"room": old_task, "sibling": sibling_task}
            await asyncio.sleep(0)
            def renew(exact_path, room_id, intent):
                self.assertTrue(stopped.is_set())
                self.assertEqual(exact_path, path)
                self.assertEqual(room_id, "room")
                self.assertFalse(sibling_task.done())
                self.assertIn("room", instance._renewing_rooms)
                adapter_state.update(lambda s: setattr(s.binding(room_id), "credential", "replacement"), path)
            async def watch(binding, **kwargs):
                self.assertEqual(binding.credential, "replacement")
                self.assertNotIn("room", instance._renewing_rooms)
                resumed.set()
            instance._watch = watch
            with mock.patch.object(adapter.renewal, "discover", return_value={"requestId": "test-intent"}), \
                    mock.patch.object(adapter.renewal, "resume", renew):
                await instance._maintain_renewal(target)
            await asyncio.wait_for(resumed.wait(), 1)
            self.assertIs(instance._tasks["sibling"], sibling_task)
            self.assertEqual(adapter_state.load(path).binding("sibling"), sibling)
            sibling_task.cancel()
            await asyncio.gather(sibling_task, instance._tasks["room"], return_exceptions=True)

    async def test_unsupported_discovery_and_pending_work_do_not_stop_watchers_or_leak_errors(self):
        from test_delivery_lifecycle import load_adapter
        adapter = load_adapter()
        adapter_state = __import__(adapter.__package__ + ".state", fromlist=["save"])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            binding = adapter.RoomBinding("https://room.example/api", "room", "member", "base", installation_id="instance")
            adapter_state.save(adapter.PluginState(bindings=[binding]), path)
            with mock.patch.object(adapter, "load", lambda *_: adapter_state.load(path)):
                instance = adapter.SyntheticSocialityAdapter(adapter.PlatformConfig())
            instance._state_file = path
            watcher = asyncio.create_task(asyncio.Event().wait())
            instance._tasks["room"] = watcher
            with mock.patch.object(adapter.renewal, "discover", side_effect=ValueError("<html>private-token</html>")), \
                    mock.patch.object(adapter.renewal, "resume") as resume, \
                    self.assertLogs(adapter.logger, level="WARNING") as logs:
                await instance._maintain_renewal(binding)
            self.assertFalse(watcher.done())
            self.assertNotIn("private-token", " ".join(logs.output))
            resume.assert_not_called()
            binding.inbox = {"12": "pending"}
            instance._renewal_retry_after.clear()
            with mock.patch.object(adapter.renewal, "discover") as discover:
                await instance._maintain_renewal(binding)
                discover.assert_not_called()
            self.assertFalse(watcher.done())
            watcher.cancel()
            await asyncio.gather(watcher, return_exceptions=True)

    async def test_cancellation_drains_inflight_worker(self):
        from test_delivery_lifecycle import load_adapter
        adapter = load_adapter()
        instance = object.__new__(adapter.SyntheticSocialityAdapter)
        binding = adapter.RoomBinding("https://room.example/api", "room", "member", "base")
        started, finished = threading.Event(), threading.Event()
        def operation(_api):
            started.set()
            finished.wait(2)
            return True
        task = asyncio.create_task(instance._call(binding, operation))
        await asyncio.to_thread(started.wait, 1)
        task.cancel()
        await asyncio.sleep(0.01)
        self.assertFalse(task.done())
        finished.set()
        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_expiry_only_profile_bootstraps_maintenance_without_room_watcher(self):
        from test_delivery_lifecycle import load_adapter
        adapter = load_adapter()
        binding = adapter.RoomBinding("https://room.example/api", "room", "member", "base",
                                      installation_id="instance", enabled=False, disabled_reason="credential_expired")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            adapter_state = __import__(adapter.__package__ + ".state", fromlist=["save"])
            adapter_state.save(adapter.PluginState(bindings=[binding]), path)
            # Contract P2c-1 start gate: this test is about maintenance, not the
            # host tool backend (P2c-01 covers the gate).
            with mock.patch.object(adapter, "load", lambda *_: adapter_state.load(path)), \
                    mock.patch.object(adapter.secret_isolation, "store_reachability", lambda *_a, **_k: []):
                self.assertTrue(adapter.validate_config(None))
                instance = adapter.SyntheticSocialityAdapter(adapter.PlatformConfig())
                instance._state_file = path
                called = asyncio.Event()
                async def maintain(_binding):
                    called.set()
                instance._maintain_renewal = maintain
                self.assertTrue(await instance.connect())
                await asyncio.wait_for(called.wait(), 1)
                self.assertEqual(instance._tasks, {})
                instance._stop.set()
                instance._renewal_task.cancel()
                await asyncio.gather(instance._renewal_task, return_exceptions=True)


if __name__ == "__main__":
    unittest.main()
