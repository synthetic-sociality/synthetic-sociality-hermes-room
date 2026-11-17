"""Hermes 1.0.113: contract P2c-3d, a membership stopped by the Room.

The universal rule is the contract paragraph P2c-3d, byte-identical in OpenClaw;
the shared case names are ``conformance.json`` ``parityP2c3dCases`` (SBR-01 to
SBR-04), the same four strings in both connectors. The fault (17 Nov 2026):
two OpenClaw memberships in the closed Room "Alibaba", whose credentials
expired on 9 Nov, reported ``connecting`` with no code while they waited for a
renewal the Room owner never approves.

Every test runs a real ``SyntheticSocialityAdapter`` on a real state file and a
real ``LocalStatus`` in a throwaway directory; only the Room is a fake (the
P7a ``FakeRoom``, per membership). Each status write is recorded, so a
transient ``connecting`` or ``reconnecting`` cannot hide between two reads.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from test_delivery_lifecycle import ROOT, adapter
from test_parity_p7a import REAL_SLEEP, FakeRoom, edge_error, http_error

CONFORMANCE = json.loads((ROOT / "conformance.json").read_text(encoding="utf-8"))
CONTRACT = (ROOT / "docs" / "delivery-lifecycle-contract-v1.md").read_text(encoding="utf-8")
local_status = adapter.local_status
adapter_state = __import__(adapter.__package__ + ".state", fromlist=["save"])

STOPPED_BY_ROOM_CASES = [
    "SBR-01 a credential_expired answer reports the membership as stopped with lastErrorCode credential_expired",
    "SBR-02 a credential_revoked answer reports the membership as stopped with lastErrorCode credential_revoked",
    "SBR-03 while it waits for a renewal or re-admission the membership is never reported connecting or reconnecting and keeps its code",
    "SBR-04 a successful renewal returns the membership to the normal path; a sibling membership's entry is never changed",
]
PARAGRAPH = """\
P2c-3d Stopped by the Room (2026-11-17). When the Room answers `credential_expired` or `credential_revoked` for a
membership, the connector reports that membership in its local status (P2c-4) with connection `stopped` and that code
as `lastErrorCode`, from the first such answer on and for as long as the membership waits for a renewal or a
re-admission (P18). It never reports such a membership as `connecting` or `reconnecting`, and it never clears the
code while the credential stays unusable. A renewal or re-admission that succeeds returns the membership to the normal
path, and only then does its connection state change. The per-Room summary follows the membership entries (P2c-4).
"""
PARAGRAPH_SHA256 = "ecd00b4ed1f0e1cc8045ca9fa786e97a3ab678ecc28ccb674ce022fcb991804d"
HANG_GUARD = 20.0
TARGET_ROOM, TARGET = "room-1", "member-1"
SIBLING_ROOM, SIBLING = "room-2", "member-2"


def room_answer(code: str) -> adapter.ProtocolError:
    """The ProtocolError the real client raises for the Room's 401 answer with ``code``."""
    return edge_error(401, json.dumps({"code": code, "message": "refused"}).encode())


class MemberRoom(FakeRoom):
    """The P7a fake Room for one membership."""

    def __init__(self, membership_id: str):
        super().__init__()
        self.membership_id = membership_id

    def room_state(self, _room):
        with self.lock:
            self.state_reads += 1
            return {"roster": [{"membershipId": self.membership_id, "status": "active"}],
                    "activeEpoch": {"id": "epoch-1", "startsAtSeq": 1}}


def stored(room_id: str, membership_id: str, **fields) -> adapter.RoomBinding:
    return adapter.RoomBinding("https://room.example/api", room_id, membership_id, f"credential-{membership_id}",
                               installation_id=f"inst-{membership_id}", **fields)


def unstamped(entry: dict) -> dict:
    return {key: value for key, value in entry.items() if not key.endswith("At")}


class Gateway:
    """One real adapter on a state file in ``directory``; the Room is a fake per membership."""

    def __init__(self, directory: str, bindings: list[adapter.RoomBinding], *, save: bool = True):
        self.path = Path(directory) / "state.json"
        self.status_path = self.path.parent / "status.json"
        if save:
            adapter_state.save(adapter.PluginState(bindings=bindings), self.path)
        self.rooms = {TARGET_ROOM: MemberRoom(TARGET), SIBLING_ROOM: MemberRoom(SIBLING)}
        # Every status write, in order: (membership, connection, error_code), and markers.
        self.writes: list[tuple] = []
        with self.scope():
            self.instance = adapter.SyntheticSocialityAdapter(adapter.PlatformConfig())
        instance = self.instance
        instance._state_file = self.path
        status_room = local_status.LocalStatus.room
        writes = self.writes

        def recorded(status, room_id, membership_id="", **fields):
            writes.append((membership_id, fields.get("connection"), fields.get("error_code")))
            return status_room(status, room_id, membership_id, **fields)

        instance._status.room = recorded.__get__(instance._status)

        def noop(*_args, **_kwargs):
            return REAL_SLEEP(0)

        instance._refresh_binding = lambda _binding: REAL_SLEEP(0, True)
        instance._expire_stale_pending = noop
        instance._ensure_epoch_session_routing = noop
        instance._repair_pending_lifecycles = noop
        instance._publish = noop
        instance._persist_binding = lambda _binding: True
        instance._mark_connected = lambda: None
        instance._mark_disconnected = lambda: None

        def room_of(binding):
            return self.rooms[binding.room_id]

        instance._call = lambda binding, operation, **_options: asyncio.to_thread(operation, room_of(binding))
        instance._raw_call = instance._call
        instance._stream_call = lambda binding, operation: asyncio.to_thread(operation, room_of(binding))

    @contextlib.contextmanager
    def scope(self):
        with adapter_state.state_path_scope(self.path), \
                mock.patch.object(adapter.secret_isolation, "store_reachability", lambda *_a, **_k: []):
            yield

    def live(self, room_id: str) -> adapter.RoomBinding:
        return self.instance._state.binding(room_id)

    def stored(self, room_id: str) -> adapter.RoomBinding:
        return adapter_state.load(self.path).binding(room_id)

    def watch(self, binding: adapter.RoomBinding) -> asyncio.Task:
        task = asyncio.create_task(self.instance._watch(binding, is_reconnect=False))
        self.instance._tasks[binding.room_id] = task
        return task

    def entries(self) -> dict:
        return local_status.read(self.status_path).get("memberships", {})

    def entry(self, membership_id: str) -> dict:
        return self.entries()[membership_id]

    def summary(self, room_id: str) -> dict:
        return local_status.read(self.status_path)["rooms"][room_id]

    def of(self, membership_id: str, since: int = 0) -> list[tuple]:
        return [write for write in self.writes[since:] if write and write[0] == membership_id]

    async def until(self, condition, timeout: float = HANG_GUARD) -> bool:
        deadline = time.monotonic() + timeout
        while not condition() and time.monotonic() < deadline:
            await REAL_SLEEP(0.01)
        return bool(condition())

    async def stop(self) -> None:
        instance = self.instance
        instance._stop.set()
        pending = [*instance._tasks.values(), *instance._heartbeat_tasks.values(),
                   *instance._room_map("_enrollment_tasks").values()]
        if instance._renewal_task is not None:
            pending.append(instance._renewal_task)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)


def quiet():
    """The Room fake's sleeps compressed; logs muted (they carry no secret, but are noise here)."""
    stack = contextlib.ExitStack()
    stack.enter_context(mock.patch.object(asyncio, "sleep", lambda delay, result=None: REAL_SLEEP(
        0.002 if delay > 0 else 0, result)))
    stack.enter_context(mock.patch.object(adapter, "logger", mock.Mock()))
    return stack


async def stopped_by(test: unittest.TestCase, gateway: Gateway, code: str, contact: str) -> int:
    """Run the target's and the sibling's watch until the Room's ``code`` answer
    on ``contact`` stopped the target; return the index of the first write after it."""
    target, sibling = gateway.live(TARGET_ROOM), gateway.live(SIBLING_ROOM)
    room = gateway.rooms[TARGET_ROOM]
    if contact == "register":
        room.register_errors = [room_answer(code)]
    else:
        room.heartbeat_errors = [room_answer(code)]
        room.register_connector = lambda *args, original=room.register_connector: {
            **original(*args), "heartbeatIntervalSeconds": 0}
    sibling_task = gateway.watch(sibling)
    target_task = gateway.watch(target)
    test.assertTrue(await gateway.until(lambda: not target.enabled), f"{code} on {contact}: not stopped")
    test.assertTrue(await gateway.until(lambda: target_task.done()), "the target's watch ended")
    test.assertTrue(await gateway.until(lambda: SIBLING in gateway.entries()
                                        and gateway.entry(SIBLING)["connection"] == "connected"))
    test.assertFalse(sibling_task.done(), "the sibling's watch keeps running")
    first = next(index for index, write in enumerate(gateway.writes)
                 if write[0] == TARGET and write[2] == code)
    return first


class StoppedByRoomConformanceTests(unittest.TestCase):
    def test_the_paragraph_follows_p2c_3c_byte_identical(self):
        self.assertEqual(hashlib.sha256(PARAGRAPH.encode("utf-8")).hexdigest(), PARAGRAPH_SHA256)
        lines = CONTRACT.split("\n")
        start = next(index for index, line in enumerate(lines) if line.startswith("P2c-3c Attempt renewal retry "))
        end = next(index for index in range(start, len(lines)) if not lines[index])
        self.assertTrue(lines[end - 1].endswith("whatever the host run still does."))
        block = PARAGRAPH.split("\n")[:-1]
        self.assertEqual(lines[end + 1:end + 1 + len(block)], block)
        self.assertEqual(lines[end + 1 + len(block)], "")
        self.assertTrue(lines[end + 2 + len(block)].startswith("P2c-4 Local status. "))
        self.assertEqual(CONTRACT.count("P2c-3d Stopped by the Room"), 1)

    def test_the_case_names_are_the_shared_ones_and_each_has_a_test(self):
        self.assertEqual(CONFORMANCE["parityP2c3dCases"], STOPPED_BY_ROOM_CASES)
        source = Path(__file__).read_text(encoding="utf-8")
        for index, name in enumerate(STOPPED_BY_ROOM_CASES, start=1):
            self.assertTrue(name.startswith(f"SBR-{index:02d} "), name)
            self.assertGreaterEqual(source.count(f"    def test_sbr_{index:02d}_"), 1, name)

    def test_the_contract_hash_and_the_p2c_section_pin_are_the_pinned_ones(self):
        digest = hashlib.sha256((ROOT / "docs" / "delivery-lifecycle-contract-v1.md").read_bytes()).hexdigest()
        self.assertEqual(CONFORMANCE["contractSha256"], digest)
        self.assertIn("P2c", CONFORMANCE["parityContractSectionSha256"])


class StoppedByRoomTests(unittest.TestCase):
    def check_stopped_answer(self, code: str) -> None:
        for contact in ("register", "heartbeat"):
            with self.subTest(contact=contact), tempfile.TemporaryDirectory() as directory, quiet():
                gateway = Gateway(directory, [stored(TARGET_ROOM, TARGET), stored(SIBLING_ROOM, SIBLING)])

                async def run():
                    try:
                        return await stopped_by(self, gateway, code, contact)
                    finally:
                        await gateway.stop()

                with gateway.scope():
                    first = asyncio.run(run())
                entry = gateway.entry(TARGET)
                self.assertEqual((entry["connection"], entry["lastErrorCode"]), ("stopped", code))
                # The per-Room summary follows the membership entry.
                summary = gateway.summary(TARGET_ROOM)
                self.assertEqual((summary["connection"], summary["lastErrorCode"]), ("stopped", code))
                # From the first such answer on: no write moves it off stopped or clears the code.
                for _member, connection, error_code in gateway.of(TARGET, first):
                    self.assertIn(connection, (None, "stopped"), gateway.of(TARGET))
                    self.assertIn(error_code, (None, code), gateway.of(TARGET))
                self.assertEqual(gateway.entry(SIBLING)["connection"], "connected")
                self.assertEqual(gateway.entry(SIBLING)["lastErrorCode"], "")
                binding = gateway.stored(TARGET_ROOM)
                self.assertFalse(binding.enabled)
                self.assertEqual(binding.disabled_reason, code)
                self.assertEqual(binding.revoked, code == "credential_revoked")
                self.assertTrue(gateway.stored(SIBLING_ROOM).enabled)

    def test_sbr_01_a_credential_expired_answer_reports_the_membership_as_stopped(self):
        self.check_stopped_answer("credential_expired")

    def test_sbr_02_a_credential_revoked_answer_reports_the_membership_as_stopped(self):
        self.check_stopped_answer("credential_revoked")

    def test_sbr_03_maintenance_passes_while_waiting_for_renewal_keep_stopped_and_the_code(self):
        # The owner never approves (the closed Room "Alibaba"): discovery finds
        # no intent, the Room answers credential_expired, or the route fails.
        answers = [None, room_answer("credential_expired"), None, http_error(503), None, ValueError("<html>")]
        with tempfile.TemporaryDirectory() as directory, quiet():
            gateway = Gateway(directory, [stored(TARGET_ROOM, TARGET), stored(SIBLING_ROOM, SIBLING)])
            discovered = []

            def discover(binding):
                # The single passes get the scripted answers; the loop's passes find no intent
                # (an error answer holds the next pass back for 60 s, P8-3).
                discovered.append(binding.room_id)
                answer = answers[len(discovered) - 1] if len(discovered) <= len(answers) else None
                if isinstance(answer, Exception):
                    raise answer
                return answer

            async def run():
                try:
                    first = await stopped_by(self, gateway, "credential_expired", "register")
                    target = gateway.live(TARGET_ROOM)
                    self.assertTrue(adapter.renewal.eligible(gateway.stored(TARGET_ROOM)))
                    # Single maintenance passes, then the gateway's own maintenance loop.
                    for _ in range(len(answers)):
                        gateway.instance._renewal_retry_after.clear()
                        with contextlib.suppress(Exception):
                            await gateway.instance._maintain_renewal(target)
                    gateway.instance._renewal_retry_after.clear()
                    gateway.instance._renewal_task = asyncio.create_task(gateway.instance._renewal_loop())
                    with mock.patch.object(adapter, "RENEWAL_TICK_SECONDS", 0):
                        self.assertTrue(await gateway.until(lambda: len(discovered) >= 2 * len(answers)))
                    self.assertNotIn(TARGET_ROOM, gateway.instance._tasks, "no watch restarts while it waits")
                    return first
                finally:
                    await gateway.stop()

            with gateway.scope(), mock.patch.object(adapter.renewal, "discover", discover), \
                    mock.patch.object(adapter.renewal, "resume") as resume:
                first = asyncio.run(run())
            resume.assert_not_called()
            self.assertGreaterEqual(discovered.count(TARGET_ROOM), 2 * len(answers))
            for _member, connection, error_code in gateway.of(TARGET, first):
                self.assertIn(connection, (None, "stopped"), gateway.of(TARGET))
                self.assertIn(error_code, (None, "credential_expired"), gateway.of(TARGET))
            entry = gateway.entry(TARGET)
            self.assertEqual((entry["connection"], entry["lastErrorCode"]), ("stopped", "credential_expired"))

    def test_sbr_03_a_restarted_gateway_reports_a_waiting_membership_as_stopped_with_its_code(self):
        # The Alibaba memberships waited from 9 Nov: across gateway restarts.
        for code, fields in (
            ("credential_expired", dict(enabled=False, disabled_reason="credential_expired")),
            ("credential_revoked", dict(enabled=False, revoked=True, disabled_reason="credential_revoked")),
        ):
            with self.subTest(code=code), tempfile.TemporaryDirectory() as directory, quiet():
                gateway = Gateway(directory, [stored(TARGET_ROOM, TARGET, **fields), stored(SIBLING_ROOM, SIBLING)])

                async def run():
                    try:
                        self.assertTrue(await gateway.instance.connect())
                        self.assertTrue(await gateway.until(
                            lambda: SIBLING in gateway.entries()
                            and gateway.entry(SIBLING)["connection"] == "connected"))
                        await REAL_SLEEP(0.05)
                        self.assertNotIn(TARGET_ROOM, gateway.instance._tasks)
                    finally:
                        await gateway.stop()

                with gateway.scope(), mock.patch.object(adapter.renewal, "discover", return_value=None), \
                        mock.patch.object(adapter, "RENEWAL_TICK_SECONDS", 0):
                    asyncio.run(run())
                self.assertIn(TARGET, gateway.entries(), "the waiting membership is reported")
                entry = gateway.entry(TARGET)
                self.assertEqual((entry["connection"], entry["lastErrorCode"]), ("stopped", code))
                summary = gateway.summary(TARGET_ROOM)
                self.assertEqual((summary["connection"], summary["lastErrorCode"]), ("stopped", code))
                for _member, connection, error_code in gateway.of(TARGET):
                    self.assertIn(connection, (None, "stopped"), gateway.of(TARGET))
                    self.assertIn(error_code, (None, code), gateway.of(TARGET))
                self.assertEqual(gateway.entry(SIBLING)["lastErrorCode"], "")

    def test_sbr_04_a_successful_renewal_returns_the_membership_to_the_normal_path(self):
        with tempfile.TemporaryDirectory() as directory, quiet():
            gateway = Gateway(directory, [stored(TARGET_ROOM, TARGET), stored(SIBLING_ROOM, SIBLING)])
            observed = {}

            def resume(path, room_id, intent):
                # The owner approved: the journaled CLI engine stores the new credential.
                self.assertEqual((path, room_id, intent), (gateway.path, TARGET_ROOM, {"requestId": "intent-1"}))
                observed["before"] = gateway.entry(TARGET)
                observed["sibling"] = unstamped(gateway.entry(SIBLING))
                gateway.writes.append(("renewed", None, None))

                def renewed(latest):
                    binding = latest.binding(room_id)
                    binding.credential = "credential-renewed"
                    binding.enabled = True
                    binding.disabled_reason = ""
                    binding.credential_expires_at = "2099-01-01T00:00:00Z"
                adapter_state.update(renewed, path)

            async def run():
                try:
                    first = await stopped_by(self, gateway, "credential_expired", "register")
                    sibling_task = gateway.instance._tasks[SIBLING_ROOM]
                    observed["sibling-start"] = unstamped(gateway.entry(SIBLING))
                    registrations = gateway.rooms[TARGET_ROOM].registrations
                    await gateway.instance._maintain_renewal(gateway.live(TARGET_ROOM))
                    self.assertTrue(await gateway.until(
                        lambda: gateway.entry(TARGET)["connection"] == "connected"), gateway.of(TARGET))
                    self.assertGreater(gateway.rooms[TARGET_ROOM].registrations, registrations)
                    self.assertIs(gateway.instance._tasks[SIBLING_ROOM], sibling_task)
                    self.assertFalse(sibling_task.done())
                    return first
                finally:
                    await gateway.stop()

            with gateway.scope(), mock.patch.object(adapter.renewal, "discover", return_value={"requestId": "intent-1"}), \
                    mock.patch.object(adapter.renewal, "resume", resume):
                first = asyncio.run(run())
            # Until the renewal succeeded the membership stayed stopped with its code ...
            self.assertEqual((observed["before"]["connection"], observed["before"]["lastErrorCode"]),
                             ("stopped", "credential_expired"))
            marker = gateway.writes.index(("renewed", None, None))
            before = [write for write in gateway.writes[first:marker] if write[0] == TARGET]
            for _member, connection, error_code in before:
                self.assertIn(connection, (None, "stopped"), before)
                self.assertIn(error_code, (None, "credential_expired"), before)
            # ... and only then did its connection state change, on the normal path.
            after = [write for write in gateway.writes[marker:] if write[0] == TARGET]
            self.assertTrue(after, "the renewed membership is reported again")
            entry = gateway.entry(TARGET)
            self.assertEqual(entry["connection"], "connected")
            self.assertEqual(gateway.summary(TARGET_ROOM)["connection"], "connected")
            binding = gateway.stored(TARGET_ROOM)
            self.assertTrue(binding.enabled)
            self.assertEqual(binding.credential, "credential-renewed")
            # A sibling membership's entry is never changed by the stop, the waits or the renewal.
            self.assertEqual(observed["sibling"], observed["sibling-start"])
            self.assertEqual(unstamped(gateway.entry(SIBLING)), observed["sibling-start"])
            self.assertEqual(gateway.summary(SIBLING_ROOM)["membershipIds"], [SIBLING])
            self.assertEqual(gateway.stored(SIBLING_ROOM).credential, f"credential-{SIBLING}")


if __name__ == "__main__":
    unittest.main()
