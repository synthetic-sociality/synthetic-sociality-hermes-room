"""Shared conformance cases for parity release P3.

Delivery contract section "P3 — Ledger, recovery and holes"; case names are
the keys of ``conformance.json`` ``parityP3Cases``; OpenClaw implements the
same names. Fixtures follow the two documented holes (Paula 379: a quarantined
cycle_superseded intent; Claude 383: an owner message with no local record).
No test uses a real credential, and no test talks to a live Room.
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import json
import os
import stat
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

from test_delivery_lifecycle import ROOT, adapter

CONFORMANCE = json.loads((ROOT / "conformance.json").read_text())
recovery = adapter.recovery
secret_isolation = adapter.secret_isolation
local_status = adapter.local_status
cli = adapter.cli
state_module = __import__(adapter.__name__.rsplit(".", 1)[0] + ".state", fromlist=["state"])

CYCLE = "cyb1cd6c68e7df2"
ATTEMPT_READY = {"id": "ev379", "seq": 379, "type": "discussion.cycle_attempt_ready", "epoch": "epoch-old",
                 "payload": {"cycleId": CYCLE, "generation": 1, "membershipId": "member-1"}}
OWNER_MESSAGE = {"id": "ev383", "seq": 383, "type": "message.posted", "epoch": "epoch-old",
                 "actorRole": "human_owner", "payload": {"body": "a message body that never reaches an audit record"}}


class ProofRoom:
    """The read-only Room routes a closure uses, plus the acknowledgement."""

    def __init__(self, *, active="epoch-new", cycle_state="interrupted", cycle_generation=1, events=None):
        self.active = active
        self.cycle = {"id": CYCLE, "state": cycle_state, "generation": cycle_generation}
        self.events = list([ATTEMPT_READY, OWNER_MESSAGE] if events is None else events)
        self.reads, self.acks = [], []

    def epochs(self, _room):
        self.reads.append("epochs")
        return {"epochs": [{"id": "epoch-old", "startsAtSeq": 300, "status": "closed" if self.active != "epoch-old" else "active"},
                           {"id": "epoch-new", "startsAtSeq": 500, "status": "active" if self.active == "epoch-new" else "closed"}]}

    def events_in_epoch(self, _room, after, epoch_id):
        self.reads.append(("events", after, epoch_id))
        return {"events": [dict(item) for item in self.events if item["seq"] > after and item["epoch"] == epoch_id][:10],
                "deliveredSeq": 990}

    def room_state(self, _room):
        self.reads.append("state")
        return {"activeEpoch": {"id": self.active, "startsAtSeq": 500}}

    def get_discussion_cycle(self, _room, cycle_id):
        self.reads.append(("cycle", cycle_id))
        return dict(self.cycle)

    def acknowledge(self, _room, seq):
        self.acks.append(seq)
        return {"acknowledgedSeq": seq}


def paula_binding():
    """Paula's shape: ack 378, 379 quarantined (cycle_superseded), evidence 380-382."""
    binding = adapter.RoomBinding("https://room.example/api", "room-1", "member-1", "credential",
                                  installation_id="inst-1", cursor=378, acknowledged_cursor=378)
    binding.inbox = {"379": "quarantined", "380": "ignored", "381": "ignored", "382": "ignored"}
    binding.terminal_evidence = {str(seq): {"status": "ignored", "sourceEventId": f"ev{seq}", "canonicalEventId": "",
                                            "canonicalSeq": 0, "canonicalTs": "", "reason": "not_addressed"}
                                 for seq in (380, 381, 382)}
    binding.delivery_intents = {"ev379": {
        "state": "quarantined", "delivery_state": "quarantined", "lifecycle_state": "not_started",
        "last_error_code": "cycle_superseded", "failed_at": 1.0,
        "selected": {"source_event_id": "ev379", "source_seq": 379, "state": "selected"},
    }}
    return binding


def claude_binding():
    """Claude's shape: ack 382, nothing for 383, evidence 384-385."""
    binding = adapter.RoomBinding("https://room.example/api", "room-1", "member-1", "credential",
                                  installation_id="inst-1", cursor=382, acknowledged_cursor=382)
    binding.inbox = {"384": "ignored", "385": "ignored"}
    binding.terminal_evidence = {str(seq): {"status": "ignored", "sourceEventId": f"ev{seq}", "canonicalEventId": "",
                                            "canonicalSeq": 0, "canonicalTs": "", "reason": "not_addressed"}
                                 for seq in (384, 385)}
    return binding


def ledger_instance(binding, api, state_file):
    instance = object.__new__(adapter.SyntheticSocialityAdapter)
    instance._state_file = Path(state_file)
    instance._ledger_locks = {}
    instance._status = local_status.LocalStatus(Path(state_file).parent / "status.json", plugin="hermes",
                                                version=adapter.CONNECTOR_VERSION, host_label="host-a")
    instance.persisted = []
    instance._persist_binding = lambda current: instance.persisted.append(json.loads(json.dumps(
        {"inbox": current.inbox, "ack": current.acknowledged_cursor, "intents": sorted(current.delivery_intents)}))) or True
    instance._call = lambda _binding, operation: asyncio.sleep(0, result=operation(api))
    return instance


@contextlib.contextmanager
def profile(binding):
    """A scratch HERMES_HOME holding exactly this binding."""
    with tempfile.TemporaryDirectory() as scratch:
        home = Path(scratch) / ".hermes"
        path = home / "synthetic-sociality-room" / "state.json"
        with mock.patch.dict(os.environ, {"HERMES_HOME": str(home)}):
            state_module.save(adapter.PluginState(bindings=[binding]), path)
            yield path


CORE_SELECTION = r"""
import json, sys
sys.path.insert(0, sys.argv[1])
from support import load_module
adapter = load_module("adapter")
import importlib, os
from pathlib import Path
names, mcp = adapter._core_tool_selection()
missing = []
for module, attribute in (("agent.skill_utils", "parse_config_string_list"), ("model_tools", "get_tool_definitions")):
    try:
        if not hasattr(importlib.import_module(module), attribute):
            missing.append(f"{module}.{attribute}")
    except ImportError:
        missing.append(f"{module}.{attribute}")
instance = object.__new__(adapter.SyntheticSocialityAdapter)
instance._state_file = Path(os.environ["HERMES_HOME"]) / "synthetic-sociality-room" / "state.json"
print("RESULT " + json.dumps({"names": sorted(names) if names is not None else None, "mcp": mcp,
                              "missing": missing, "reasons": instance._secret_store_reasons()}))
"""


def core_selection(config_yaml):
    """adapter._core_tool_selection() against the real core, in a clean interpreter."""
    import subprocess
    import sys
    with tempfile.TemporaryDirectory() as scratch:
        home = Path(scratch) / ".hermes"
        home.mkdir()
        (home / "config.yaml").write_text(config_yaml)
        completed = subprocess.run(
            [sys.executable, "-c", CORE_SELECTION, str(ROOT / "tests")], capture_output=True, text=True,
            timeout=240, env={**os.environ, "HERMES_HOME": str(home), "HOME": scratch}, cwd=str(ROOT / "tests"),
        )
    line = next((item for item in completed.stdout.splitlines() if item.startswith("RESULT ")), "")
    if not line:
        raise AssertionError(f"core selection failed: rc={completed.returncode} {completed.stderr[-800:]!r}")
    return json.loads(line[len("RESULT "):])


def run_cli(*argv):
    parser = __import__("argparse").ArgumentParser()
    cli.setup_cli(parser)
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = cli.dispatch(parser.parse_args(list(argv)))
    return code, out.getvalue(), err.getvalue()


class P3ConformanceTests(unittest.TestCase):
    def test_case_names_resolve_to_tests(self):
        cases = CONFORMANCE["parityP3Cases"]
        self.assertEqual(len(cases), 9)
        sources = "\n".join(path.read_text() for path in (ROOT / "tests").glob("test_*.py"))
        for name, method in cases.items():
            self.assertTrue(name.startswith("P3-0"), name)
            self.assertEqual(sources.count(f"def {method}(self"), 1, method)
        self.assertEqual(CONFORMANCE["parityP3TerminalIntentRetention"], recovery.TERMINAL_INTENT_RETENTION)

    def test_audit_record_is_byte_compatible_with_openclaw(self):
        # The same vector is pinned in OpenClaw's test/parity-p3.test.mjs.
        proven = {"eventId": "ev379", "eventType": "discussion.cycle_attempt_ready", "eventEpochId": "epoch-old",
                  "proof": {"activeEpochId": "epoch-new", "eventEpochStatus": "closed", "cycleId": CYCLE,
                            "cycleState": "interrupted", "cycleGeneration": 1, "eventGeneration": 1,
                            "deliveredSeq": 990, "fetchedAt": "2026-10-03T00:00:00Z"}}
        record = recovery.build_record(
            connector="shared/vector", room_id="bjqiu9u7dwqptlq", membership_id="member-1", client_instance_id="inst-1",
            seq=379, reason="cycle_terminal", evidence_ref=f"cycle:{CYCLE}", proven=proven,
            prior={"kind": "quarantine", "intentKey": "jyj0e25ev0a5hn8", "lastErrorCode": "cycle_superseded"},
            operator="owner — tjé")
        record["recordedAt"] = "2026-10-03T00:00:01Z"
        record["recordDigest"] = recovery.record_digest(record)
        self.assertEqual(record["confirmToken"], "311dfde5099d1d36")
        self.assertEqual(record["recordDigest"], "c91b6942a3039f95738450503b6c4c8dd63909c066eefeedabb164ca4f9026ed")


class P3Tests(unittest.TestCase):
    def test_p3_01_every_event_read_above_the_acknowledgement_has_a_ledger_entry_before_it_is_processed(self):
        # Hermes is the reference: the receive path writes the ledger entry and
        # persists it before any policy read, dispatch or completion.
        from test_parity_p2a import consume_instance, message

        instance, binding, _starts, completed, _persisted = consume_instance()
        order = []
        instance._persist_binding = lambda current: order.append(("persist", dict(current.inbox))) or True
        original_call = instance._call

        def call(current, operation):
            order.append(("room", dict(current.inbox)))
            return original_call(current, operation)

        instance._call = call
        asyncio.run(instance._consume(binding, message(2964, "paula-1", "participant_agent", "Not for you.")))
        self.assertEqual(order[0], ("persist", {"2964": "pending"}))
        self.assertTrue(all(snapshot.get("2964") for _kind, snapshot in order))
        self.assertTrue(completed, "the event reaches a terminal outcome")

    def test_p3_02_an_older_state_file_is_upgraded_once_in_place_without_data_loss_and_with_a_copy_of_the_original(self):
        # Hermes needs no format migration: a 1.0.76 state (no recovery_closures)
        # loads and saves with every field intact; saving again is byte-identical.
        binding = paula_binding()
        with tempfile.TemporaryDirectory() as scratch:
            path = Path(scratch) / "profile" / "state.json"
            state_module.save(adapter.PluginState(bindings=[binding]), path)
            older = json.loads(path.read_text())
            del older["bindings"][0]["recovery_closures"]
            path.write_text(json.dumps(older))
            loaded = state_module.load(path)
            state_module.save(loaded, path)
            first = path.read_bytes()
            upgraded = json.loads(first)["bindings"][0]
            for key, value in older["bindings"][0].items():
                self.assertEqual(upgraded[key], value, key)
            self.assertEqual(upgraded["recovery_closures"], {})
            state_module.save(state_module.load(path), path)
            self.assertEqual(path.read_bytes(), first)

    def test_p3_03_terminal_intents_beyond_the_newest_32_are_pruned_and_quarantined_or_open_intents_never_are(self):
        binding = paula_binding()
        for number in range(40):
            binding.delivery_intents[f"posted-{number}"] = {"state": "posted", "delivery_state": "posted",
                                                            "lifecycle_state": "complete" if number % 2 else "not_required"}
        binding.delivery_intents["selected-1"] = {"state": "selected", "delivery_state": "selected",
                                                  "lifecycle_state": "not_started", "selected": {"source_seq": 100}}
        binding.delivery_intents["pending-life"] = {"state": "lifecycle_pending", "delivery_state": "posted",
                                                    "lifecycle_state": "pending"}
        pruned = recovery.prune_terminal_intents(binding)
        self.assertEqual(pruned, [f"posted-{number}" for number in range(8)])
        self.assertEqual(sum(recovery.is_terminal_intent(item) for item in binding.delivery_intents.values()), 32)
        for kept in ("ev379", "selected-1", "pending-life", "posted-8", "posted-39"):
            self.assertIn(kept, binding.delivery_intents)
        self.assertEqual(recovery.prune_terminal_intents(binding), [])

    def test_p3_04_a_hole_is_reported_as_a_code_logged_once_and_never_closed_by_the_connector(self):
        with tempfile.TemporaryDirectory() as scratch:
            binding = claude_binding()
            before = json.dumps(binding.__dict__, sort_keys=True, default=str)
            instance = ledger_instance(binding, ProofRoom(), Path(scratch) / "state.json")
            with self.assertLogs(adapter.logger, level="WARNING") as logs:
                instance._report_ledger(binding)
                instance._report_ledger(binding)
            self.assertEqual(len([line for line in logs.output if "ack_hole seq 383" in line]), 1, logs.output)
            entry = local_status.read(Path(scratch) / "status.json")["rooms"]["room-1"]
            self.assertEqual((entry["acknowledgedSeq"], entry["holeSeqs"], entry["quarantinedSeqs"], entry["ledgerCode"]),
                             (382, [383], [], "ack_hole"))
            self.assertEqual(json.dumps(binding.__dict__, sort_keys=True, default=str), before)
            self.assertEqual(instance.persisted, [])
            report = recovery.ledger_report(paula_binding())
            self.assertEqual((report["holeSeqs"], report["quarantinedSeqs"], report["ledgerCode"]), ([], [379], "quarantine_held"))
            self.assertEqual(recovery.ledger_report(adapter.RoomBinding("u", "r", "m", "c", acknowledged_cursor=5))["ledgerCode"], "")

    def test_p3_05_recovery_list_is_read_only_and_shows_holes_quarantines_and_stuck_intents(self):
        binding = paula_binding()
        binding.delivery_intents["old-selected"] = {"state": "selected", "delivery_state": "selected",
                                                    "selected": {"source_seq": 144}}
        with profile(binding) as path:
            before = path.read_bytes()
            code, out, _err = run_cli("recovery", "list", "room-1")
            self.assertEqual(code, 0)
            self.assertEqual(path.read_bytes(), before)
            self.assertFalse(recovery.journal_path(path).exists())
        self.assertIn(f"state: {path}", out)
        self.assertIn("acknowledged 378", out)
        self.assertIn("quarantines: 379", out)
        self.assertIn("stuck intent ev379: source seq 379, quarantined, cycle_superseded", out)
        self.assertIn("stuck intent old-selected: source seq 144", out)
        self.assertNotIn("credential", out)

    def test_p3_06_a_recovery_closure_is_a_dry_run_until_applied_with_its_token_and_acknowledges_through_the_normal_path(self):
        room = ProofRoom()
        args = ("recovery", "close", "room-1", "--seq", "379", "--reason", "cycle_terminal",
                "--evidence", f"cycle:{CYCLE}", "--operator", "owner-tje")
        with profile(paula_binding()) as path, mock.patch.object(cli, "RoomProtocol", lambda *_a: room):
            before = path.read_bytes()
            code, out, _ = run_cli(*args)
            self.assertEqual(code, 0)
            token = out.split("--confirm ")[-1].strip()
            self.assertEqual(len(token), 16)
            self.assertFalse(recovery.journal_path(path).exists(), "a dry run writes nothing")
            code, _out, err = run_cli(*args, "--apply", "--confirm", "0" * 16)
            self.assertEqual(code, 1)
            self.assertIn("confirmation token does not match", err)
            self.assertFalse(recovery.journal_path(path).exists())
            code, out, _ = run_cli(*args, "--apply", "--confirm", token)
            self.assertEqual(code, 0)
            self.assertEqual(path.read_bytes(), before, "the CLI never writes the state")
            journal = recovery.journal_path(path)
            self.assertEqual(stat.S_IMODE(journal.stat().st_mode), 0o600)
            records = recovery.read_records(journal)
            self.assertEqual(len(records), 1)
            record = records[0]
            self.assertEqual(sorted(record), sorted([
                "version", "verb", "connector", "room", "membership", "clientInstanceId", "seq", "eventId", "eventType",
                "eventEpochId", "reason", "evidenceRef", "proof", "priorState", "operator", "confirmToken",
                "recordedAt", "recordDigest"]))
            self.assertEqual((record["seq"], record["eventId"], record["reason"], record["operator"], record["confirmToken"]),
                             (379, "ev379", "cycle_terminal", "owner-tje", token))
            self.assertEqual(record["priorState"], {"kind": "quarantine", "intentKey": "ev379", "lastErrorCode": "cycle_superseded"})
            self.assertEqual(record["proof"]["cycleState"], "interrupted")
            self.assertNotIn("body", journal.read_text())
            self.assertNotIn("credential", journal.read_text())
            self.assertTrue(all(read == "epochs" or read == "state" or read[0] in {"events", "cycle"} for read in room.reads))
            self.assertTrue(all(read[2] == "epoch-old" for read in room.reads if isinstance(read, tuple) and read[0] == "events"))
            # An altered line is not a record.
            journal.write_text(journal.read_text().replace("owner-tje", "someone-else"))
            self.assertEqual(recovery.read_records(journal), [])
            journal.write_text("")
            recovery.append_record(journal, record)

            # The running connector applies it through the normal ack path.
            binding = state_module.load(path).bindings[0]
            instance = ledger_instance(binding, room, path)
            with self.assertLogs(adapter.logger, level="WARNING") as logs:
                asyncio.run(instance._apply_recovery_closures(binding))
                asyncio.run(instance._apply_recovery_closures(binding))
            self.assertEqual(room.acks, [382], "acknowledged once, contiguously, through the Room route")
            self.assertEqual(binding.acknowledged_cursor, 382)
            self.assertIn("ev379", binding.delivery_intents, "the quarantined intent is kept as evidence")
            self.assertEqual(binding.delivery_intents["ev379"]["delivery_state"], "quarantined")
            self.assertEqual(binding.recovery_closures["379"]["recordDigest"], record["recordDigest"])
            self.assertEqual(adapter.SyntheticSocialityAdapter._overtaken_delivery_intents(binding), [])
            self.assertEqual(len([line for line in logs.output if "recovery closure applied" in line]), 1)

        # Claude's hole: epoch_closed, evidence epoch:<the epoch of 383>.
        room = ProofRoom()
        with profile(claude_binding()) as path, mock.patch.object(cli, "RoomProtocol", lambda *_a: room):
            args = ("recovery", "close", "room-1", "--seq", "383", "--reason", "epoch_closed",
                    "--evidence", "epoch:epoch-old", "--operator", "owner-tje")
            code, out, _ = run_cli(*args)
            self.assertEqual(code, 0)
            token = out.split("--confirm ")[-1].strip()
            self.assertEqual(run_cli(*args, "--apply", "--confirm", token)[0], 0)
            record = recovery.read_records(recovery.journal_path(path))[0]
            self.assertEqual(record["priorState"]["kind"], "hole")
            self.assertNotIn("never reaches", recovery.journal_path(path).read_text())
            binding = state_module.load(path).bindings[0]
            instance = ledger_instance(binding, room, path)
            with self.assertLogs(adapter.logger, level="WARNING"):
                asyncio.run(instance._apply_recovery_closures(binding))
            self.assertEqual((room.acks, binding.acknowledged_cursor), ([385], 385))

    def test_p3_07_a_recovery_closure_is_refused_without_the_room_s_proof_that_the_event_is_terminal_for_this_member(self):
        cases = [
            (ProofRoom(cycle_state="running"), 379, "cycle_terminal", f"cycle:{CYCLE}", "not_proven"),
            (ProofRoom(cycle_generation=1), 379, "cycle_superseded", f"cycle:{CYCLE}", "not_proven"),
            (ProofRoom(), 379, "cycle_terminal", "cycle:another", "evidence_mismatch"),
            (ProofRoom(active="epoch-old"), 383, "epoch_closed", "epoch:epoch-old", "not_proven"),
            (ProofRoom(), 383, "epoch_closed", "epoch:epoch-new", "evidence_mismatch"),
            (ProofRoom(), 383, "cycle_terminal", "cycle:x", "not_proven"),
            (ProofRoom(events=[{**ATTEMPT_READY, "payload": {**ATTEMPT_READY["payload"], "membershipId": "other"}}]),
             379, "cycle_terminal", f"cycle:{CYCLE}", "not_for_member"),
            (ProofRoom(events=[]), 379, "cycle_terminal", f"cycle:{CYCLE}", "event_not_found"),
            (ProofRoom(), 379, "made_up", f"cycle:{CYCLE}", "unknown_reason"),
        ]
        for room, seq, reason, evidence, code in cases:
            with self.subTest(code=code, reason=reason), self.assertRaises(recovery.RecoveryRefused) as raised:
                recovery.server_proof(room, "room-1", "member-1", seq, reason, evidence)
            self.assertEqual(raised.exception.code, code)
        self.assertEqual(recovery.server_proof(ProofRoom(cycle_generation=2), "room-1", "member-1", 379,
                                               "cycle_superseded", f"cycle:{CYCLE}")["proof"]["cycleGeneration"], 2)
        # The state must describe a hole or a quarantine above the acknowledgement.
        for binding, seq, code in ((paula_binding(), 378, "already_acknowledged"), (paula_binding(), 380, "not_closable"),
                                   (claude_binding(), 390, "not_closable")):
            with self.assertRaises(recovery.RecoveryRefused) as raised:
                recovery.prior_state(binding, seq)
            self.assertEqual(raised.exception.code, code)
        # The CLI refuses without proof; the connector refuses a record whose
        # proof no longer holds and changes nothing.
        refused = ProofRoom(cycle_state="running")
        with profile(paula_binding()) as path, mock.patch.object(cli, "RoomProtocol", lambda *_a: refused):
            code, _out, err = run_cli("recovery", "close", "room-1", "--seq", "379", "--reason", "cycle_terminal",
                                      "--evidence", f"cycle:{CYCLE}", "--operator", "owner-tje")
            self.assertEqual(code, 1)
            self.assertIn("not_proven", err)
            record = recovery.seal(recovery.build_record(
                connector="hermes/test", room_id="room-1", membership_id="member-1", client_instance_id="inst-1",
                seq=379, reason="cycle_terminal", evidence_ref=f"cycle:{CYCLE}",
                proven=recovery.server_proof(ProofRoom(), "room-1", "member-1", 379, "cycle_terminal", f"cycle:{CYCLE}"),
                prior={"kind": "quarantine", "intentKey": "ev379", "lastErrorCode": "cycle_superseded"}, operator="owner-tje"))
            recovery.append_record(recovery.journal_path(path), record)
            binding = state_module.load(path).bindings[0]
            instance = ledger_instance(binding, refused, path)
            with self.assertLogs(adapter.logger, level="WARNING") as logs:
                asyncio.run(instance._apply_recovery_closures(binding))
            self.assertIn("not applied: not_proven", "\n".join(logs.output))
            self.assertEqual((refused.acks, binding.acknowledged_cursor, binding.recovery_closures), ([], 378, {}))
            self.assertEqual(binding.inbox["379"], "quarantined")

        # A sealed record for another binding (client instance, membership or
        # Room) is never applied to this one.
        for field, value in (("clientInstanceId", "other-instance"), ("membership", "member-2"), ("room", "room-2")):
            with self.subTest(binding_field=field), profile(paula_binding()) as path:
                proven = recovery.server_proof(ProofRoom(), "room-1", "member-1", 379, "cycle_terminal", f"cycle:{CYCLE}")
                record = recovery.build_record(
                    connector="hermes/test", room_id="room-1", membership_id="member-1", client_instance_id="inst-1",
                    seq=379, reason="cycle_terminal", evidence_ref=f"cycle:{CYCLE}", proven=proven,
                    prior={"kind": "quarantine", "intentKey": "ev379", "lastErrorCode": "cycle_superseded"}, operator="o")
                record[field] = value
                record["confirmToken"] = recovery.confirm_token(record)
                recovery.append_record(recovery.journal_path(path), recovery.seal(record))
                room = ProofRoom()
                binding = state_module.load(path).bindings[0]
                asyncio.run(ledger_instance(binding, room, path)._apply_recovery_closures(binding))
                self.assertEqual((room.acks, binding.recovery_closures, binding.acknowledged_cursor), ([], {}, 378))

        # P3 final review: a mark counts as recorded only for its own record.
        # A mark with another digest plus a valid record runs the proof again.
        for cycle_state, applied in (("interrupted", True), ("running", False)):
            with self.subTest(stale_mark=cycle_state), profile(paula_binding()) as path:
                record = recovery.seal(recovery.build_record(
                    connector="hermes/test", room_id="room-1", membership_id="member-1", client_instance_id="inst-1",
                    seq=379, reason="cycle_terminal", evidence_ref=f"cycle:{CYCLE}",
                    proven=recovery.server_proof(ProofRoom(), "room-1", "member-1", 379, "cycle_terminal", f"cycle:{CYCLE}"),
                    prior={"kind": "quarantine", "intentKey": "ev379", "lastErrorCode": "cycle_superseded"}, operator="o"))
                recovery.append_record(recovery.journal_path(path), record)
                binding = state_module.load(path).bindings[0]
                binding.recovery_closures = {"379": {"recordDigest": "0" * 64, "appliedAt": "2026-10-03T00:00:00Z"}}
                room = ProofRoom(cycle_state=cycle_state)
                with self.assertLogs(adapter.logger, level="WARNING"):
                    asyncio.run(ledger_instance(binding, room, path)._apply_recovery_closures(binding))
                self.assertIn(("cycle", CYCLE), room.reads, "the proof was checked again")
                if applied:
                    self.assertEqual((room.acks, binding.recovery_closures["379"]["recordDigest"]), ([382], record["recordDigest"]))
                else:
                    self.assertEqual((room.acks, binding.acknowledged_cursor), ([], 378))

        # OP-12 F2/F3: a transient error is retried at the next iteration; any
        # other error retires only that record, logged once; the loop goes on.
        with profile(paula_binding()) as path:
            proven = recovery.server_proof(ProofRoom(), "room-1", "member-1", 379, "cycle_terminal", f"cycle:{CYCLE}")
            recovery.append_record(recovery.journal_path(path), recovery.seal(recovery.build_record(
                connector="hermes/test", room_id="room-1", membership_id="member-1", client_instance_id="inst-1",
                seq=379, reason="cycle_terminal", evidence_ref=f"cycle:{CYCLE}", proven=proven,
                prior={"kind": "quarantine", "intentKey": "ev379", "lastErrorCode": "cycle_superseded"}, operator="o")))
            flaky = ProofRoom()
            failures = [adapter.ProtocolError("Room API unavailable: timed out", retryable=True), TimeoutError("slow")]
            original = flaky.get_discussion_cycle

            def cycle(room_id, cycle_id):
                if failures:
                    raise failures.pop(0)
                return original(room_id, cycle_id)

            flaky.get_discussion_cycle = cycle
            binding = state_module.load(path).bindings[0]
            instance = ledger_instance(binding, flaky, path)
            with self.assertLogs(adapter.logger, level="WARNING") as logs:
                for _ in range(3):
                    asyncio.run(instance._apply_recovery_closures(binding))
            self.assertEqual((flaky.acks, binding.acknowledged_cursor), ([382], 382))
            self.assertEqual(len([line for line in logs.output if "not applied yet (retrying)" in line]), 1, logs.output)
        with profile(paula_binding()) as path:
            recovery.append_record(recovery.journal_path(path), recovery.seal(recovery.build_record(
                connector="hermes/test", room_id="room-1", membership_id="member-1", client_instance_id="inst-1",
                seq=379, reason="cycle_terminal", evidence_ref=f"cycle:{CYCLE}", proven=proven,
                prior={"kind": "quarantine", "intentKey": "ev379", "lastErrorCode": "cycle_superseded"}, operator="o")))
            broken = ProofRoom()
            broken.get_discussion_cycle = lambda *_a: (_ for _ in ()).throw(KeyError("unexpected"))
            binding = state_module.load(path).bindings[0]
            instance = ledger_instance(binding, broken, path)
            with self.assertLogs(adapter.logger, level="WARNING") as logs:
                asyncio.run(instance._apply_recovery_closures(binding))
                asyncio.run(instance._apply_recovery_closures(binding))
            # Contract P3-4 (delta): an unexpected exception is retried, logged once.
            self.assertEqual(len([line for line in logs.output if "not applied yet (retrying): KeyError" in line]), 1, logs.output)
            self.assertEqual((broken.acks, binding.recovery_closures), ([], {}))
            self.assertNotIn((binding.room_id, 379, recovery.read_records(recovery.journal_path(path))[0]["recordDigest"]),
                             instance._refused_closures)
            # A final Room error retires the record.
            final = ProofRoom()
            final.get_discussion_cycle = lambda *_a: (_ for _ in ()).throw(adapter.ProtocolError("gone", status=404, code="not_found"))
            instance = ledger_instance(state_module.load(path).bindings[0], final, path)
            binding = instance_binding = state_module.load(path).bindings[0]
            with self.assertLogs(adapter.logger, level="WARNING") as logs:
                asyncio.run(instance._apply_recovery_closures(instance_binding))
                asyncio.run(instance._apply_recovery_closures(instance_binding))
            self.assertEqual(len([line for line in logs.output if "not applied: not_found" in line]), 1, logs.output)

        # OP-12 P3 delta: an undecodable line or a record whose proof is not an
        # object is skipped; a journal read that raises is logged once and the
        # watch loop goes on.
        with profile(paula_binding()) as path:
            journal = recovery.journal_path(path)
            good = recovery.seal(recovery.build_record(
                connector="hermes/test", room_id="room-1", membership_id="member-1", client_instance_id="inst-1",
                seq=379, reason="cycle_terminal", evidence_ref=f"cycle:{CYCLE}", proven=proven,
                prior={"kind": "quarantine", "intentKey": "ev379", "lastErrorCode": "cycle_superseded"}, operator="o"))
            not_object = {**good, "proof": "not-an-object"}
            not_object["recordDigest"] = recovery.record_digest(not_object)
            journal.write_bytes(b'{"version": 1, "verb": "close", "operator": "\xff\xfe"}\n'
                                + (json.dumps(not_object) + "\n").encode()
                                + b"\xc3\x28 not utf-8\n"
                                + b"[" * 200000 + b"\n"
                                + (recovery.canonical(good) + "\n").encode())
            self.assertEqual([record["recordDigest"] for record in recovery.read_records(journal)], [good["recordDigest"]])
            with self.assertRaises(ValueError):
                recovery.confirm_token(not_object)
            room = ProofRoom()
            binding = state_module.load(path).bindings[0]
            instance = ledger_instance(binding, room, path)
            with self.assertLogs(adapter.logger, level="WARNING"):
                asyncio.run(instance._apply_recovery_closures(binding))
            self.assertEqual(room.acks, [382], "the valid record still applies")
            with mock.patch.object(recovery, "read_records", side_effect=RuntimeError("bad journal")), \
                    self.assertLogs(adapter.logger, level="WARNING") as logs:
                fresh = ledger_instance(paula_binding(), ProofRoom(), path)
                asyncio.run(fresh._apply_recovery_closures(paula_binding()))
                asyncio.run(fresh._apply_recovery_closures(paula_binding()))
            self.assertEqual(len([line for line in logs.output if "journal could not be read: RuntimeError" in line]), 1)

    def test_p3_08_the_status_file_is_written_next_to_the_state_in_use_and_its_directory_is_created_0700(self):
        with tempfile.TemporaryDirectory() as scratch:
            path = Path(scratch) / "profile" / "synthetic-sociality-room" / "status.json"
            status = local_status.LocalStatus(path, plugin="hermes", version=adapter.CONNECTOR_VERSION, host_label="h")
            self.assertTrue(status.write())
            self.assertEqual(stat.S_IMODE(path.parent.stat().st_mode), 0o700)
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            # The rule: the status file sits in the directory of the state file
            # in use, wherever that state lives.
            custom = Path(scratch) / "elsewhere" / "custom-state.json"
            with mock.patch.object(adapter, "state_path", lambda: custom), \
                    mock.patch.object(adapter, "load", lambda *_: adapter.PluginState()):
                built = adapter.SyntheticSocialityAdapter(adapter.PlatformConfig())
            self.assertEqual(built._status.path, custom.parent / "status.json")
        # Never a filesystem root or a top-level directory (P2a-5).
        self.assertFalse(local_status.LocalStatus(Path("/p3-status-test/status.json"), plugin="hermes",
                                                  version="x", host_label="h").write())
        self.assertFalse(Path("/p3-status-test").exists())

    def test_p3_09_a_known_path_argument_is_resolved_whatever_its_shape_and_denied_tools_count_as_isolated(self):
        with tempfile.TemporaryDirectory() as scratch:
            home = Path(scratch) / ".hermes"
            store = home / "synthetic-sociality-room"
            store.mkdir(parents=True)
            with mock.patch.dict(os.environ, {"HERMES_HOME": str(home), "HOME": scratch, "TERMINAL_CWD": str(home)}), \
                    self.assertLogs(adapter.logger, level="WARNING"):
                # cd ~/.hermes, then a bare directory name.
                self.assertEqual(adapter._on_pre_tool_call(tool_name="search_files", args={"path": "synthetic-sociality-room"})["action"], "block")
                with mock.patch.dict(os.environ, {"TERMINAL_CWD": str(store)}):
                    self.assertEqual(adapter._on_pre_tool_call(tool_name="read_file", args={"path": "state.json"})["action"], "block")
                    self.assertEqual(adapter._on_pre_tool_call(tool_name="read_file", args={"file": "state.json"})["action"], "block")
                self.assertEqual(adapter._on_pre_tool_call(tool_name="process_manage",
                                                           args={"command": f"cat {store}/state.json"})["action"], "block")
            with mock.patch.dict(os.environ, {"HERMES_HOME": str(home), "HOME": scratch, "TERMINAL_CWD": scratch}):
                self.assertIsNone(adapter._on_pre_tool_call(tool_name="read_file", args={"path": "notes.md"}))
            # Denied tools, judged by core's own tool selection (exact names):
            # none of terminal / process / process_manage / execute_code left.
            local = {"TERMINAL_ENV": "local"}
            reach = lambda **kw: secret_isolation.store_reachability(store, local, mount_sources=[], **kw)
            self.assertEqual(reach(tool_names={"read_file", "patch", "search_files"}), [])
            self.assertEqual(reach(tool_names={"read_file", "process"}), ["host_shell_backend"])
            self.assertEqual(reach(tool_names={"read_file", "process_manage"}), ["host_shell_backend"])
            self.assertEqual(reach(tool_names=None), ["host_shell_backend"])
            # Computer use and MCP servers reach the host outside both routes.
            self.assertEqual(reach(tool_names={"read_file", "computer_use"}), ["computer_use_tool"])
            self.assertEqual(reach(tool_names={"read_file"}, mcp_servers=["filesystem"]), ["mcp_server_enabled"])
            sandboxed = {"TERMINAL_ENV": "docker"}
            self.assertEqual(secret_isolation.store_reachability(store, sandboxed, mount_sources=[], mcp_servers=["x"]),
                             ["mcp_server_enabled"])
            instance = object.__new__(adapter.SyntheticSocialityAdapter)
            instance._state_file = store / "state.json"
            with mock.patch.dict(os.environ, {"HERMES_HOME": str(home), "TERMINAL_ENV": "local"}), \
                    mock.patch.object(adapter, "_core_tool_selection", lambda: ({"read_file", "patch"}, [])):
                self.assertEqual(instance._secret_store_reasons(), [])
            with mock.patch.dict(os.environ, {"HERMES_HOME": str(home), "TERMINAL_ENV": "local"}), \
                    mock.patch.object(adapter, "_core_tool_selection", lambda: (None, [])):
                self.assertEqual(instance._secret_store_reasons(), ["host_shell_backend", "tool_selection_unavailable"])
            # Contract L1-15: unknown selection is reachable on a sandbox backend too.
            with mock.patch.dict(os.environ, {"HERMES_HOME": str(home), "TERMINAL_ENV": "docker", "TERMINAL_DOCKER_VOLUMES": "[]"}), \
                    mock.patch.object(adapter, "_core_tool_selection", lambda: (None, [])), \
                    mock.patch.object(secret_isolation, "_sandbox_mount_sources", lambda: []):
                self.assertEqual(instance._secret_store_reasons(), ["tool_selection_unavailable"])

        # The config forms from the OP-12 review, through the real core (its
        # parse_config_string_list and get_tool_definitions) in a clean
        # interpreter: a list and a JSON-array string deny; a comma string is
        # one name and an off-case name matches nothing, so both stay live.
        mcp = "mcp_servers:\n  alpha:\n    command: x\n  beta:\n    command: y\n    enabled: false\n"
        forms = {
            "list": ("agent:\n  disabled_toolsets:\n    - terminal\n    - code_execution\n", True),
            "json_string": ("agent:\n  disabled_toolsets: '[\"terminal\", \"code_execution\"]'\n", True),
            "comma_string": ("agent:\n  disabled_toolsets: terminal, code_execution\n", False),
            "wrong_case": ("agent:\n  disabled_toolsets:\n    - Terminal\n    - code_execution\n", False),
        }
        for label, (agent, denied) in forms.items():
            with self.subTest(form=label):
                result = core_selection(agent + mcp)
                self.assertEqual(result["mcp"], ["alpha"])
                if result["missing"]:
                    # Contract L1-15: a core that genuinely lacks the API (core
                    # c896c09c has no parse_config_string_list) reports no
                    # selection, and the store counts as reachable.
                    self.assertIsNone(result["names"], result)
                    self.assertIn("tool_selection_unavailable", result["reasons"], result)
                    self.skipTest(f"core lacks {', '.join(result['missing'])}: exact selection not checkable")
                self.assertIsNotNone(result["names"], result)
                self.assertNotIn("tool_selection_unavailable", result["reasons"], result)
                self.assertEqual(not (set(result["names"]) & {"terminal", "process", "process_manage", "execute_code"}),
                                 denied, result["names"])


if __name__ == "__main__":
    unittest.main()
