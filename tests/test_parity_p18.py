"""P18: abandon the pending work of a revoked binding on re-admission (Hermes 1.0.105)."""
from __future__ import annotations

import ast
import json
import re
import unittest

from test_delivery_lifecycle import ROOT

CONTRACT = "docs/delivery-lifecycle-contract-v1.md#p18--abandon-the-pending-work-of-a-revoked-binding-on-re-admission-2026-11-13"
HEADING = "## P18 — Abandon the pending work of a revoked binding on re-admission (2026-11-13)"
CASE_TESTS = {
    "P18-01": ["test_p18_01_the_live_real_binding_is_replaced_by_the_fresh_credential"],
    "P18-02": ["test_p18_02_the_journal_records_every_abandoned_entry_and_survives_replacement_and_reload"],
    "P18-03": ["test_p18_03_an_active_binding_with_pending_work_is_untouched_and_unclaimed"],
    "P18-04": ["test_p18_04_an_operator_disabled_binding_is_untouched_and_unclaimed"],
    "P18-05": ["test_p18_05_a_replay_appends_no_duplicate_audit_entry"],
    "P18-06": ["test_p18_06_a_binding_changed_before_the_transaction_is_kept_and_unclaimed"],
    "P18-07": ["test_p18_07_a_revoked_binding_with_an_intent_or_quarantine_is_not_abandoned"],
}


class P18ConformanceTests(unittest.TestCase):
    def test_conformance_pins_the_contract_and_cases(self):
        conf = json.loads((ROOT / "conformance.json").read_text())
        self.assertEqual(conf["parityP18Contract"], CONTRACT)
        self.assertEqual([case.split(" ", 1)[0] for case in conf["parityP18Cases"]], sorted(CASE_TESTS))

    def test_contract_section_lists_the_clauses_and_the_same_cases(self):
        document = (ROOT / "docs/delivery-lifecycle-contract-v1.md").read_text()
        self.assertEqual(document.count(HEADING), 1)
        section = document.split(HEADING)[1].split("\n## ")[0]
        conf = json.loads((ROOT / "conformance.json").read_text())
        self.assertEqual([line[2:] for line in section.splitlines() if re.match(r"- P18-\d\d ", line)], conf["parityP18Cases"])
        for number in range(1, 6):
            self.assertIn(f"- P18-{number} ", section)

    def test_every_case_maps_to_an_existing_test(self):
        source = ast.parse((ROOT / "tests/test_revoked_abandonment.py").read_text())
        defined = {node.name for node in ast.walk(source) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
        for case, tests in CASE_TESTS.items():
            for name in tests:
                with self.subTest(case=case, test=name):
                    self.assertIn(name, defined)

    def test_the_whole_shared_contract_matches_conformance_contract_sha256(self):
        """PR80-H-005: the whole shared contract is pinned, so a single-repo edit to any clause fails the suite."""
        import hashlib
        conf = json.loads((ROOT / "conformance.json").read_text())
        self.assertRegex(conf["contractSha256"], r"^[0-9a-f]{64}$")
        digest = hashlib.sha256((ROOT / "docs/delivery-lifecycle-contract-v1.md").read_bytes()).hexdigest()
        self.assertEqual(digest, conf["contractSha256"])


if __name__ == "__main__":
    unittest.main()
