"""P16: one live binding per Room (shared clause with OpenClaw 0.2.78)."""
from __future__ import annotations

import ast
import json
import unittest

from test_delivery_lifecycle import ROOT

CONTRACT = "docs/delivery-lifecycle-contract-v1.md#p16--one-live-binding-per-room-2026-11-11"
HEADING = "## P16 — One live binding per Room (2026-11-11)"
CASES = [
    "P16-01 expiry of one binding keeps sibling receive and send on one live binding",
    "P16-02 revocation of one binding keeps sibling receive and send on one live binding",
    "P16-03 renewal of one binding keeps one live binding per Room",
    "P16-04 a stopped binding keeps its reason and posts nothing",
    "P16-05 a persisted revocation wins over a later expiry",
]
# Each shared case is proven by these Hermes tests (tests/test_binding_split.py).
CASE_TESTS = {
    "P16-01": ["test_expiry_in_one_room_keeps_sibling_send_path_on_the_listener_binding"],
    "P16-02": ["test_revocation_in_one_room_keeps_sibling_send_path_on_the_listener_binding"],
    "P16-03": ["test_renewal_in_one_room_keeps_one_shared_binding_per_room",
               "test_renewal_while_the_listener_runs_never_replaces_its_binding"],
    # assert_stopped_room_posts_nothing runs inside both of these.
    "P16-04": ["test_expiry_in_one_room_keeps_sibling_send_path_on_the_listener_binding",
               "test_revocation_in_one_room_keeps_sibling_send_path_on_the_listener_binding"],
    "P16-05": ["test_expiry_preserves_a_concurrently_persisted_revocation"],
}


class P16ConformanceTests(unittest.TestCase):
    def test_conformance_pins_the_shared_contract_and_cases(self):
        conf = json.loads((ROOT / "conformance.json").read_text())
        self.assertEqual(conf["parityP16Contract"], CONTRACT)
        self.assertEqual(conf["parityP16Cases"], CASES)

    def test_contract_section_lists_the_same_cases(self):
        document = (ROOT / "docs/delivery-lifecycle-contract-v1.md").read_text()
        self.assertEqual(document.count(HEADING), 1)
        section = document.split(HEADING)[1].split("\n## ")[0]
        self.assertEqual([line[2:] for line in section.splitlines() if line.startswith("- P16-0")], CASES)
        for clause in ("- P16-1 ", "- P16-2 ", "- P16-3 "):
            self.assertIn(clause, section)

    def test_every_case_maps_to_an_existing_test(self):
        self.assertEqual(sorted(CASE_TESTS), [case.split(" ", 1)[0] for case in CASES])
        source = ast.parse((ROOT / "tests/test_binding_split.py").read_text())
        defined = {node.name for node in ast.walk(source) if isinstance(node, ast.AsyncFunctionDef)}
        for case, tests in CASE_TESTS.items():
            for name in tests:
                with self.subTest(case=case, test=name):
                    self.assertIn(name, defined)


if __name__ == "__main__":
    unittest.main()
