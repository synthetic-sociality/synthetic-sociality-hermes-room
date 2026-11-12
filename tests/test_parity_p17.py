"""P17: owner connector control (shared clause with OpenClaw 0.2.79)."""
from __future__ import annotations

import ast
import json
import re
import unittest

from test_delivery_lifecycle import ROOT

CONTRACT = "docs/delivery-lifecycle-contract-v1.md#p17--owner-connector-control-2026-11-12"
HEADING = "## P17 — Owner connector control (2026-11-12)"
CLAUSES = [f"- P17-{number} " for number in range(1, 9)]
CASES = [
    "P17-01 registration metadata/capability",
    "P17-02 malformed/absent connectorControl ignored",
    "P17-03 claim once across bindings, 409 ignored",
    "P17-04 refusal for active dispatch",
    "P17-05 refusal for pending renewal/credential_rotation",
    "P17-06 refusal for an unfinished delivery intent in another Room",
    "P17-07 fence blocks a new dispatch during the guard",
    "P17-08 reconnect reloads once, one listener per enabled binding, disabled untouched, "
    "`_binding(room) is` listener object",
    "P17-09 reconnect cures the 1.0.101 split: a sibling turn after reconnect is accepted",
    "P17-10 restart receipt persisted before the requester, simulated supervisor fixture receives the request",
    "P17-11 requester failure → supervisor_unavailable and listeners restored",
    "P17-12 post-restart completion/mismatch report, no repeat, state.json bytes unchanged by control",
]
# Each shared case is proven by these Hermes tests (tests/test_connector_control.py).
CASE_TESTS = {
    "P17-01": ["test_registration_reports_installation_identity_version_and_control_capability"],
    "P17-02": ["test_malformed_or_absent_connector_control_is_ignored_and_the_loop_continues"],
    "P17-03": ["test_one_claim_per_command_across_bindings_and_409_answers_are_ignored",
               "test_protocol_claim_and_report_routes"],
    "P17-04": ["test_work_in_progress_anywhere_in_the_installation_refuses_at_once"],
    "P17-05": ["test_pending_renewal_or_credential_rotation_refuses"],
    "P17-06": ["test_unfinished_delivery_intent_in_another_room_refuses_but_plain_quarantine_does_not"],
    "P17-07": ["test_fence_blocks_new_dispatch_attempt_claims_and_renewal_during_the_guard"],
    "P17-08": ["test_reconnect_reloads_once_and_starts_one_listener_per_enabled_binding",
               "test_reconnect_reports_incomplete_when_a_binding_does_not_register_again"],
    "P17-09": ["test_reconnect_cures_the_split_and_a_sibling_turn_is_accepted"],
    "P17-10": ["test_restart_persists_the_receipt_before_the_simulated_supervisor_is_asked",
               "test_default_requester_signals_only_its_own_gateway_process"],
    "P17-11": ["test_requester_failure_reports_supervisor_unavailable_and_restores_listeners"],
    "P17-12": ["test_post_restart_reports_completion_once_and_never_restarts_again",
               "test_post_restart_reports_an_inventory_mismatch"],
}


class P17ConformanceTests(unittest.TestCase):
    def test_conformance_pins_the_shared_contract_and_cases(self):
        conf = json.loads((ROOT / "conformance.json").read_text())
        self.assertEqual(conf["parityP17Contract"], CONTRACT)
        self.assertEqual(conf["parityP17Cases"], CASES)

    def test_contract_section_lists_the_same_clauses_and_cases(self):
        document = (ROOT / "docs/delivery-lifecycle-contract-v1.md").read_text()
        self.assertEqual(document.count(HEADING), 1)
        section = document.split(HEADING)[1].split("\n## ")[0]
        self.assertEqual([line[2:] for line in section.splitlines() if re.match(r"- P17-\d\d ", line)], CASES)
        for clause in CLAUSES:
            self.assertIn(clause, section)
        self.assertNotIn("- P17-9 ", section)

    def test_every_case_maps_to_an_existing_test(self):
        self.assertEqual(sorted(CASE_TESTS), [case.split(" ", 1)[0] for case in CASES])
        source = ast.parse((ROOT / "tests/test_connector_control.py").read_text())
        defined = {node.name for node in ast.walk(source) if isinstance(node, ast.AsyncFunctionDef)}
        for case, tests in CASE_TESTS.items():
            for name in tests:
                with self.subTest(case=case, test=name):
                    self.assertIn(name, defined)


if __name__ == "__main__":
    unittest.main()
