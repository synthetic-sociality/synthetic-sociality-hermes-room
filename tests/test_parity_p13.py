"""P13: execute production reply selection and serialization without a host.

Like ISSUE-106, AST extraction avoids the absent gateway imports. Only host/API
boundaries are replaced; the resolver, send method and freeze logic are real.
"""
from __future__ import annotations

import ast
import asyncio
import copy
import json
import re
import types
import unittest

from test_issue106 import ROOT, Harness as DeliveryHarness, ns as delivery_ns, protocol

CASES = [
    'P13-01 normal cycle reply resolves an exact active agent mention outside its roster',
    'P13-02 explicit mentions merge with rotation once in mention order',
    'P13-03 self everyone inactive unknown and near matches never resolve',
    'P13-04 ambiguous names refuse delivery before a post',
    'P13-05 active human mentions resolve without granting a cycle turn',
    'P13-06 retry and restart preserve frozen recipient selectors',
    'P13-07 follow-ups off still sends explicit mention selectors',
]


def member(mid, name, role='participant_agent', status='active'):
    return dict(membershipId=mid, displayName=name, role=role, status=status)


def extracted(path, names, namespace):
    nodes = []
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names:
            nodes.append(node)
    module = ast.Module(body=[ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0), *nodes], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), str(path), 'exec'), namespace)


ns = dict(delivery_ns)
ns.update(_decode_dispatch_source=lambda source: (source, ''),
          _GATEWAY_OPERATIONAL_ERRORS=set(), _known_epoch_start=lambda value: 0,
          SendResult=lambda **kwargs: types.SimpleNamespace(**kwargs))
extracted(ROOT / 'room_tools.py', {'_inline_agent_recipients'}, ns)
extracted(ROOT / 'adapter.py', {'_next_cycle_recipient', '_cycle_delivery_payload', '_send_final_owned', '_select_delivery_intent'}, ns)
resolve = ns['_inline_agent_recipients']


class Selected(Exception):
    pass


class ReplyHarness(DeliveryHarness):
    _send_final_owned = ns['_send_final_owned']
    def __init__(self, roster, rotation=False, followups=2):
        self.binding = types.SimpleNamespace(
            room_id='room', membership_id='zurie', cursor=1628,
            delivery_intents={}, share_uploads={}, message_payload_dialect='v2',
            turn_sequences={}, base_url='https://fixture.invalid/api', credential='fixture',
        )
        self.roster = roster
        self._event_epoch = {'source': 'epoch'}
        self._cycle_attempts = {'source': {
            'cycle': dict(id='cycle', generation=1, followUps=0,
                          budgets=dict(maxFollowUps=followups, perAgentTurns=2),
                          roster=[dict(membershipId='zurie'), *([dict(membershipId='rotation')] if rotation else [])]),
            'attempt': {'id': 'attempt'},
        }}
        self.api = types.SimpleNamespace(room_state=lambda _: dict(activeEpoch={'id': 'epoch'}, roster=self.roster))
    def _binding(self, _):
        return self.binding
    def _binding_generation_active(self, _):
        return True
    @staticmethod
    def _recoverable_selection(intent):
        return intent.get('selected')
    def _select_delivery_intent(self, *args, **kwargs):
        self.selected = ns['_select_delivery_intent'](self, *args, **kwargs)
        # Stop at the real durable-selection boundary; serialization below is
        # independently exercised through the real protocol implementation.
        raise Selected()
    def run_reply(self, body):
        with unittest.TestCase().assertRaises(Selected):
            asyncio.run(self._send_final_owned('room', 'source', body))
        return self.selected


class ParityP13Tests(unittest.TestCase):
    def test_conformance_names_and_contract(self):
        conformance = json.loads((ROOT / 'conformance.json').read_text())
        self.assertEqual(conformance['parityP13Cases'], CASES)
        self.assertEqual(conformance['parityP13Contract'], 'docs/delivery-lifecycle-contract-v1.md#p13--agent-replies-address-room-members-by-displayname-2026-10-27')
        text = (ROOT / 'docs/delivery-lifecycle-contract-v1.md').read_text()
        section = text.split('## P13 — Agent replies address Room members by @DisplayName (2026-10-27)', 1)[1].split('\n## ', 1)[0]
        self.assertEqual([line[2:] for line in section.splitlines() if line.startswith('- ')], CASES)

    def test_normal_reply_outside_roster_and_rotation_merge(self):
        for rotation in [False, True]:
            harness = ReplyHarness([member('zurie', 'Zurie'), member('paula', 'Paula'), member('rotation', 'Next')], rotation)
            selected = harness.run_reply('@Paula, then @Next; @Paula again.')
            self.assertEqual(selected['recipient_membership_ids'], ['paula', 'rotation'])
            self.assertEqual(selected['cycle']['recipients'], ['paula', 'rotation'])
            self.assertEqual(selected['contribution_type'], 'claim')
        harness = ReplyHarness([member('paula', 'Paula'), member('rotation', 'Next')], True)
        self.assertEqual(harness.run_reply('@Paula first')['recipient_membership_ids'], ['paula', 'rotation'])

    def test_exact_boundaries_self_everyone_inactive_and_unknown(self):
        roster = [member('zurie', 'Zurie'), member('paula', 'Paula'),
                  member('muted', 'Muted', status='muted'), member('removed', 'Removed', status='removed'),
                  member('everyone', 'everyone'), member('special', 'A.B (Ops)')]
        self.assertEqual(resolve({'roster': roster}, '@Zurie @everyone @Muted @Removed @Unknown mail@Paula @Paula-extra @PaulaX', 'zurie'), [])
        self.assertEqual(resolve({'roster': roster}, '@paula! @A.B (Ops):', 'zurie'), ['paula', 'special'])

    def test_ambiguity_refuses_before_selection_including_own_alias(self):
        for roster in [[member('one', 'Paula'), member('two', 'PAULA')],
                       [member('zurie', 'Paula'), member('other', 'Paula')]]:
            harness = ReplyHarness(roster)
            result = asyncio.run(harness._send_final_owned('room', 'source', '@Paula?'))
            self.assertFalse(result.success)
            self.assertIn('ambiguous', result.error)
            self.assertEqual(harness.binding.delivery_intents, {})

    def test_active_humans_resolve_locally(self):
        roster = [member('owner', 'Thorsten', 'human_owner'), member('human', 'Guest', 'human_member'), member('admin', 'Admin', 'agent_owner')]
        self.assertEqual(resolve({'roster': roster}, '@Thorsten @Guest @Admin', 'zurie'), ['owner', 'human', 'admin'])

    def test_retry_and_restart_keep_frozen_selectors_and_protocol_payload(self):
        harness = ReplyHarness([member('paula', 'Paula'), member('rotation', 'Next')], True)
        selected = copy.deepcopy(harness.run_reply('@Paula, your view?'))
        harness.roster = [member('intruder', 'Paula')]
        self.assertEqual(harness.run_reply('@Paula, your view?'), selected)
        posts = []
        def request(api, method, path, payload=None):
            if method == 'POST':
                posts.append(copy.deepcopy(payload))
                return {'id': 'posted', 'seq': 1630, 'ts': '2026-10-27T00:00:00Z'}
            return {}
        original = protocol.RoomProtocol.request
        protocol.RoomProtocol.request = request
        try:
            # The adapter's production serialization sends the selected IDs.
            api = protocol.RoomProtocol('https://fixture.invalid/api', 'fixture')
            harness.api = api
            asyncio.run(harness._post_with_fresh_context(harness.binding, 'room', '', 1628, 'source', 'ignored', 'epoch'))
            harness.binding.delivery_intents = json.loads(json.dumps(harness.binding.delivery_intents))
            restarted = DeliveryHarness()
            restarted.api = api
            asyncio.run(restarted._post_with_fresh_context(harness.binding, 'room', '', 1628, 'source', '@SomeoneElse', 'epoch'))
        finally:
            protocol.RoomProtocol.request = original
        self.assertEqual(posts[0], posts[1])
        self.assertEqual(posts[0]['recipientSelectors'], [{'kind': 'membership', 'membershipId': 'paula'}, {'kind': 'membership', 'membershipId': 'rotation'}])

    def test_followups_off_still_sends_mentions(self):
        harness = ReplyHarness([member('paula', 'Paula'), member('rotation', 'Next')], True, followups=0)
        self.assertEqual(harness.run_reply('@Paula, your view?')['recipient_membership_ids'], ['paula'])


if __name__ == '__main__':
    unittest.main()
