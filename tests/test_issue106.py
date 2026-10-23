"""ISSUE-106 offline tests: execute production methods without importing a host.

AST extraction keeps host imports from accessing anything outside this checkout.
The selection, freeze, retry and serialization implementations are unmodified.
"""
from __future__ import annotations
import ast
import asyncio
import copy
import importlib.util
import json
import logging
from pathlib import Path
import re
import runpy
import sys
import time
import types
import unittest

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = json.loads((ROOT / 'tests/fixtures/issue-106.json').read_text())
spec = importlib.util.spec_from_file_location('issue106_protocol', ROOT / 'protocol.py')
protocol = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = protocol
spec.loader.exec_module(protocol)

share_spec = importlib.util.spec_from_file_location('issue106_share_file', ROOT / 'share_file.py')
share_file = importlib.util.module_from_spec(share_spec)
share_spec.loader.exec_module(share_file)

# Only local source and standard library. No gateway, plugins or user config.
tree = ast.parse((ROOT / 'adapter.py').read_text())
names = {'_restore_escaped_json_layout', '_decode_compatibility_body',
         '_recover_malformed_contribute_body', '_DuplicateJSONKey',
         '_reject_duplicate_json_pairs', '_contains_room_envelope_fragment',
         'extract_visible_body', 'extract_contribution_type', '_cycle_phase_instruction', '_valid_canonical_timestamp',
         '_with_idempotency_key', '_with_logical_contribution_id',
         '_with_message_payload_dialect'}
assignments = {'_JSON_STRING_TOKEN', '_FENCE', '_ROOM_JSON_DECODER', '_ROOM_ENVELOPE_FRAGMENT',
               '_JSON_OBJECT_KEY', '_ESCAPED_JSON_OBJECT_KEY'}
methods = {'_select_delivery_intent', '_post_with_fresh_context', '_persisted_payload_dialect', '_record_canonical_delivery'}
from datetime import datetime
ns = dict(share_file=share_file, copy=copy, datetime=datetime, json=json, re=re, time=time, asyncio=asyncio, logger=logging.getLogger(__name__),
          ProtocolError=protocol.ProtocolError, stable_key=protocol.stable_key,
          _cycle_delivery_payload=lambda *a, **kw: {},
          _post_cycle_authority_attempt=lambda *a: None)
selected = [ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0)]
for node in tree.body:
    if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names:
        selected.append(node)
    elif isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id in assignments for t in node.targets):
        selected.append(node)
    elif isinstance(node, ast.ClassDef) and node.name == 'SyntheticSocialityAdapter':
        selected.extend(n for n in node.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in methods)
exec(compile(ast.fix_missing_locations(ast.Module(body=selected, type_ignores=[])), str(ROOT/'adapter.py'), 'exec'), ns)

class Harness:
    _select_delivery_intent = ns['_select_delivery_intent']
    _post_with_fresh_context = ns['_post_with_fresh_context']
    _persisted_payload_dialect = staticmethod(ns['_persisted_payload_dialect'])
    _record_canonical_delivery = ns['_record_canonical_delivery']
    _intent_binding = lambda self, binding: {}
    _intent_matches_binding = lambda *a: True
    _persist_binding = lambda *a: True
    _reply_target = lambda self, room, body: ('', body)
    async def _call(self, binding, operation):
        return operation(self.api)

class Issue106Tests(unittest.TestCase):
    def test_i106_01_04_explicit_and_omitted(self):
        for case in FIXTURE['cases']:
            with self.subTest(case=case['id']):
                envelope = {'action':'contribute', 'body':case['text']}
                if case['type'] is not None:
                    envelope['contributionType'] = case['type']
                for raw in [json.dumps(envelope), '```json\n'+json.dumps(envelope)+'\n```']:
                    self.assertEqual(ns['extract_visible_body'](raw), case['text'])
                    self.assertEqual(ns['extract_contribution_type'](raw), case['type'] or 'claim')
        self.assertEqual(ns['extract_contribution_type']('Is this a question?'), 'claim')

    def test_i106_07_reject_unsupported(self):
        for kind in FIXTURE['rejected']:
            raw = json.dumps({'action':'contribute','body':'Same text?', 'contributionType':kind})
            self.assertIsNone(ns['extract_visible_body'](raw))
            with self.assertRaises(ValueError):
                ns['extract_contribution_type'](raw)
            with self.assertRaises(ValueError):
                protocol.RoomProtocol('https://fixture.invalid/api','fixture').post_message(
                    'room','turn',0,'source','Same text?',payload_dialect='v1',contribution_type=kind)

    def test_i106_05_06_frozen_retry_restart_and_legacy(self):
        for kind in ['claim','question','response',None]:
            with self.subTest(kind=kind):
                binding = types.SimpleNamespace(room_id='room',membership_id='self',delivery_intents={},share_uploads={},delivery_lifecycle={},turn_sequences={},message_payload_dialect='v1')
                instance = Harness()
                args = (binding,'source','Same text?','source',['peer'],'open',1,'epoch',None)
                kwargs = {} if kind is None else {'contribution_type':kind}
                selection = instance._select_delivery_intent(*args, **kwargs)
                self.assertEqual(selection['contribution_type'],kind or 'claim')
                if kind is None: # old selection with legacy classification frozen
                    selection.pop('contribution_type')
                self.assertEqual(instance._select_delivery_intent(*args, contribution_type='response'),selection)
                expected = kind or 'question'
                posts=[]
                api=protocol.RoomProtocol('https://fixture.invalid/api','fixture')
                def request(method,path,payload):
                    posts.append(copy.deepcopy(payload))
                    if len(posts) == 1:
                        raise protocol.ProtocolError('ambiguous fixture disconnect', retryable=True)
                    return {'id':'canonical','seq':2,'ts':'2026-10-21T00:00:00Z','payload':dict(payload)}
                # Overrides create clones, so intercept the class transport.
                original=protocol.RoomProtocol.request
                protocol.RoomProtocol.request=lambda self,*a: request(*a)
                instance.api=api
                try:
                    asyncio.run(instance._post_with_fresh_context(binding,'room','',1,'source','Same text?','epoch'))
                    if kind is None:
                        binding.delivery_intents['source']['post'].pop('contribution_type')
                    binding.delivery_intents=json.loads(json.dumps(binding.delivery_intents))
                    restarted=Harness(); restarted.api=api
                    asyncio.run(restarted._post_with_fresh_context(binding,'room','',1,'source','Changed text','epoch'))
                    self.assertEqual(posts[0],posts[1])
                    self.assertEqual(posts[1],posts[2])
                    self.assertEqual(posts[0]['contributionType'],expected)
                    self.assertEqual(posts[0]['body'],'Same text?')
                    self.assertTrue(posts[0]['recipientSelectors'])
                    frozen = copy.deepcopy(binding.delivery_intents['source']['post'])
                    restarted._record_canonical_delivery(binding, 'source', {'id':'canonical','seq':2,'ts':'2026-10-21T00:00:00Z'}, completion=None)
                    self.assertEqual(binding.delivery_intents['source']['post'], frozen)
                    self.assertEqual(binding.delivery_intents['source']['delivery_state'], 'posted')
                finally:
                    protocol.RoomProtocol.request=original

    def test_i106_08_moderator_fixture(self):
        source=ast.parse((ROOT/'tests/test_parity_p7c.py').read_text())
        instruction=next(ast.literal_eval(n.value) for n in source.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='RESPONSE_INSTRUCTION' for t in n.targets))
        self.assertEqual(ns['_cycle_phase_instruction']({}, {}, {'phase':'response','phaseInstruction':instruction}), ('response',instruction))
        for text in ['every sub-question','by name','may include a short interim summary','one short clarifying question']:
            self.assertIn(text,instruction)


    def test_i106_09_procedural_sentence(self):
        instruction = next(ast.literal_eval(n.value) for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='ROOM_RESPONSE_INSTRUCTION' for t in n.targets))
        self.assertIn(FIXTURE['formatSentence'], instruction)
        self.assertEqual(json.loads((ROOT/'conformance.json').read_text())['issue106Cases']['formatSentence'],FIXTURE['formatSentence'])
        self.assertIn('ROOM_RESPONSE_INSTRUCTION', (ROOT/'adapter.py').read_text().split('def _build',1)[-1])

    def test_i106_10_shared_matrix(self):
        for item in FIXTURE['matrix']:
            with self.subTest(input=item['input']):
                if item['outcome']=='fail':
                    with self.assertRaises(ValueError): ns['extract_contribution_type'](item['raw'])
                else:
                    self.assertEqual(ns['extract_visible_body'](item['raw']),item['body'])
                    if item['outcome']=='post': self.assertEqual(ns['extract_contribution_type'](item['raw']),item['type'])

    def test_i106_11_external_literal_and_legacy_replay(self):
        source=ast.parse((ROOT/'room_tools.py').read_text())
        method=next(n for n in source.body if isinstance(n,ast.FunctionDef) and n.name=='room_post')
        for mode in ['open','coordinated']:
            for legacy in [False,True]:
                with self.subTest(mode=mode,legacy=legacy):
                    body=FIXTURE['externalText']; posts=[]; stored={}
                    binding=types.SimpleNamespace(room_id='room',membership_id='self',base_url='https://fixture.invalid/api',credential='fixture')
                    old={'body':body,'roomId':'room','recipientMembershipIds':['peer'],'coordinationMode':mode,'messagePayloadDialect':'v1','postObserved':1,'epochId':'epoch'} if legacy else None
                    class API:
                        def room_policy(self,*a): return {'coordinationMode':mode}
                        def room_state(self,*a): return {'headSeq':1,'activeEpoch':{'id':'epoch'}}
                        def request_turn(self,*a): return {'turnId':'turn'}
                        def post_message(self,*a,**kw):
                            posts.append((a,kw)); return {'id':'canonical','seq':2}
                    def write(key,value,**kw): stored.update(copy.deepcopy(value)); return value
                    scope=dict(Any=object, is_room_context=lambda **kw:False,_result=lambda v:v,
                        _select_room=lambda *a:(binding,{},[]), RoomProtocol=lambda *a:API(),
                        _origin_id=lambda *a:'source',_read_action=lambda *a:old,
                        _inline_agent_recipients=lambda *a:['peer'],_read_payload_dialect=lambda *a:'v1',
                        _write_action=write,_retryable_call=lambda op:op(),_with_message_payload_dialect=lambda api,*a:api,
                        _with_logical_contribution_id=lambda api,*a:api,stable_key=lambda *a,**kw:'key',
                        _wait_for_turn=lambda api,binding,turn,*a:turn,_finish_action=lambda *a:None,
                        ProtocolError=protocol.ProtocolError)
                    module=ast.Module(body=[ast.ImportFrom(module='__future__',names=[ast.alias(name='annotations')],level=0),method],type_ignores=[])
                    exec(compile(ast.fix_missing_locations(module),str(ROOT/'room_tools.py'),'exec'),scope)
                    result=scope['room_post']({'body':body})
                    self.assertTrue(result['success'],result)
                    self.assertEqual(posts[0][0][4],body)
                    self.assertEqual(posts[0][1].get('contribution_type'),'question' if legacy else 'claim')
                    if not legacy:self.assertEqual(stored['contributionType'],'claim')

    def test_packaging_covers_all_tests_and_fixtures(self):
        packager=runpy.run_path(str(ROOT/'tools/package-release.py'))
        shipped={p.relative_to(ROOT).as_posix() for p in (ROOT/'tests').rglob('*') if p.is_file() and p.suffix in {'.py','.json','.txt'} and '__pycache__' not in p.parts}
        self.assertEqual(sorted(shipped-set(packager['PAYLOAD'])),[])

if __name__ == '__main__': unittest.main()
