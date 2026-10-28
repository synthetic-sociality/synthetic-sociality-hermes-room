"""ISSUE-138/139: real source rendering and shared watcher contact boundary."""
from __future__ import annotations
import asyncio
import copy
import json
import types
import unittest
from unittest import mock
from test_delivery_lifecycle import ROOT, adapter
from test_parity_p1a import build_message, cycle

FIXTURE = json.loads((ROOT/'tests/fixtures/issues-138-139.json').read_text())
CASES = [
    'P15-01 loaded ask source reaches the model with resolved target addressing',
    'P15-02 older ask source resolves within the bounded epoch scan',
    'P15-03 all watcher contacts pause together and resume on active',
    'P15-04 forbidden watcher pauses all contacts until membership changes',
    'P15-05 unread missing and other memberships cannot resume participation',
    'P15-06 credential failures stay terminal and shutdown cancels paused work',
    'P15-07 sibling memberships continue while one pauses',
    'P15-08 inactive enrollment pauses heartbeat and event watchers',
]


def setup():
    instance = object.__new__(adapter.SyntheticSocialityAdapter)
    instance._stop = asyncio.Event()
    binding = adapter.RoomBinding('https://room.invalid/api', 'room-1', 'member-1', 'fixture', installation_id='install')
    gate = instance._membership_gate(binding)
    clock = [1000.0]
    gate.clock = lambda: clock[0]
    state = {'status': 'muted', 'reads': 0, 'calls': [], 'denied': None}
    class API:
        def room_state(self, room_id):
            state['reads'] += 1
            if isinstance(state['status'], Exception):
                raise state['status']
            return {'roster': [{'membershipId': 'member-1', 'status': state['status']}]}
        def contact(self, label):
            state['calls'].append(label)
            if state['denied']:
                error, state['denied'] = state['denied'], None
                raise error
            return label
        def connector_enrollments(self, installation):
            state['calls'].append('enrollment')
            raise adapter.ProtocolError('inactive', status=401, code='connector_not_active')
    api = API()
    return instance, binding, gate, clock, state, api


async def settle():
    for _ in range(30):
        await asyncio.sleep(0)


class P15Tests(unittest.TestCase):
    def test_conformance(self):
        conf = json.loads((ROOT/'conformance.json').read_text())
        self.assertEqual(conf['parityP15Cases'], CASES)
        self.assertEqual(conf['parityP15Contract'], 'docs/delivery-lifecycle-contract-v1.md#p15--ask-sources-and-membership-watcher-pause')
        section = (ROOT/'docs/delivery-lifecycle-contract-v1.md').read_text().split('## P15 — Ask sources and membership watcher pause')[1]
        self.assertEqual([line[2:] for line in section.splitlines() if line.startswith('- ')], CASES)

    def test_01(self):
        source, ready = copy.deepcopy(FIXTURE['source']), copy.deepcopy(FIXTURE['ready'])
        message = build_message(ready, context=[source], attempt=cycle(), roster=[{'membershipId':'owner-1','displayName':'Thorsten'}])
        self.assertIn('Thorsten: '+source['payload']['command']['arguments']['instruction'], message.text)
        self.assertNotIn('not available to the connector', message.text)
        # The original event's server-resolved targets, not the misleading selector.
        async def run():
            instance = object.__new__(adapter.SyntheticSocialityAdapter)
            posts = []
            api = types.SimpleNamespace(
                room_state=lambda _: {'activeEpoch':{'id':'epoch-2'},'roster':[{'membershipId':mid,'status':'active','role':'participant_agent'} for mid in ['oz4hxxig9zrj4ku','other']]},
                room_policy=lambda _: {},
                start_discussion_cycle=lambda _room,payload: posts.append(payload) or {},
            )
            instance._call = lambda _binding,op: asyncio.sleep(0,result=op(api))
            binding = adapter.RoomBinding('https://room.invalid/api','room','oz4hxxig9zrj4ku','fixture')
            await instance._ensure_discussion_cycle(binding,source)
            self.assertEqual([member['membershipId'] for member in posts[0]['roster']],['oz4hxxig9zrj4ku'])
        asyncio.run(run())

    def test_02(self):
        source, ready = copy.deepcopy(FIXTURE['source']), copy.deepcopy(FIXTURE['ready'])
        ready['seq'] = 1613
        kept=[]
        message = build_message(ready, context=[source], attempt=cycle(), starts_at=1200, keep_instance=kept)
        self.assertIn(ready['id'],kept[0][0]._cycle_command_sources)
        self.assertIn(source['payload']['command']['arguments']['instruction'], message.text)
        self.assertNotIn('not available to the connector', message.text)
        reads=[]
        self.assertIsNone(adapter._cycle_source_message(lambda after: reads.append(after) or {'events':[source]},source['id'],before_seq=1613,active_epoch_starts_at=0,types=adapter.MISSED_SOURCE_EVENT_TYPES))
        self.assertEqual(reads,[])
        self.assertIs(adapter._cycle_prompt_event(ready,ready['payload'],[{**source,'id':'different'}]),ready)

    def test_03(self):
        async def run():
            instance,binding,gate,clock,state,api=setup()
            gate.interval=0.01
            instance._pause_membership(binding,'muted')
            labels=['heartbeat','registration','enrollment','document','photo','ack','attempt renewal','attempt settle','activity','recovery']
            async def worker(label):
                return await instance._call(binding,lambda _:api.contact(label))
            with mock.patch.object(adapter,'RoomProtocol',lambda *_:api):
                tasks=[asyncio.create_task(worker(label)) for label in labels]
                await settle()
                self.assertEqual(state['calls'],[])
                self.assertEqual(state['reads'],0)
                clock[0]+=60
                self.assertEqual(await asyncio.gather(*(instance._membership_active(binding) for _ in labels)),[False]*len(labels))
                self.assertEqual(state['reads'],1)
                state['status']='active'; clock[0]+=60
                self.assertTrue(await instance._membership_active(binding))
                self.assertEqual(await asyncio.wait_for(asyncio.gather(*tasks),1),labels)
                self.assertEqual(binding.cursor,0)
        asyncio.run(run())

    def test_04(self):
        async def run():
            instance,binding,gate,clock,state,api=setup()
            gate.interval=0.01
            state['denied']=adapter.ProtocolError('the authenticated identity is not allowed to perform this operation',status=403)
            async def raw(_binding,op): return op(api)
            instance._raw_call=raw
            task=asyncio.create_task(instance._call(binding,lambda _:api.contact('events')))
            await settle()
            self.assertTrue(gate.paused);self.assertEqual(state['calls'],['events']);self.assertEqual(state['reads'],1)
            self.assertFalse(await instance._membership_active(binding))
            self.assertEqual(state['calls'],['events'])
            state['status']='active';clock[0]+=60
            self.assertTrue(await instance._membership_active(binding))
            self.assertEqual(await asyncio.wait_for(task,1),'events')
            # A refusal with a non-403 status still classifies the same way.
            self.assertTrue(instance._membership_denied(adapter.ProtocolError('not allowed',status=409)))
        asyncio.run(run())

    def test_05(self):
        async def run():
            gate=adapter.MembershipGate(clock=lambda:clock[0]);clock=[1000.0]
            gate.pause('muted')
            for status in ['', '', 'removed', 'pending']:
                clock[0]+=60
                self.assertFalse(await gate.active(lambda:asyncio.sleep(0,result=status)))
            clock[0]+=60
            self.assertTrue(await gate.active(lambda:asyncio.sleep(0,result='active')))
            gate.pause();clock[0]+=60
            self.assertFalse(await gate.active(lambda:asyncio.sleep(0,result='active')))
            clock[0]+=60
            self.assertFalse(await gate.active(lambda:asyncio.sleep(0,result='active')))
        asyncio.run(run())

    def test_06(self):
        async def run():
            instance,binding,gate,clock,state,api=setup()
            for code in ['credential_revoked','credential_expired']:
                error=adapter.ProtocolError('terminal',status=403,code=code)
                self.assertFalse(instance._membership_denied(error))
                async def failed(_binding,op):raise error
                instance._raw_call=failed
                with self.assertRaises(adapter.ProtocolError):await instance._call(binding,lambda _:None)
            instance._pause_membership(binding,'muted')
            task=asyncio.create_task(instance._call(binding,lambda _:None))
            await settle();instance._stop.set()
            with self.assertRaises(asyncio.CancelledError):await task
        asyncio.run(run())

    def test_07(self):
        async def run():
            instance,binding,gate,clock,state,api=setup()
            instance._pause_membership(binding,'muted')
            sibling=copy.copy(binding);sibling.room_id='room-2'
            async def raw(_binding,op):return op(api)
            instance._raw_call=raw
            self.assertEqual(await instance._call(sibling,lambda _:api.contact('sibling')),'sibling')
            self.assertTrue(gate.paused)
            replacement=copy.copy(binding);replacement.credential='replacement'
            self.assertFalse(instance._membership_gate(replacement).paused)
        asyncio.run(run())

    def test_08(self):
        async def run():
            instance,binding,gate,clock,state,api=setup()
            async def raw(_binding,op):return op(api)
            instance._raw_call=raw
            task=asyncio.create_task(instance._call(binding,lambda _:api.connector_enrollments('install')))
            await settle()
            self.assertTrue(gate.paused);self.assertEqual(gate.status,'muted')
            self.assertEqual(state['calls'],['enrollment'])
            # Both transports consult the same contact gate; no SSE work starts.
            async def stream(_binding,op):self.fail('SSE opened while muted')
            instance._raw_stream_call=stream
            stream_task=asyncio.create_task(instance._stream_call(binding,lambda _:None))
            heartbeat_task=asyncio.create_task(instance._call(binding,lambda _:api.contact('heartbeat')))
            await settle();self.assertEqual(state['calls'],['enrollment'])
            instance._stop.set()
            await asyncio.gather(task,stream_task,heartbeat_task,return_exceptions=True)
        asyncio.run(run())
