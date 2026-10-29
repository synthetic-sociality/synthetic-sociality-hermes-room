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
    'P15-04 active member post refusals surface without pausing',
    'P15-05 unread missing and other memberships cannot resume participation',
    'P15-06 credential failures stay terminal and shutdown cancels paused work',
    'P15-07 sibling memberships continue while one pauses',
    'P15-08 inactive enrollment pauses heartbeat and event watchers',
    'P15-09 muted turn tools and replies fail fast',
    'P15-10 active member photo download refusal surfaces',
    'P15-11 a turn offer probes promptly and coalesces concurrent offers',
    'P15-12 denied background contacts probe before pausing',
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
            instance._call = lambda _binding,op, **_request_options: asyncio.sleep(0,result=op(api))
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
                return await instance._call(binding,lambda _:api.contact(label),background=True)
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
            for status in [403, 409]:
                instance,binding,gate,clock,state,api=setup()
                state['status']='active'
                error=adapter.ProtocolError('not allowed: artifact version is not accessible',status=status)
                async def raw(_binding,op):
                    if op == 'post': raise error
                    return op(api)
                instance._raw_call=raw
                async def post():
                    with self.assertRaises(adapter.ProtocolError) as caught:
                        await instance._call(binding, 'post')
                    self.assertIs(caught.exception,error)
                await asyncio.gather(*(post() for _ in range(8)))
                self.assertEqual(state['reads'],1)
                self.assertEqual(gate.status,'active');self.assertFalse(gate.paused)
                self.assertEqual(await instance._call(binding,lambda _:api.contact('next')),'next')
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
            # A first readable active probe initialises an open gate.
            first=adapter.MembershipGate()
            self.assertTrue(await first.active(lambda:asyncio.sleep(0,result='active'),refresh=True))
            self.assertEqual(first.status,'active');self.assertFalse(first.paused)
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
            task=asyncio.create_task(instance._call(binding,lambda _:None,background=True))
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
            task=asyncio.create_task(instance._call(binding,lambda _:api.connector_enrollments('install'),background=True))
            await settle()
            self.assertTrue(gate.paused);self.assertEqual(gate.status,'muted')
            self.assertEqual(state['calls'],['enrollment'])
            # Both transports consult the same contact gate; no SSE work starts.
            async def stream(_binding,op):self.fail('SSE opened while muted')
            instance._raw_stream_call=stream
            stream_task=asyncio.create_task(instance._stream_call(binding,lambda _:None))
            heartbeat_task=asyncio.create_task(instance._call(binding,lambda _:api.contact('heartbeat'),background=True))
            await settle();self.assertEqual(state['calls'],['enrollment'])
            instance._stop.set()
            await asyncio.gather(task,stream_task,heartbeat_task,return_exceptions=True)
        asyncio.run(run())

    def test_09(self):
        async def run():
            instance,binding,gate,clock,state,api=setup()
            instance._pause_membership(binding,'muted')
            for label in ['ArtCraft','document','share_file','post']:
                with self.assertRaisesRegex(adapter.ProtocolError,'muted') as caught:
                    await asyncio.wait_for(instance._call(binding,lambda _:api.contact(label)),0.1)
                self.assertEqual(caught.exception.code,'membership_inactive')
            @adapter.turn_contacts
            async def reply():
                return await instance._call(binding,lambda _:api.contact('nested ack'),background=True)
            with self.assertRaisesRegex(adapter.ProtocolError,'muted'):
                await asyncio.wait_for(reply(),0.1)
            self.assertEqual(state['calls'],[]);self.assertEqual(state['reads'],0)
        asyncio.run(run())
        # Exercise the actual tool callbacks constructed at dispatch time.
        def prepare(instance,binding):
            original_handle=instance.handle_message
            async def handle(_message):
                instance._pause_membership(binding,'muted')
                turn=adapter.share_file._ACTIVE[binding.room_id]
                result=turn.artcraft.execute('artcraft_capabilities',{})
                self.assertEqual(result['error']['code'],'not_member')
                self.assertIn('muted',result['error']['message'])
                self.assertEqual(turn.share({'path':'never-opened.pdf'})['code'],'muted')
                from importlib import import_module
                tools=import_module(adapter.__package__+'.room_tools')
                with mock.patch.object(tools,'room_for_session',return_value=binding.room_id), \
                     mock.patch.object(tools.share_file,'active_turn',return_value=(binding.room_id,turn)), \
                     mock.patch.object(tools,'_select_room',side_effect=AssertionError('muted tool contacted Room')):
                    result=json.loads(tools.room_document({'artifactId':'artifact'},session_id='session'))
                self.assertEqual(result['code'],'membership_inactive')
                self.assertIn('muted',result['error'])
                await original_handle(_message)
            instance.handle_message=handle
        build_message(copy.deepcopy(FIXTURE['ready']),context=[copy.deepcopy(FIXTURE['source'])],attempt=cycle(),prepare=prepare)


    def test_10(self):
        # Real binary protocol boundary, including its HTTPError decoding.
        import io
        import urllib.error
        async def run():
            instance,binding,gate,clock,state,api=setup();state['status']='active'
            api.download_artifact=lambda *_: adapter.RoomProtocol(binding.base_url,binding.credential).download_artifact('room-1','artifact','version','image/png')
            async def raw(_binding,op):
                # The own-roster probe stays an in-memory fake.
                return op(api)
            instance._raw_call=raw
            failure=urllib.error.HTTPError('https://room.invalid',403,'Forbidden',{},io.BytesIO(b'{"message":"photo forbidden"}'))
            open_photo=mock.Mock(side_effect=failure)
            # Other suites reload modules under the same test package name.
            # Patch the exact production function's globals, not a newer copy.
            with mock.patch.dict(adapter.RoomProtocol.download_artifact.__globals__, {'_urlopen':open_photo}):
                with self.assertRaises(adapter.ProtocolError) as caught:
                    await instance._call(binding,lambda client:client.download_artifact(),background=True)
            open_photo.assert_called_once()
            self.assertEqual(caught.exception.status,403)
            self.assertEqual(str(caught.exception),'photo forbidden')
            self.assertFalse(gate.paused);self.assertEqual(state['reads'],1)
        asyncio.run(run())

    def test_11(self):
        async def run():
            instance,binding,gate,clock,state,api=setup()
            instance._pause_membership(binding,'muted')
            state['status']='active'
            async def raw(_binding,op):
                await asyncio.sleep(0)
                return op(api)
            instance._raw_call=raw
            # The production offer entry rechecks before the background wait.
            consumed=[]
            async def consume(_binding,event): consumed.append(event)
            instance._consume_locked=consume
            ready=copy.deepcopy(FIXTURE['ready'])
            ready['payload']['membershipId']=binding.membership_id
            await asyncio.gather(*(instance._consume(binding,ready) for _ in range(8)))
            self.assertEqual(state['reads'],1);self.assertFalse(gate.paused)
            self.assertEqual(len(consumed),8)
        asyncio.run(run())

    def test_only_assigned_events_force_a_roster_probe(self):
        async def run():
            instance,binding,gate,clock,state,api=setup()
            state['status']='active'
            async def raw(_binding,op): return op(api)
            instance._raw_call=raw
            consumed=[]
            async def consume(_binding,event): consumed.append(event)
            instance._consume_locked=consume

            def message(role='participant_agent', actor='peer', **payload):
                return {'type':'message.posted','actorRole':role,'actorId':actor,'payload':payload}
            def command(name='ask', role='human_owner', **payload):
                return {'type':'human.command','actorRole':role,'payload':{'command':{'command':name,**payload}}}
            def ready(member):
                return {'type':'discussion.cycle_attempt_ready','payload':{'membershipId':member}}

            cases=[
                ('another member message',message(resolvedRecipientMembershipIds=['other']),False),
                ('assigned message',message(resolvedRecipientMembershipIds=['member-1']),True),
                ('own post',message(actor='member-1',resolvedRecipientMembershipIds=['member-1']),False),
                ('another attempt',ready('other'),False),
                ('own attempt',ready('member-1'),True),
                ('another addressed ask',command(resolvedTargetMembershipIds=['other'],targetSelectors=[{'membershipId':'member-1'}]),False),
                ('own addressed ask',command(resolvedTargetMembershipIds=['member-1'],targetSelectors=[{'membershipId':'other'}]),True),
                ('human summary',command('summarize'),True),
                ('unaddressed ask',command(),False),
                ('empty ask targets',command(resolvedTargetMembershipIds=[]),False),
                ('existing cycle ask',{**command(resolvedTargetMembershipIds=['member-1']),'payload':{'cycleId':'cycle','command':{'command':'ask','resolvedTargetMembershipIds':['member-1']}}},False),
                ('other human command',command('hold'),False),
                ('nonhuman ask',command(role='participant_agent',resolvedTargetMembershipIds=['member-1']),False),
                ('general agent post',message(resolvedRecipientMembershipIds=[]),True),
                ('cycle post without recipients',message(cycleId='cycle',resolvedRecipientMembershipIds=[]),False),
                ('human open message',message('human_member'),True),
                ('human addressed elsewhere',message('agent_owner',resolvedRecipientMembershipIds=['other']),False),
                ('human resolved targets win',message('human_owner',resolvedRecipientMembershipIds=['other'],recipientSelectors=[{'kind':'everyone'}]),False),
                ('human unresolved selector',message('human',resolvedRecipientMembershipIds=[],recipientSelectors=[{'membershipId':'member-1'}]),False),
                ('human selector fallback',message('human',recipientSelectors=[{'membershipId':'member-1'}]),True),
                ('room master message',message('room_master',resolvedRecipientMembershipIds=['other']),True),
                ('admin message',message('admin'),True),
                ('topic change',{'type':'topic.changed'},False),
            ]
            for label,event,assigned in cases:
                with self.subTest(event=label):
                    before=state['reads']
                    await instance._consume(binding,event)
                    self.assertEqual(state['reads']-before,int(assigned))
                    self.assertIs(consumed[-1],event)
        asyncio.run(run())

    def test_tool_thread_probe_is_retained_and_failure_logged_without_secrets(self):
        for outcome in ['failure','success','cancelled']:
            with self.subTest(outcome=outcome):
                secret='fixture-probe-secret-never-log'
                refusal=adapter.ProtocolError('tool refused',status=403)
                def prepare(instance,binding):
                    original_handle=instance.handle_message
                    started=asyncio.Event()
                    release=asyncio.Event()
                    async def probe(_binding,**options):
                        self.assertIs(_binding,binding)
                        self.assertEqual(options,{'refresh':True})
                        started.set()
                        await release.wait()
                        if outcome=='failure':
                            raise adapter.ProtocolError(secret,status=403,code='credential_revoked')
                        return True
                    instance._membership_active=probe
                    async def handle(_message):
                        debug.reset_mock()
                        turn=adapter.share_file._ACTIVE[binding.room_id]
                        def denied(): raise refusal
                        def tool():
                            with self.assertRaises(adapter.ProtocolError) as caught:
                                turn.contact(denied)
                            self.assertIs(caught.exception,refusal)
                        await asyncio.to_thread(tool)
                        await asyncio.wait_for(started.wait(),1)
                        tasks=list(instance._membership_probe_tasks)
                        self.assertEqual(len(tasks),1)
                        self.assertFalse(tasks[0].done())
                        if outcome=='cancelled': tasks[0].cancel()
                        else: release.set()
                        await asyncio.gather(*tasks,return_exceptions=True)
                        await settle()
                        self.assertEqual(instance._membership_probe_tasks,set())
                        await original_handle(_message)
                    instance.handle_message=handle
                with mock.patch.object(adapter.logger,'debug') as debug:
                    build_message(copy.deepcopy(FIXTURE['ready']),context=[copy.deepcopy(FIXTURE['source'])],attempt=cycle(),prepare=prepare)
                if outcome=='failure':
                    debug.assert_called_once_with('Room tool membership probe failed (%s)','ProtocolError')
                    self.assertNotIn(secret,str(debug.call_args))
                else:
                    debug.assert_not_called()

    def test_12(self):
        async def run():
            instance,binding,gate,clock,state,api=setup()
            async def raw(_binding,op): return op(api)
            instance._raw_call=raw
            state['status']='active'
            await instance._membership_active(binding,refresh=True)
            state['status']='muted';clock[0]+=60
            state['denied']=adapter.ProtocolError('not allowed',status=403)
            task=asyncio.create_task(instance._call(binding,lambda _:api.contact('events'),background=True))
            await settle()
            self.assertTrue(gate.paused);self.assertEqual(gate.status,'muted')
            self.assertEqual(state['reads'],2);self.assertEqual(state['calls'],['events'])
            state['status']='active';clock[0]+=60
            self.assertTrue(await instance._membership_active(binding))
            self.assertEqual(await asyncio.wait_for(task,1),'events')
        asyncio.run(run())


class MutedEnrollmentLockTests(unittest.TestCase):
    """1.0.99: one Room's membership never stops another Room's heartbeat.

    Live 2026-10-29 (Hermes 1.0.98, 14 Rooms, one muted membership): the muted
    Room's enrollment listing waited for an unmute inside the process-wide
    enrollment lock, and every other Room's heartbeat and event watcher queued
    behind that lock until the leases expired. The real participation gate and
    `_call` path run here; only the Room transport is a double (P8 idle Room).
    """

    def run_rooms(self, *, mute_at, until, hang=None):
        from test_parity_p8 import IdleRoom, REAL_SLEEP, START, VirtualClock, binding_for, idle_instance, simulated, start, stop
        clock = VirtualClock()
        room = IdleRoom(clock)
        active, other = binding_for('a1', 'inst-A'), binding_for('m1', 'inst-A')
        observed = {}
        released = asyncio.Event()

        async def body():
            instance = idle_instance(room, [active, other])
            del instance._call  # the production _call -> _membership_contact path

            async def raw(binding, operation):
                await REAL_SLEEP(0)
                if hang is not None and binding.room_id == 'm1' and clock.now >= START + hang:
                    probe = types.SimpleNamespace(connector_enrollments=lambda _installation: 'listing')
                    try:
                        is_listing = operation(probe) == 'listing'
                    except Exception:  # noqa: BLE001 - another route
                        is_listing = False
                    if is_listing:
                        # The listing of m1 never answers (a stalled server).
                        observed.setdefault('hung', clock.now)
                        await released.wait()
                return operation(room.api(binding))
            instance._raw_call = raw
            # The active Room runs (registration, listing, heartbeats) before
            # the other one starts, as in the live gateway.
            await start(instance, [active])
            await clock.run_until(START + 30)
            if mute_at == 0:
                room.muted.add('m1')
            await start(instance, [other])
            if mute_at:
                await clock.run_until(START + mute_at)
                room.muted.add('m1')
            await clock.run_until(START + until)
            observed['gate'] = instance._membership_gate(other)
            observed['watching'] = not instance._tasks['m1'].done()
            observed['lock_held'] = instance._enrollment_lock.locked()
            released.set()
            await stop(instance)

        with simulated(clock):
            asyncio.run(body())
        return room, observed, START

    def assert_beats(self, room, room_id, since, until):
        beats = room.times('heartbeat', room_id, since=since, until=until)
        # One heartbeat every 15 s from `since` (a beat time) until before `until`.
        self.assertEqual(len(beats), int((until - since - 1e-9) // 15) + 1, (room_id, beats))
        self.assertEqual({round(later - earlier, 6) for earlier, later in zip(beats, beats[1:])}, {15.0})

    def test_muted_enrollment_listing_never_stops_sibling_heartbeats(self):
        for label, mute_at in [('muted before start', 0), ('muted while running', 100)]:
            with self.subTest(label):
                room, observed, start_at = self.run_rooms(mute_at=mute_at, until=400)
                muted_listing = room.times('enrollments', 'm1', since=start_at + mute_at)
                # The muted listing paused m1 (P15-2) and m1 waits for an unmute ...
                self.assertEqual(len(muted_listing), 1, room.calls)
                self.assertTrue(observed['gate'].paused)
                self.assertEqual(observed['gate'].status, 'muted')
                self.assertTrue(observed['watching'])
                self.assertEqual(room.times('heartbeat', 'm1', since=muted_listing[0] + 1), [])
                # ... outside the lock: a1 keeps its heartbeat and the lineage's listing.
                self.assertFalse(observed['lock_held'])
                self.assert_beats(room, 'a1', start_at + 15, start_at + 400)
                listings = room.times('enrollments', 'a1', since=muted_listing[0] + 1)
                self.assertGreaterEqual(len(listings), 4, listings)

    def test_a_stalled_enrollment_listing_never_delays_a_heartbeat(self):
        # m1's own listing stalls inside the lock from +100 on: neither its own
        # heartbeat nor a1's waits for the listing or the lock.
        room, observed, start_at = self.run_rooms(mute_at=None, until=400, hang=100)
        self.assertIn('hung', observed)
        self.assertTrue(observed['lock_held'])
        self.assert_beats(room, 'a1', start_at + 15, start_at + 400)
        self.assert_beats(room, 'm1', start_at + 45, start_at + 400)
