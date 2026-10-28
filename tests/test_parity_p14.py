"""P14: fake HTTP boundary; real Room client, registry and turn authority."""
import hashlib
import io
import json
import threading
import time
import types
import unittest
import urllib.error
from unittest.mock import patch
from support import ROOT, load_module

art = load_module('artcraft')
share = load_module('share_file')
protocol = load_module('protocol')
CASES = [
    'P14-01 five tools register identical closed schemas',
    'P14-02 each tool succeeds through the authenticated Room client',
    'P14-03 each stable server error passes through unchanged for each tool',
    'P14-04 active Room turn alone supplies room identity',
    'P14-05 outside ended sealed and foreign turns refuse without IO',
    'P14-06 create and run generate unique keys and preserve supplied keys',
    'P14-07 job polling and in-flight reads are bounded by 20 seconds',
    'P14-08 descriptions name artifact kinds and prohibit download or execution',
    'P14-09 results expose no credentials or host paths',
    'P14-10 invalid arguments fail before IO',
    'P14-11 run resolves app and file name and rechecks turn authority',
    'P14-12 registration on a fresh host includes tools for existing sessions',
    'P14-13 transport failures return a fixed upstream error',
]
ARGS = {
    'artcraft_capabilities': {'app': 'cadcraft'},
    'artcraft_create': {'app': 'cadcraft', 'file_name': 'plan.dxf', 'calls': [{'tool': 'draw', 'arguments': {}}]},
    'artcraft_run': {'artifact_id': 'artifact', 'base_revision': 'r1', 'operation': 'draw', 'inputs': []},
    'artcraft_job': {'job_id': 'job'},
    'artcraft_artifact': {'artifact_id': 'artifact'},
}
ARTIFACT = {'artifactId': 'artifact', 'app': 'cadcraft', 'name': 'plan.dxf', 'revisions': [{'revision': 'r1', 'createdAt': '2026-10-28T00:00:00Z', 'by': 'agent'}], 'cardUrl': '/?roomId=room01234567890#artcraft-art012345678901-ver012345678901'}

class FakeServer:
    def __init__(self):
        self.calls = []
        self.body_sizes = []
        self.failure = None
        self.answer = None
        self.hook = lambda: None
    def open(self, request, timeout):
        body = json.loads(request.data) if request.data else None
        self.body_sizes.append(len(request.data) if request.data else 0)
        self.calls.append((request.get_method(), request.full_url, body, timeout))
        assert request.get_header('Authorization') == 'Bearer private-credential'
        if self.failure:
            payload = {'error': {'code': self.failure, 'message': 'Exact server explanation: ' + self.failure}}
            raise urllib.error.HTTPError(request.full_url, 403, 'refused', {}, io.BytesIO(json.dumps(payload).encode()))
        if self.answer is not None: answer = self.answer
        elif '/capabilities' in request.full_url: answer = {'available': True, 'apps': [{'app': 'cadcraft', 'operations': ['draw']}], 'device': 'ready', 'schema': {'operations': ['draw']}}
        elif '/artifacts/' in request.full_url: answer = ARTIFACT
        else: answer = {'jobId': 'job', 'status': 'queued' if request.get_method() == 'POST' else 'done'}
        self.hook()
        return Response(answer)

class Response(io.BytesIO):
    status = 200
    def __init__(self, value): super().__init__(json.dumps(value).encode())

class P14Tests(unittest.TestCase):
    def setUp(self):
        self.server = FakeServer()
        self.patch = patch.object(protocol, '_urlopen', self.server.open)
        self.patch.start(); self.addCleanup(self.patch.stop)
        self.turn = types.SimpleNamespace(active=True, gate=threading.RLock(), sealed=lambda: False)
        share.begin('bound-room', self.turn, 'host-session')
        self.addCleanup(share.end, 'bound-room', self.turn)
        self.turn.artcraft = art.ArtCraftTurn('bound-room', self.request, lambda: self.turn.active and not self.turn.sealed(), 'private-credential')
        self.registry = {}
        art.register(types.SimpleNamespace(register_tool=lambda **kw: self.registry.__setitem__(kw['name'], kw)))
    def request(self, method, path, body, timeout):
        return protocol.RoomProtocol('https://room.invalid/api', 'private-credential', timeout=timeout).request(method, path, body)
    def call(self, name, args=None, session='host-session'):
        return json.loads(self.registry[name]['handler'](ARGS[name] if args is None else args, session_id=session))
    def test_01(self):
        conf = json.loads((ROOT/'conformance.json').read_text())
        self.assertEqual(conf['parityP14Cases'], CASES)
        self.assertEqual(conf['parityP14Contract'], 'docs/delivery-lifecycle-contract-v1.md#p14--artcraft-agent-tools')
        self.assertEqual(conf['parityP14ToolsSHA256'], hashlib.sha256((ROOT/'artcraft-tools.json').read_bytes()).hexdigest())
        contract=(ROOT/'docs/delivery-lifecycle-contract-v1.md').read_text().split('## P14 — ArtCraft agent tools')[1]
        for case in CASES: self.assertIn(case, contract)
        self.assertEqual(set(self.registry), set(ARGS))
        for name, reg in self.registry.items():
            self.assertEqual(reg['schema'], art.TOOLS[name])
            self.assertFalse(reg['schema']['parameters']['additionalProperties'])
            self.assertNotIn('roomId',reg['schema']['parameters']['properties'])
        self.assertIn('artcraft.register(ctx)', (ROOT/'adapter.py').read_text())
        self.assertIn('turn.artcraft = artcraft.ArtCraftTurn', (ROOT/'adapter.py').read_text())
        self.assertIn('artcraft-tools.json', (ROOT/'install.sh').read_text())
        for name in ARGS: self.assertIn('  - '+name, (ROOT/'plugin.yaml').read_text())
        for f in ('artcraft.py', 'artcraft-tools.json', 'tests/test_parity_p14.py'):
            self.assertIn('"'+f+'"', (ROOT/'tools/package-release.py').read_text())
    def test_02(self):
        for name in ARGS:
            with self.subTest(name=name): self.assertNotIn('error', self.call(name))
        methods=[x[0] for x in self.server.calls]
        self.assertEqual(methods,['GET','POST','GET','POST','GET','GET'])
        self.assertTrue(self.server.calls[0][1].endswith('/api/rooms/bound-room/artcraft/agent/capabilities?app=cadcraft'))
        create=self.server.calls[1][2]; run=self.server.calls[3][2]
        self.assertEqual(create['kind'],'create'); self.assertEqual(create['fileName'],'plan.dxf')
        self.assertEqual(run['kind'],'run'); self.assertEqual(run['app'],'cadcraft'); self.assertEqual(run['fileName'],'plan.dxf')
        self.assertEqual(run['artifactId'],'artifact'); self.assertEqual(run['baseRevision'],'r1')
        self.assertEqual(run['operation'],'draw'); self.assertEqual(run['inputs'],[])
        self.assertNotIn('error',self.call('artcraft_capabilities',{}))
        self.assertTrue(self.server.calls[-1][1].endswith('/capabilities'))
        self.server.answer={'jobId':'job','status':'done','artifact':ARTIFACT}
        self.assertEqual(self.call('artcraft_job'), self.server.answer)
        self.assertEqual(self.call('artcraft_job')['artifact']['cardUrl'], ARTIFACT['cardUrl'])
        # Failed job DTO retains status, id and its server error.
        self.server.answer={'jobId':'job','status':'failed','error':{'code':'upstream','message':'App failed'}}
        self.assertEqual(self.call('artcraft_job'), self.server.answer)
    def test_03(self):
        codes = ['artcraft_off','app_not_allowed','not_member','agent_not_granted','device_offline','busy','invalid_request','stale_base','quota','not_found','upstream']
        self.assertEqual(list(art.ERROR_CODES), codes)
        for name in ARGS:
            for code in codes:
                self.server.failure=code
                with self.subTest(tool=name,code=code): self.assertEqual(self.call(name), art.error(code,'Exact server explanation: '+code))
    def test_04(self):
        for name in ARGS:
            self.assertEqual(self.call(name,{**ARGS[name],'roomId':'foreign'})['error']['code'],'invalid_request')
            self.assertNotIn('error', self.call(name))
        self.assertTrue(all('/rooms/bound-room/' in c[1] for c in self.server.calls))
    def test_05(self):
        with self.assertLogs(share._DIAG.name, level='WARNING') as logs:
            self.assertEqual(self.call('artcraft_job',session='foreign')['error']['code'],'not_member')
        self.assertIn('ArtCraft request refused: not in a Room turn', logs.output[0])
        self.assertNotIn('share refused', logs.output[0])
        with patch.object(share, '_diagnose_refusal', side_effect=RuntimeError), self.assertLogs(share._DIAG.name, level='WARNING') as logs:
            self.call('artcraft_job',session='foreign')
        self.assertIn('ArtCraft request refused: not in a Room turn: diagnostic failed', logs.output[0])
        with self.assertLogs(share._DIAG.name, level='WARNING') as logs:
            share.room_share_file({},session_id='foreign')
        self.assertIn('share refused', logs.output[0])
        for name in ARGS: self.assertEqual(self.call(name,session='foreign')['error']['code'],'not_member')
        self.turn.sealed=lambda: True
        for name in ARGS: self.assertEqual(self.call(name)['error']['code'],'not_member')
        share.end('bound-room',self.turn)
        for name in ARGS: self.assertEqual(self.call(name)['error']['code'],'not_member')
        self.assertEqual(self.server.calls,[])
    def test_06(self):
        for name in ('artcraft_create','artcraft_run'):
            self.call(name); self.call(name)
            posts=[c[2] for c in self.server.calls if c[0]=='POST']
            self.assertNotEqual(posts[-1]['idempotencyKey'],posts[-2]['idempotencyKey'])
            self.assertTrue(16 <= len(posts[-1]['idempotencyKey']) <= 128)
            key='supplied-idempotency-key'
            self.call(name,{**ARGS[name],'idempotency_key':key}); self.call(name,{**ARGS[name],'idempotency_key':key})
            self.assertEqual([c[2]['idempotencyKey'] for c in self.server.calls if c[0]=='POST'][-2:],[key,key])
    def test_07(self):
        clock=[0.0]; self.turn.artcraft.clock=lambda: clock[0]
        self.turn.artcraft.sleep=lambda n: clock.__setitem__(0,clock[0]+n)
        self.server.answer={'jobId':'job','status':'running'}
        result=self.call('artcraft_job',{'job_id':'job','wait_seconds':20})
        self.assertEqual(result,self.server.answer); self.assertEqual(clock[0],20)
        self.assertEqual(len(self.server.calls),20)  # one read per second for 20 s
        self.assertTrue(all(0 < c[3] <= 20 for c in self.server.calls))
        release=threading.Event()
        self.addCleanup(release.set)
        bound=art.ArtCraftTurn('room',lambda *a: release.wait(.3) or {},lambda:True)
        started=time.monotonic()
        self.assertEqual(bound.execute('artcraft_job',{'job_id':'job','wait_seconds':.02})['error']['code'],'upstream')
        self.assertLess(time.monotonic()-started,.2)
    def test_08(self):
        for tool in self.registry.values():
            desc=tool['description']
            for word in ('Floor plans','DXF','slides','documents','SVG','page layouts','PDFs','CADCraft','DeckCraft','WordCraft','VectorCraft','DesignCraft','PdfCraft','if ArtCraft is unavailable, tell the user; never download or run a program'):
                self.assertIn(word,desc)
    def test_09(self):
        self.server.answer={**ARTIFACT,'credential':'private-credential','hostPath':'/tmp/secret'}
        self.assertEqual(self.call('artcraft_artifact'),ARTIFACT)
        for field,value in [('name','/private/tmp/output.dxf'),('name','private-credential'),('cardUrl','Bearer private-credential'),('name','file:///root/private/file'),('name','path:/root/private/file')]:
            self.server.answer={**ARTIFACT,field:value}
            result=self.call('artcraft_artifact')
            self.assertEqual(result['error']['code'],'upstream'); self.assertNotIn(value,json.dumps(result))
        for directory in ('root','home','Users','var','etc','opt','tmp','private','srv','usr','run'):
            for value in ('/'+directory, 'Read /'+directory+'/private/file failed'):
                self.server.answer={**ARTIFACT,'name':value}
                self.assertEqual(self.call('artcraft_artifact')['error']['code'],'upstream')
        card=ARTIFACT['cardUrl']
        for value in ('/layer','relative/card','//room.invalid/card',card.replace('roomId=','roomId=x'),card.replace('art012345678901','ART012345678901'),card.replace('ver012345678901','ver01234567890'),card+'\n',card+'&extra=1',card.replace('#artcraft-','#other-')):
            for name in ('artcraft_artifact','artcraft_job'):
                artifact={**ARTIFACT,'cardUrl':value}
                self.server.answer=artifact if name=='artcraft_artifact' else {'jobId':'job','status':'done','artifact':artifact}
                self.assertEqual(self.call(name)['error']['code'],'upstream')
        self.server.answer={**ARTIFACT,'cardUrl':'https://room.invalid/card/artifact'}
        self.assertEqual(self.call('artcraft_artifact'),self.server.answer)
        schema={'operations': {'stroke_path': {'description':'Use /layer or (?:/ syntax', 'properties': {'path': {'type': 'array'}}}}}
        self.server.answer={'available':True,'apps':[],'device':'ready','schema':schema}
        self.assertEqual(self.call('artcraft_capabilities')['schema'],schema)
        self.server.answer={'error':{'code':'invalid_request','message':'Cannot resolve a/b in /layer'}}
        self.assertEqual(self.call('artcraft_job'),self.server.answer)
        self.server.answer={'error':{'code':'upstream','message':'Read /root/private/file failed'}}
        self.assertEqual(self.call('artcraft_job'),art.error('upstream','ArtCraft request failed.'))
    def test_10(self):
        bad=[('artcraft_create',{'app':'cadcraft','file_name':'../secret','calls':[{}]}),('artcraft_create',{'app':'cadcraft','file_name':'a.dxf'}),('artcraft_run',{'artifact_id':'a','base_revision':'r','calls':[]}),('artcraft_job',{'job_id':'job','wait_seconds':21}),('artcraft_job',{'job_id':'job','wait_seconds':True}),('artcraft_job',{'job_id':'job','wait_seconds':float('nan')}),('artcraft_job',{'job_id':'..'}),('artcraft_create',{'app':'cadcraft','file_name':'name.dxf\n','calls':[{}]}),('artcraft_create',{**ARGS['artcraft_create'],'operation':'draw','inputs':[]}),('artcraft_run',{**ARGS['artcraft_run'],'idempotency_key':'short'})]
        for name,args in bad: self.assertEqual(self.call(name,args)['error']['code'],'invalid_request')
        self.assertEqual(self.server.calls,[])
    def test_10_request_bounds(self):
        key='supplied-idempotency-key'
        for name in ('artcraft_create','artcraft_run'):
            with self.subTest(tool=name):
                base={k:v for k,v in ARGS[name].items() if k not in ('calls','operation','inputs')}
                base['idempotency_key']=key
                for count in (64,65):
                    before=len(self.server.calls)
                    result=self.call(name,{**base,'calls':[{} for _ in range(count)]})
                    if count==64:
                        self.assertNotIn('error',result)
                        self.assertEqual(len(self.server.calls[-1][2]['calls']),64)
                    else:
                        self.assertEqual(result['error']['code'],'invalid_request')
                        self.assertEqual(len(self.server.calls),before)
                for mode in ('calls','inputs'):
                    payload={'calls':[{'text':''}]} if mode=='calls' else {'operation':'draw','inputs':['']}
                    self.assertNotIn('error',self.call(name,{**base,**payload}))
                    overhead=self.server.body_sizes[-1]
                    available=256*1024-overhead
                    text='é'*(available//2)+'x'*(available%2)
                    for extra in ('','x'):
                        payload={'calls':[{'text':text+extra}]} if mode=='calls' else {'operation':'draw','inputs':[text+extra]}
                        before=len([c for c in self.server.calls if c[0]=='POST'])
                        result=self.call(name,{**base,**payload})
                        if not extra:
                            self.assertNotIn('error',result)
                            self.assertEqual(self.server.body_sizes[-1],256*1024)
                        else:
                            self.assertEqual(result['error']['code'],'invalid_request')
                            self.assertEqual(len([c for c in self.server.calls if c[0]=='POST']),before)
    def test_11(self):
        self.server.hook=lambda: setattr(self.turn,'active',False)
        self.assertEqual(self.call('artcraft_run')['error']['code'],'not_member')
        self.assertEqual(len(self.server.calls),1)
        self.assertEqual(self.server.calls[0][0],'GET')
    def test_12(self):
        # Registration has no dependency on when a host session was created.
        self.registry.clear()
        art.register(types.SimpleNamespace(register_tool=lambda **kw:self.registry.__setitem__(kw['name'],kw)))
        for name in ARGS: self.assertNotIn('error',self.call(name))
    def test_13(self):
        self.server.answer={'code':'not_found','message':'Legacy route missing'}
        self.assertEqual(self.call('artcraft_job'),art.error('upstream','Invalid ArtCraft response.'))
        with patch.object(protocol,'_urlopen',side_effect=OSError('private-credential /tmp/file')):
            for name in ARGS: self.assertEqual(self.call(name),art.error('upstream','ArtCraft request failed.'))

if __name__=='__main__': unittest.main()
