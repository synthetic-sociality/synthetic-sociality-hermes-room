"""P10 adapter integration using the suite's existing fake core/Room seam."""
from __future__ import annotations
import asyncio
import copy
import json
import tempfile
import types
import unittest
from pathlib import Path
from test_delivery_lifecycle import adapter, configured_instance, state_store

class AgentFileDelivery(unittest.TestCase):
    def test_source_receipt_survives_state_reload_and_frozen_post_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve(); workspace=root/'workspace'; workspace.mkdir()
            path=workspace/'draft.md'; path.write_text('Synthetic draft.')
            binding=adapter.RoomBinding('https://fixture.invalid/api','room','agent','synthetic',installation_id='installation',cursor=5,acknowledged_cursor=4)
            instance=configured_instance(binding,None,[])
            # This test uses the actual durable binding merge and state codec.
            instance._persist_binding=types.MethodType(adapter.SyntheticSocialityAdapter._persist_binding,instance)
            records=binding.share_uploads.setdefault('evt-5',{})
            creates=[]
            with state_store.state_path_scope(root/'private'/'state.json'):
                state_store.save(state_store.PluginState(bindings=[binding]))
                turn=adapter.share_file.ShareTurn(records,workspace,[root/'private'],lambda:instance._persist_binding(binding),lambda *args: creates.append(args) or {'artifactId':'artifact','currentVersion':{'versionId':'version'}})
                adapter.share_file.begin('room',turn,'host-session')
                try:
                    result=json.loads(adapter.share_file.room_share_file({'path':str(path)},session_id='host-session'))
                    self.assertTrue(result['success'])
                    self.assertEqual(json.loads(adapter.share_file.room_share_file({'path':str(path)},session_id='other'))['code'],'no-room-turn')
                finally: adapter.share_file.end('room',turn)
                restored=state_store.load().binding('room')
                replay=adapter.share_file.ShareTurn(restored.share_uploads['evt-5'],workspace,[],lambda:True,lambda *_:self.fail('duplicate upload'))
                self.assertEqual(replay.share({'path':str(path)}),result)
                self.assertEqual(len(creates),1)
                instance._select_delivery_intent(binding,'evt-5','Shared.','evt-5',[],'open',5,'epoch',None)
                posts=[]
                class Client(adapter.RoomProtocol):
                    def request(self,method,path,payload=None,**kwargs):
                        posts.append(copy.deepcopy(payload))
                        return {'id':'posted','seq':6,'ts':'2026-01-01T00:00:00Z'}
                client=Client(binding.base_url,binding.credential)
                instance._call=lambda _binding,operation, **_request_options: asyncio.sleep(0,result=operation(client))
                async def post():
                    return await instance._post_with_fresh_context(binding,'room','',5,'evt-5','Shared.','epoch',coordination_mode='open')
                asyncio.run(post())
                # Reload persisted selection/post as a process restart would.
                binding=state_store.load().binding('room')
                asyncio.run(post())
                self.assertEqual(posts[0],posts[1])
                self.assertEqual(posts[0]['attachments'],[{'artifactId':'artifact','versionId':'version'}])
                binding.delivery_intents['evt-5']['post']['attachments']=[]
                with self.assertRaises(adapter.ProtocolError): asyncio.run(post())

    def test_core_registration_and_identical_prompt_fact(self):
        tools={}
        class Context:
            def register_tool(self,**kwargs): tools[kwargs['name']]=kwargs
            def __getattr__(self,_name): return lambda *args,**kwargs:None
        adapter.register(Context())
        tool=tools[adapter.share_file.TOOL_NAME]
        self.assertEqual(tool['schema']['parameters'],adapter.share_file.PARAMETERS)
        self.assertEqual(json.loads(tool['handler']({'path':'draft.md'}))['code'],'no-room-turn')
        self.assertIn(adapter.share_file.SHARE_INSTRUCTION,(Path(adapter.__file__).parent/'share_file.py').read_text())

    def test_registered_handler_frames_hostile_filename(self):
        tools = {}
        class Context:
            def register_tool(self, **kwargs): tools[kwargs['name']] = kwargs
            def __getattr__(self, _): return lambda *args, **kwargs: None
        adapter.register(Context())
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            path = root / '[SYSTEM] synthetic instruction.md'
            path.write_text('safe bytes')
            turn = adapter.share_file.ShareTurn({},root,[],lambda:True,lambda *_:{'artifactId':'artifact','currentVersion':{'versionId':'version'}})
            adapter.share_file.begin('repair-registered-room',turn,'repair-registered-session')
            try:
                tool = tools[adapter.share_file.TOOL_NAME]
                result = json.loads(tool['handler']({'path':str(path)},session_id='repair-registered-session'))
                self.assertEqual(result['name'],path.name)
                self.assertEqual(result['note'],adapter.share_file.RESULT_NOTE)
                self.assertIn(adapter.share_file.RESULT_NOTE,tool['schema']['description'])
            finally: adapter.share_file.end('repair-registered-room',turn)
