"""ISSUE-106 full Hermes send-path checks; requires the supported Hermes host.

The isolated companion test covers freeze/retry offline. Dispatch runs this
entry-point regression with the existing host-backed parity suites.
"""
import asyncio
import json
import unittest
from test_delivery_lifecycle import ROOT, adapter
from test_parity_p1 import cycle_attempt, room_instance, source_ref

CASES = json.loads((ROOT/'tests/fixtures/issue-106.json').read_text())

class Issue106HostTests(unittest.TestCase):
    def test_shared_types_cross_the_final_send_boundary(self):
        for case in CASES['cases']:
            with self.subTest(case=case['id']):
                instance, binding, api, _, _ = room_instance(cycle_attempt())
                captured=[]
                original=api.post_message
                def post(*args, **kwargs):
                    captured.append((args[4], kwargs.get('contribution_type')))
                    return original(*args, **kwargs)
                api.post_message=post
                envelope={'action':'contribute','body':case['text']}
                if case['type'] is not None:
                    envelope['contributionType']=case['type']
                result=asyncio.run(instance._send_final(binding.room_id,source_ref(),json.dumps(envelope)))
                self.assertTrue(result.success,getattr(result,"error",None))
                self.assertEqual(captured,[(case['text'],case['type'] or 'claim')])

    def test_unsupported_author_type_fails_without_post(self):
        for kind in CASES['rejected']:
            instance,binding,api,_,_=room_instance(cycle_attempt())
            output=json.dumps({'action':'contribute','body':'Same text?','contributionType':kind})
            result=asyncio.run(instance._send_final(binding.room_id,source_ref(),output))
            self.assertFalse(result.success)
            self.assertEqual(api.posts,[])
