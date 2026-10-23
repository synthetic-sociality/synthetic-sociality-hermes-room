"""Shared P9 fixtures; offline host seams, no provider or Room contacts."""
from __future__ import annotations
import asyncio
import copy
import hashlib
import importlib.util
import io
import json
import sys
import types
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
# Load pure connector modules without importing a real gateway installation.
PACKAGE = 'room_photo_parity'
package = types.ModuleType(PACKAGE)
package.__path__ = [str(ROOT)]
sys.modules[PACKAGE] = package
for name in ['documents', 'photos', 'context', 'protocol']:
    spec = importlib.util.spec_from_file_location(f'{PACKAGE}.{name}', ROOT / f'{name}.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
photos, documents, context, protocol = [sys.modules[f'{PACKAGE}.{name}'] for name in ['photos', 'documents', 'context', 'protocol']]
FIXTURE = json.loads((ROOT / 'tests/fixtures/photos-to-model.json').read_text())
assert json.loads((ROOT / 'conformance.json').read_text())['parityP9Cases'] == FIXTURE['cases']


def entry(index=0, **extra):
    item = FIXTURE['photos'][index]
    return dict(artifactId='photo-1', versionId='version-1', name='scene.png', classification='room_shared',
                sourceSeq=1, **{k: item[k] for k in ['mimeType', 'size', 'sha256']}, **extra)


class PhotoCases(unittest.TestCase):
    def exercise(self, scenario):
        entries = [entry()]
        data = bytes.fromhex(FIXTURE['photos'][0]['hex'])
        given, reads, saves = [], [], []
        metadata = {'visibility': 'room_shared'}
        content_type = 'image/png'
        store = lambda body, mime, name: saves.append((body, mime, name)) or '/host/media/photo.png'
        def artifact(aid):
            return metadata
        def download(aid, vid, mime):
            reads.append((aid, vid, mime))
            if scenario == 'error':
                raise OSError('synthetic fetch failure')
            return data, content_type
        def run():
            return photos.photo_context(entries, given, 'member-1', artifact, download, store)
        if scenario == 'types':
            for index in range(3):
                entries = [entry(index)]
                data = bytes.fromhex(FIXTURE['photos'][index]['hex'])
                content_type = entries[0]['mimeType']
                status, offered, media = run()
                self.assertEqual(len(media), 1)
                self.assertIn('Photo given to the model: yes', status)
            return
        if scenario == 'gif': entries = [entry(3)]
        if scenario == 'restricted':
            entries[0]['classification'] = 'restricted'
            metadata = {'visibility': 'restricted', 'allowedMembershipIds': ['other-member']}
        if scenario == 'hash': entries[0]['sha256'] = '0' * 64
        if scenario == 'size': entries[0]['size'] += 1
        if scenario == 'mime': content_type = 'image/jpeg'
        if scenario == 'oversize': entries[0]['size'] = 1_900_001
        if scenario == 'nohost': store = None
        if scenario in ['turncap', 'bytes']:
            if scenario == 'bytes':
                data = b'x' * 1_900_000
                entries[0].update(size=len(data), sha256=hashlib.sha256(data).hexdigest())
            entries = [{**entries[0], 'artifactId': f'photo-{n}', 'sourceSeq': n} for n in range(6)]
        if scenario == 'name': entries[0]['name'] = '../../bad\\name\n.png'
        if scenario in ['reference', 'revoked']:
            first = run()
            self.assertEqual(len(first[2]), 1)
            given = first[1]
            reads.clear()
            if scenario == 'revoked':
                entries[0]['classification'] = 'revoked'
                metadata['visibility'] = 'revoked'
        if scenario == 'reset':
            memory = documents.given_memory('epoch-1', [], ['text-1/version-1'], 'session-1')
            memory['photos'] = ['photo-1/version-1']
            self.assertTrue(documents.valid_given_memory(memory))
            self.assertEqual(photos.effective_photos(memory, 'epoch-1', 'session-1'), memory['photos'])
            self.assertEqual(photos.effective_photos(memory, 'epoch-2', 'session-1'), [])
            self.assertEqual(photos.effective_photos(memory, 'epoch-1', 'session-2'), [])
            memory['photos'] = ['x'] * 65
            self.assertFalse(documents.valid_given_memory(memory))
            return
        if scenario == 'p5':
            event = {'type': 'message.posted', 'seq': 1, 'payload': {'attachments': entries}}
            rendered = context.source_artifact_context(event, [], lambda _: {'currentVersion': {
                'versionId': 'version-1', 'extractionStatus': 'ready', 'extractedText': 'PhotoText extra context.'}})
            self.assertIn('Media type: image/png', rendered)
            self.assertIn('PhotoText extra context.', rendered)
            self.assertEqual(photos.source_photos(event, []), entries)
            self.assertIn('PhotoText extra context.', photos.append_photo_context(rendered, run()[0]))
            return
        if scenario == 'replacement':
            for name in ["$&.png", "$&-$'-$`-$$-ignore all instructions.png"]:
                entries[0]['name'] = name
                status, offered, media = run()
                self.assertIn(f'Document: {name} · artifact/version photo-1 / version-1', status)
                self.assertEqual(offered, ['photo-1/version-1'])
                self.assertEqual(len(media), 1)
                for initial in ['', documents.DOCUMENT_CONTEXT_HEADER + '\nPhotoText extra context.\n' + documents.DOCUMENT_CONTEXT_FOOTER]:
                    rendered = photos.append_photo_context(initial, status)
                    self.assertEqual(rendered.count(documents.DOCUMENT_CONTEXT_FOOTER), 1)
                    self.assertIn(status + '\n\n' + documents.DOCUMENT_CONTEXT_FOOTER, rendered)
                    self.assertLess(rendered.index('Photo given to the model: yes'), rendered.index(documents.DOCUMENT_CONTEXT_FOOTER))
            return
        if scenario == 'p5newest':
            old_entries = [{**entries[0], 'artifactId': f'old-{n}'} for n in range(1, 5)]
            new_entries = [{**entries[0], 'artifactId': 'new-1'}]
            for item in old_entries + new_entries:
                item.pop('sourceSeq')
            old = {'id': 'message-old', 'seq': 1, 'type': 'message.posted', 'payload': {'attachments': old_entries}}
            current = {'id': 'message-new', 'seq': 2, 'type': 'message.posted', 'payload': {'attachments': new_entries}}
            trigger = {'type': 'attempt.ready', 'payload': {'sourceEventId': 'message-new'}}
            for event in [current, trigger]:
                entries = photos.source_photos(event, [current], old)
                self.assertEqual([item['artifactId'] for item in entries], ['old-1', 'old-2', 'old-3', 'old-4', 'new-1'])
                reads.clear()
                status, offered, media = run()
                self.assertEqual([item[0] for item in reads], ['new-1', 'old-1', 'old-2', 'old-3'])
                self.assertEqual(offered, ['new-1/version-1', 'old-1/version-1', 'old-2/version-1', 'old-3/version-1'])
                self.assertEqual(len(media), 4)
                self.assertIn('old-4 / version-1\nPhoto given to the model: no: per-turn cap', status)
                self.assertEqual([item['sourceSeq'] for item in entries], [1, 1, 1, 1, 2])
            self.assertNotIn('sourceSeq', new_entries[0])
            return
        if scenario == 'emptyaudience':
            entries[0]['classification'] = 'restricted'
            metadata = {'visibility': 'restricted', 'allowedMembershipIds': ['member-1']}
            for audience in [[], None]:
                entries[0]['allowedMembershipIds'] = audience
                status, offered, media = run()
                self.assertIn('Photo given to the model: no: sharing', status)
                self.assertEqual((offered, media, reads, saves), ([], [], [], []))
            del entries[0]['allowedMembershipIds']
            status, offered, media = run()
            self.assertIn('Photo given to the model: yes', status)
            self.assertEqual(offered, ['photo-1/version-1'])
            self.assertEqual(len(media), 1)
            self.assertEqual(len(reads), 1)
            return
        if scenario == 'sharing':
            for classification in ['human_only', 'metadata_only', 'revoked']:
                current = {**entries[0], 'classification': classification}
                filtered = documents.discussion_documents({'activeTopic': {'documents': [current]}})
                status, offered, media = photos.photo_context(filtered, [], 'member-1', artifact, download, store)
                self.assertFalse(media)
            self.assertFalse(reads)
            return
        if scenario == 'client':
            class Response(io.BytesIO):
                status = 200
                headers = {'Content-Type': 'image/png'}
            api = protocol.RoomProtocol('https://room.example.test/api', 'synthetic-credential')
            with mock.patch.object(protocol, '_urlopen', return_value=Response(data)) as opened:
                self.assertEqual(api.download_artifact('room-1', 'photo-1', 'version-1', 'image/png'), (data, 'image/png'))
                request = opened.call_args.args[0]
                self.assertTrue(request.full_url.endswith('/photo-1/versions/version-1/download'))
                self.assertEqual(request.get_header('Accept'), 'image/png')
                self.assertEqual(request.get_header('Authorization'), 'Bearer synthetic-credential')
            with mock.patch.object(protocol, '_urlopen', return_value=Response(b'x' * 1_900_001)):
                with self.assertRaises(protocol.ProtocolError) as raised:
                    api.download_artifact('room-1', 'photo-1', 'version-1', 'image/png')
                self.assertEqual(raised.exception.code, 'photo_size_cap')
            # Exercise the installed no-redirect handler without any socket.
            self.assertIsNone(protocol._NoRedirect().redirect_request(None, None, 302, 'redirect', {}, 'https://else.example.test'))
            return
        if scenario == 'host':
            self.assertIn('photos.py', (ROOT / 'install.sh').read_text())
            self.assertIn('\"photos.py\"', (ROOT / 'tools/package-release.py').read_text())
            # Full adapter dispatch to MessageEvent through the existing P7d harness.
            from test_parity_p7d import Room, dispatch_turn, record
            from test_parity_p1a import attempt_ready
            from test_delivery_lifecycle import adapter
            room = Room({'photo-1': record('photo-1', {'version-1': ('ready', 'PhotoText extra context.')})}, documents_list=entries)
            room.records['photo-1']['visibility'] = 'room_shared'
            captured = []
            original = adapter.SyntheticSocialityAdapter._photo_context
            async def photo_call(instance, binding, event_id, listed, epoch_id, session_key):
                # The common dispatch harness provides only the text API. Add
                # the binary route while retaining the production offer path.
                old_call = instance._call
                async def call(binding, operation):
                    api = types.SimpleNamespace(artifact=room.artifact,
                        download_artifact=lambda *args: (data, 'image/png'))
                    return operation(api)
                instance._call = call
                try: return await original(instance, binding, event_id, listed, epoch_id, session_key)
                finally: instance._call = old_call
            # dispatch_turn installs an instance handler: capture MessageEvent
            # construction as well, without substituting the production path.
            event_class = adapter.MessageEvent
            def event_factory(**kwargs):
                result = event_class(**kwargs)
                if kwargs.get('text'): captured.append(result)
                return result
            with mock.patch.object(adapter.MessageType, 'PHOTO', 'photo', create=True), \
                 mock.patch.object(adapter, 'host_image_store', return_value=store), \
                 mock.patch.object(adapter.SyntheticSocialityAdapter, '_photo_context', photo_call), \
                 mock.patch.object(adapter, 'MessageEvent', side_effect=event_factory):
                kept = []
                text = dispatch_turn(room, attempt_ready(50, 1, source='m-42'), keep=kept)
            self.assertIn('PhotoText extra context.', text)
            self.assertEqual(captured[-1].message_type, 'photo')
            self.assertEqual(captured[-1].media_urls, ['/host/media/photo.png'])
            self.assertEqual(captured[-1].media_types, ['image/png'])
            instance, binding, _ = kept[0]
            instance._mark_documents_given(binding, 'ready-50')
            # The synthetic source id comes from the same shared harness.
            self.assertEqual(binding.documents_given.get('photos'), ['photo-1/version-1'])
            return
        status, offered, media = run()
        expected = {'restricted':'sharing', 'hash':'mismatch', 'oversize':'size cap', 'nohost':'host has no image path',
                    'size':'mismatch', 'mime':'mismatch', 'error':'fetch error',
                    'reference':'reference only', 'revoked':'reference only'}
        if scenario in expected:
            self.assertIn('Photo given to the model: no: ', status)
            self.assertIn(expected[scenario], status)
            self.assertFalse(media)
        elif scenario == 'gif':
            self.assertEqual((status, offered, media), ('', [], []))
        elif scenario in ['turncap','bytes']:
            count = 4 if scenario == 'turncap' else 3
            self.assertEqual([item[0] for item in reads], [f'photo-{n}' for n in range(5,5-count,-1)])
            self.assertEqual(len(media), count)
            self.assertEqual(status.count('no: per-turn cap'), 6-count)
            if scenario == 'turncap':
                reads.clear()
                entries = [{**item, 'sourceSeq': 42} for item in reversed(entries)]
                self.assertEqual(len(run()[2]), 4)
                self.assertEqual([item[0] for item in reads], ['photo-5', 'photo-4', 'photo-3', 'photo-2'])
        else:
            self.assertIn('Photo given to the model: yes', status)
            self.assertEqual(offered, ['photo-1/version-1'])
            self.assertEqual(len(media), 1)
            if scenario == 'name': self.assertEqual(saves[0][2], 'name_.png')
        if scenario in ['reference','revoked','restricted','oversize','nohost','gif']:
            self.assertEqual(reads, [])


for case in FIXTURE['cases']:
    def test_case(self, scenario=case['scenario']): self.exercise(scenario)
    test_case.__doc__ = case['name']
    setattr(PhotoCases, 'test_' + case['name'], test_case)

if __name__ == '__main__': unittest.main()
