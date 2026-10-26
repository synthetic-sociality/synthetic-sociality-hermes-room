"""Room share authority follows current host lineage, without a hook or core."""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from support import load_module

share = load_module('share_file')
origin = load_module('origin_context')


class ShareSessionBindingTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name).resolve()
        (self.root / 'draft.md').write_text('Synthetic share.\n')
        self.created = []
        self.turn = self.begin('lineage-room', 'lineage-session')

    def begin(self, room, session):
        turn = share.ShareTurn({}, self.root, [], lambda: True,
            lambda *_: self.created.append(room) or
            {'artifactId': 'synthetic-artifact', 'versionId': 'synthetic-version'})
        share.begin(room, turn, session)
        self.addCleanup(share.end, room, turn)
        return turn

    def call(self, **metadata):
        return json.loads(share.room_share_file({'path': 'draft.md'}, **metadata))

    def refused(self, **metadata):
        self.assertEqual(self.call(**metadata), share.refused('no-room-turn'))

    def test_own_session_without_hook_or_matching_turn_id(self):
        self.assertTrue(self.call(session_id='lineage-session', turn_id='tool-scheme')['success'])
        self.assertEqual(self.created, ['lineage-room'])

    def test_child_without_prior_mapping_or_hook(self):
        self.assertEqual(origin.room_for_session('lineage-child'), '')
        self.assertTrue(self.call(session_id='lineage-child',
                                  parent_session_id='lineage-session')['success'])
        self.assertEqual(self.created, ['lineage-room'])

    def test_child_with_room_provenance_but_no_turn_binding(self):
        origin.remember_room_session('lineage-mapped-child', 'lineage-room')
        self.assertTrue(self.call(session_id='lineage-mapped-child',
                                  parent_session_id='lineage-session')['success'])
        self.assertEqual(self.created, ['lineage-room'])

    def test_rotation_extends_only_the_current_turn_lineage(self):
        self.assertTrue(self.call(session_id=' lineage-rotated ',
                                  parent_session_id=' lineage-session ')['success'])
        self.assertTrue(self.call(session_id='lineage-rotated')['success'])
        self.assertTrue(self.call(session_id='lineage-grandchild',
                                  parent_session_id='lineage-rotated')['success'])
        self.assertEqual(self.created, ['lineage-room'])  # receipt is reused

    def test_ended_turn_refuses_own_child_and_new_descendant(self):
        self.assertTrue(share.note_host_turn('lineage-ended-child', '', 'lineage-room',
                                             parent_session_id='lineage-session'))
        share.end('lineage-room', self.turn)
        self.refused(session_id='lineage-session')
        self.refused(session_id='lineage-ended-child', parent_session_id='lineage-session')
        self.refused(session_id='lineage-late-child', parent_session_id='lineage-session')
        self.assertEqual(self.created, [])

    def test_previous_turn_sessions_and_their_children_cannot_join_successor(self):
        share.note_host_turn('lineage-old-child', '', 'lineage-room',
                             parent_session_id='lineage-session')
        share.end('lineage-room', self.turn)
        current = self.begin('lineage-room', 'lineage-successor')
        for metadata in (
            {'session_id': 'lineage-session'},
            {'session_id': 'lineage-old-child', 'parent_session_id': 'lineage-session'},
            {'session_id': 'lineage-new-child', 'parent_session_id': 'lineage-session'},
        ):
            with self.subTest(metadata=metadata):
                self.refused(**metadata)
        self.assertEqual(current.host_sessions, {'lineage-successor'})
        self.assertEqual(self.created, [])

    def test_unrelated_missing_and_invalid_sessions_cannot_gain_authority(self):
        origin.remember_room_session('lineage-historical', 'lineage-room')
        for metadata in (
            {}, {'session_id': 'lineage-unrelated'},
            {'session_id': 'lineage-historical'},
            {'session_id': 'lineage-unrelated', 'parent_session_id': 'lineage-historical'},
            {'session_id': 'lineage-unrelated', 'parent_session_id': 'lineage-missing'},
            {'parent_session_id': 'lineage-session'},
            {'session_id': None, 'parent_session_id': 'lineage-session'},
            {'session_id': 7, 'parent_session_id': 'lineage-session'},
            {'session_id': ' ', 'parent_session_id': 'lineage-session'},
            {'session_id': 'lineage-unrelated', 'parent_session_id': 7},
        ):
            with self.subTest(metadata=metadata):
                self.refused(**metadata)
        self.assertEqual(self.created, [])

    def test_cross_room_sessions_and_parents_cannot_move_authority(self):
        other = self.begin('lineage-other-room', 'lineage-other-session')
        origin.remember_room_session('lineage-foreign-history', 'lineage-other-room')
        for metadata in (
            {'session_id': 'lineage-other-session', 'parent_session_id': 'lineage-session'},
            {'session_id': 'lineage-foreign-history', 'parent_session_id': 'lineage-session'},
            {'session_id': 'lineage-session', 'parent_session_id': 'lineage-other-session'},
        ):
            with self.subTest(metadata=metadata):
                self.refused(**metadata)
        self.assertEqual(other.host_sessions, {'lineage-other-session'})
        self.assertEqual(self.turn.host_sessions, {'lineage-session'})
        self.assertEqual(origin.room_for_session('lineage-other-session'), 'lineage-other-room')
        self.assertEqual(self.created, [])

    def test_unresolved_turn_cannot_use_historical_parent(self):
        origin.remember_room_session('lineage-unresolved-parent', 'lineage-unresolved-room')
        unresolved = self.begin('lineage-unresolved-room', '')
        self.refused(session_id='lineage-unresolved-child',
                     parent_session_id='lineage-unresolved-parent')
        self.assertEqual(unresolved.host_sessions, set())
        self.assertEqual(self.created, [])

    def test_parent_lineage_does_not_bypass_canonical_sealing(self):
        self.turn.sealed = lambda: True
        self.refused(session_id='lineage-session')
        self.refused(session_id='lineage-sealed-child', parent_session_id='lineage-session')
        self.assertEqual(self.created, [])
        self.assertEqual(self.turn.records, {})


if __name__ == '__main__':
    unittest.main()
